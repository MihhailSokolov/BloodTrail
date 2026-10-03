// SPDX-License-Identifier: Apache-2.0

//go:build integration

package engine

import (
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"

	"github.com/MihhailSokolov/BloodTrail/internal/graphtest"
)

// bumpStallProxy is a TCP proxy between an engine's pool and PostgreSQL that
// can hold one statement's response on the wire after PostgreSQL has
// executed -- and, for an autocommitted statement, committed -- it: a slow
// network, or a writer's goroutine descheduled between its bump committing
// and the bump call returning. Armed, the first client message carrying
// needle marks its connection; that connection's next client write (the
// Bind/Execute half of pgx's describe-exec round trip) is forwarded, and
// every server byte after it is held until releaseHeld.
type bumpStallProxy struct {
	ln     net.Listener
	target string
	needle []byte

	armed       atomic.Bool
	stalledOnce sync.Once
	stalled     chan struct{}
	releaseOnce sync.Once
	release     chan struct{}

	mu    sync.Mutex
	conns []net.Conn
}

func newBumpStallProxy(t *testing.T, target, needle string) *bumpStallProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("proxy listen: %v", err)
	}
	p := &bumpStallProxy{ln: ln, target: target, needle: []byte(needle), stalled: make(chan struct{}), release: make(chan struct{})}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go p.serve(c)
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		p.mu.Lock()
		for _, c := range p.conns {
			_ = c.Close()
		}
		p.mu.Unlock()
	})
	return p
}

func (p *bumpStallProxy) serve(client net.Conn) {
	server, err := net.Dial("tcp", p.target)
	if err != nil {
		_ = client.Close()
		return
	}
	p.mu.Lock()
	p.conns = append(p.conns, client, server)
	p.mu.Unlock()

	var phase atomic.Int32 // 0 pass through, 1 needle seen, 2 hold the responses
	go func() {
		buf := make([]byte, 1<<16)
		for {
			n, err := client.Read(buf)
			if n > 0 {
				chunk := buf[:n]
				switch {
				case phase.Load() == 1:
					phase.Store(2)
				case p.armed.Load() && bytes.Contains(chunk, p.needle):
					if p.armed.CompareAndSwap(true, false) {
						phase.Store(1)
					}
				}
				if _, werr := server.Write(chunk); werr != nil {
					return
				}
			}
			if err != nil {
				_ = server.Close()
				return
			}
		}
	}()
	buf := make([]byte, 1<<16)
	for {
		n, err := server.Read(buf)
		if n > 0 {
			if phase.Load() == 2 {
				p.stalledOnce.Do(func() { close(p.stalled) })
				<-p.release
				phase.Store(0)
			}
			if _, werr := client.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			_ = client.Close()
			return
		}
	}
}

// arm makes the next statement carrying the needle the one whose response is
// held.
func (p *bumpStallProxy) arm() { p.armed.Store(true) }

// waitStalled blocks until the armed statement's response is being held.
func (p *bumpStallProxy) waitStalled(t *testing.T) {
	t.Helper()
	select {
	case <-p.stalled:
	case <-time.After(5 * time.Second):
		t.Fatalf("the armed statement never reached the proxy")
	}
}

// releaseHeld lets the held response through. Idempotent.
func (p *bumpStallProxy) releaseHeld() { p.releaseOnce.Do(func() { close(p.release) }) }

// bumpStallEnginePool builds a pool that reaches PostgreSQL through a
// bumpStallProxy armed for the watermark bump. Statements run in pgx's
// describe-exec mode so every execution carries its SQL text for the proxy to
// recognize. The held response is released at cleanup before the pool is
// closed, so a failing test cannot leave pool.Close waiting on it.
func bumpStallEnginePool(t *testing.T, dsn string) (*bumpStallProxy, *pgxpool.Pool) {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	proxy := newBumpStallProxy(t, net.JoinHostPort(cfg.ConnConfig.Host, strconv.Itoa(int(cfg.ConnConfig.Port))), bumpWatermarkSQL)
	host, portText, err := net.SplitHostPort(proxy.ln.Addr().String())
	if err != nil {
		t.Fatalf("proxy address: %v", err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatalf("proxy port: %v", err)
	}
	cfg.ConnConfig.Host = host
	cfg.ConnConfig.Port = uint16(port)
	cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeDescribeExec
	enginePool, err := pg.NewPool(cfg)
	if err != nil {
		t.Fatalf("engine pool: %v", err)
	}
	t.Cleanup(enginePool.Close)
	t.Cleanup(proxy.releaseHeld)
	return proxy, enginePool
}

// createWatermarkProbeNode commits one named node through the plain pg
// driver, before any engine exists, and returns its id.
func createWatermarkProbeNode(t *testing.T, ctx context.Context, pgDriver *pg.Driver, objectID, name string) graph.ID {
	t.Helper()
	var id graph.ID
	if err := pgDriver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		n, err := tx.CreateNode(graph.NewProperties().Set("objectid", objectID).Set("name", name), persistRaceKind)
		if err != nil {
			return err
		}
		id = n.ID
		return nil
	}); err != nil {
		t.Fatalf("create node %s: %v", objectID, err)
	}
	return id
}

// setWatermarkProbeNodeName renames a node with an UPDATE -- a write that
// moves no id sequence, so nothing but the watermark counter can tell a
// snapshot file it is missing.
func setWatermarkProbeNodeName(t *testing.T, ctx context.Context, pgDriver *pg.Driver, id graph.ID, objectID, name string) {
	t.Helper()
	if err := pgDriver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		return tx.UpdateNode(&graph.Node{ID: id, Kinds: graph.Kinds{persistRaceKind}, Properties: graph.NewProperties().Set("objectid", objectID).Set("name", name)})
	}); err != nil {
		t.Fatalf("rename node %d: %v", id, err)
	}
}

// waitForPGWatermark polls PostgreSQL's watermark counter until it reads want.
func waitForPGWatermark(t *testing.T, ctx context.Context, pool *pgxpool.Pool, want uint64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var got uint64
		if err := pool.QueryRow(ctx, "select counter from bloodtrail_watermark where id = 1").Scan(&got); err != nil {
			t.Fatalf("read watermark counter: %v", err)
		}
		if got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("watermark counter = %d, never reached %d", got, want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// requireNoSnapshotFile fails unless nothing is saved at path.
func requireNoSnapshotFile(t *testing.T, path string, logged string, why string) {
	t.Helper()
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a snapshot file was written %s (stat: %v):\n%s", why, err, logged)
	}
}

// bootServedName boots a fresh engine over pool and dir and returns the name
// it serves for node id.
func bootServedName(t *testing.T, ctx context.Context, pgDriver *pg.Driver, pool *pgxpool.Pool, dir string, id graph.ID) (any, string) {
	t.Helper()
	eng, buf := newLogCapturingEngine(pgDriver, pool, dir)
	eng.Start(ctx)
	t.Cleanup(eng.Stop)
	waitForFresh(t, eng)
	view, _ := eng.Fresh()
	dense, ok := view.Dense(uint64(id))
	if !ok {
		t.Fatalf("node %d missing from the booted view:\n%s", id, buf.String())
	}
	name, _ := view.PropValueByName(dense, "name")
	return name, buf.String()
}

// TestSaveSnapshotRefusesWhileAnEarlierBumpIsInFlight is a write W whose
// watermark bump has committed in PostgreSQL but whose BumpWatermark call has
// not returned yet -- its response held on the wire -- while a later write
// W2 bumps, writes and applies in full. PostgreSQL's counter then equals the
// highest counter this engine resolved and nothing it knows of is in flight,
// yet W is neither in the replica nor accounted for. A file saved now is
// stamped with a counter that vouches for W; W then commits an UPDATE (no
// sequence moves), and a boot after a hard stop would adopt the file and
// serve the replica without W for good. The save must refuse while W is out,
// and succeed -- with W -- once W resolves.
func TestSaveSnapshotRefusesWhileAnEarlierBumpIsInFlight(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)
	if _, err := pgDriver.AssertKinds(ctx, graph.Kinds{persistRaceKind}); err != nil {
		t.Fatalf("assert kinds: %v", err)
	}
	const objectID = "watermark-inflight-x"
	xID := createWatermarkProbeNode(t, ctx, pgDriver, objectID, "before")

	proxy, enginePool := bumpStallEnginePool(t, dsn)
	dir := t.TempDir()
	eng, buf := newLogCapturingEngine(pgDriver, enginePool, dir)
	resetWatermarkTable(t, ctx, eng)
	parkRebuildLoop(eng)
	defer stopEngineAndCloseWritePool(eng)
	adoptOneRebuild(t, ctx, eng)
	path := snapshotFilePathFor(t, pgDriver, dir)

	// W: a real BumpWatermark whose UPDATE commits while its response is held.
	type bumpOutcome struct {
		counter uint64
		err     error
	}
	wDone := make(chan bumpOutcome, 1)
	proxy.arm()
	go func() {
		counter, err := eng.BumpWatermark(ctx)
		wDone <- bumpOutcome{counter, err}
	}()
	proxy.waitStalled(t)
	waitForPGWatermark(t, ctx, pool, 1)

	// W2: a later write's whole bump, write and Apply, while W is out.
	applyOneWrite(t, ctx, eng)

	if err := eng.SaveSnapshot(ctx); err != nil {
		t.Fatalf("SaveSnapshot while W is out: %v", err)
	}
	requireNoSnapshotFile(t, path, buf.String(), "while an earlier bump was unresolved")
	if got := eng.inflightBumps.Load(); got != 1 {
		t.Fatalf("inflightBumps = %d while W's committed bump has not returned, want 1: a bump is in flight from before its UPDATE is sent", got)
	}

	// W's response arrives; W writes an UPDATE and applies.
	proxy.releaseHeld()
	w := <-wDone
	if w.err != nil {
		t.Fatalf("W's BumpWatermark: %v", w.err)
	}
	setWatermarkProbeNodeName(t, ctx, pgDriver, xID, objectID, "after")
	scope := NewWriteScope()
	scope.SetWatermark(w.counter)
	scope.Changes().RecordNodeID(xID)
	eng.Apply(ctx, scope)

	// Every counter is resolved now: the save goes ahead, and carries W.
	if err := eng.SaveSnapshot(ctx); err != nil {
		t.Fatalf("SaveSnapshot after W resolved: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("no snapshot file after every write resolved (stat: %v):\n%s", err, buf.String())
	}

	served, logged := bootServedName(t, ctx, pgDriver, pool, dir, xID)
	if !strings.Contains(logged, "bloodtrail: snapshot file loaded") {
		t.Fatalf("the next boot did not adopt the file saved after W resolved:\n%s", logged)
	}
	if served != "after" {
		t.Fatalf("the booted replica serves X.name=%v, PostgreSQL holds \"after\"", served)
	}
}

// TestSaveSnapshotRefusesACounterAnotherServerAdvanced is a second
// BloodTrail server writing the same database: server B bumps the counter
// and renames X, which server A never observes. A then writes once itself.
// A's own resolved counter now equals PostgreSQL's, which a highest-resolved
// comparison takes as convergence: A would stamp a file vouching for B's
// counter without B's write, and adopt it at its next boot. A must refuse to
// save, say why, and -- once a rebuild has loaded B's write -- save again.
func TestSaveSnapshotRefusesACounterAnotherServerAdvanced(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)
	if _, err := pgDriver.AssertKinds(ctx, graph.Kinds{persistRaceKind}); err != nil {
		t.Fatalf("assert kinds: %v", err)
	}
	const objectID = "watermark-second-server-x"
	xID := createWatermarkProbeNode(t, ctx, pgDriver, objectID, "before")

	dir := t.TempDir()
	engA, bufA := newLogCapturingEngine(pgDriver, pool, dir)
	resetWatermarkTable(t, ctx, engA)
	parkRebuildLoop(engA)
	defer stopEngineAndCloseWritePool(engA)
	adoptOneRebuild(t, ctx, engA)
	path := snapshotFilePathFor(t, pgDriver, dir)

	// Server B: its own engine bumps, renames X, and applies on its side.
	engB := New(pgDriver, pool, Config{Enabled: true, Log: testEngineLogger()})
	counterB, err := engB.BumpWatermark(ctx)
	if err != nil {
		t.Fatalf("B's bump: %v", err)
	}
	setWatermarkProbeNodeName(t, ctx, pgDriver, xID, objectID, "by-server-b")
	scopeB := NewWriteScope()
	scopeB.SetWatermark(counterB)
	scopeB.Changes().RecordNodeID(xID)
	engB.Apply(ctx, scopeB)

	// Server A writes once itself, and tries to save.
	applyOneWrite(t, ctx, engA)
	if err := engA.SaveSnapshot(ctx); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	requireNoSnapshotFile(t, path, bufA.String(), "over a counter another server advanced")
	if logged := bufA.String(); !strings.Contains(logged, "never resolved") {
		t.Fatalf("the refused save did not say the counter holds values this process never resolved:\n%s", logged)
	}

	// A rebuild loads B's committed write; A can account for every counter
	// again, and its next save carries B's write.
	adoptOneRebuild(t, ctx, engA)
	if err := engA.SaveSnapshot(ctx); err != nil {
		t.Fatalf("SaveSnapshot after the rebuild: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("no snapshot file after a rebuild loaded B's write (stat: %v):\n%s", err, bufA.String())
	}

	served, logged := bootServedName(t, ctx, pgDriver, pool, dir, xID)
	if !strings.Contains(logged, "bloodtrail: snapshot file loaded") {
		t.Fatalf("the next boot did not adopt the file saved after the rebuild:\n%s", logged)
	}
	if served != "by-server-b" {
		t.Fatalf("A's rebooted replica serves X.name=%v; PostgreSQL holds B's \"by-server-b\"", served)
	}
}
