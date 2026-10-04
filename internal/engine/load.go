// SPDX-License-Identifier: Apache-2.0

// Package engine hosts the BloodTrail in-memory graph engine: loading a
// PostgreSQL-backed graph into an immutable snapshot.Snapshot for
// cache-friendly traversal.
package engine

import (
	"context"
	"fmt"
	"runtime"
	"runtime/debug"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specterops/dawgs/drivers/pg"
	"golang.org/x/sync/errgroup"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

// LoadSnapshot loads pgDriver's default graph from PostgreSQL into an
// immutable snapshot.Snapshot.
//
// It resolves the default graph via the driver's SchemaManager (returning an
// error if none is set), then runs a single repeatable-read, read-only
// transaction that scans the global kind id/name table, then streams every
// node (including its property bag) ordered by id, then every edge, feeding
// all three into a snapshot.Builder, and finally runs the multi-graph probe
// (see probeMultiGraph). Build assigns dense NodeIDs in the node scan's
// ascending order and stamps the snapshot's BuiltAt.
//
// The result's WatermarkLineage is left zero: only the engine's own rebuild
// asks for it (loadSnapshot), since only a snapshot that may end up in a
// snapshot file needs one.
func LoadSnapshot(ctx context.Context, pgDriver *pg.Driver, pool *pgxpool.Pool) (*snapshot.Snapshot, error) {
	snap, _, err := loadSnapshot(ctx, pgDriver, pool, false, false)
	return snap, err
}

// loadedWatermark is what a rebuild's load read from bloodtrail_watermark
// inside its own repeatable-read transaction (loadSnapshot).
//
// counter, when counterOK, is the value the loaded rows are complete for as
// far as this process can account: every bump at or below it had committed
// before the transaction's snapshot was taken, so its write either had
// committed too -- and the rows hold it -- or had not, in which case a write
// of this process is still counted in inflightBumps and its Apply layers it
// onto whatever view is current then. That is what lets an adoption rebase
// the ledger to it (rebuildOnce). A write of ANOTHER process still in flight
// at that moment is the one thing it cannot cover; see watermarkLedger.
type loadedWatermark struct {
	counter   uint64
	counterOK bool

	// counterErr is why the counter could not be read. Reported whether or
	// not the lineage was asked for: without it the ledger is not rebased
	// (adoptRebuiltViewAndRebase), so a counter nothing else accounts for
	// stays unaccounted and every later save is refused as if another
	// BloodTrail server were writing (saveSnapshotProbe). That refusal's own
	// Warn cannot name this cause, so this one does.
	counterErr error

	// lineageErr is why the lineage could not be read, when it was asked
	// for; the snapshot then carries none. Only ever the lineage's own read:
	// the counter is read first, inside the same transaction, so its failure
	// aborts the transaction and is reported as counterErr rather than
	// blamed on a lineage read that never ran.
	lineageErr error
}

// loadSnapshot is LoadSnapshot, additionally reading the watermark counter
// (withCounter) and stamping the result's WatermarkLineage (withLineage;
// watermark.go's watermarkLineageDDL explains lineages) -- both inside the
// same repeatable-read transaction as the graph, so they describe the moment
// the scanned rows belong to. Read in a transaction of its own, before or
// after, the lineage could name one that ended while the load ran, or one
// that began after the rows were read; and the counter could vouch for
// writes the rows do not hold, or miss ones they do.
//
// They are the transaction's last statements, deliberately: on a database
// without the watermark table, or without its lineage column, the read
// fails, which aborts the transaction, so they can only go where nothing
// else still has to run in it. The counter is read first; the lineage only
// after it succeeded. A failed read fails nothing else -- the graph read is
// sound either way: a snapshot with no lineage is simply never written to a
// file, and one with no counter never rebases the ledger -- and each failure
// comes back as its own error (counterErr, lineageErr) for the caller to say
// what it costs, rather than one standing in for the other.
func loadSnapshot(ctx context.Context, pgDriver *pg.Driver, pool *pgxpool.Pool, withCounter, withLineage bool) (snap *snapshot.Snapshot, loaded loadedWatermark, err error) {
	graphModel, ok := pgDriver.DefaultGraph()
	if !ok {
		return nil, loaded, fmt.Errorf("engine: LoadSnapshot: no default graph is set")
	}

	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, loaded, fmt.Errorf("engine: LoadSnapshot: begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	builder := snapshot.NewBuilder(graphModel.ID)

	if err := loadKinds(ctx, tx, builder); err != nil {
		return nil, loaded, err
	}
	if err := loadNodes(ctx, tx, graphModel.ID, builder); err != nil {
		return nil, loaded, err
	}
	if err := loadEdges(ctx, tx, graphModel.ID, builder); err != nil {
		return nil, loaded, err
	}

	multiGraph, err := probeMultiGraph(ctx, tx)
	if err != nil {
		return nil, loaded, err
	}

	var lineage snapshot.Lineage
	if withCounter || withLineage {
		if err := tx.QueryRow(ctx, selectWatermarkCounterSQL).Scan(&loaded.counter); err != nil {
			loaded.counter = 0
			loaded.counterErr = fmt.Errorf("engine: LoadSnapshot: read watermark counter: %w", err)
		} else {
			loaded.counterOK = true
			if withLineage {
				var raw [16]byte
				if err := tx.QueryRow(ctx, selectWatermarkLineageSQL).Scan(&raw); err != nil {
					loaded.lineageErr = fmt.Errorf("engine: LoadSnapshot: read watermark lineage: %w", err)
				} else {
					lineage = snapshot.Lineage(raw)
				}
			}
		}
	}

	snap, err = builder.Build()
	if err != nil {
		return nil, loadedWatermark{}, fmt.Errorf("engine: LoadSnapshot: build snapshot: %w", err)
	}
	snap.MultiGraph = multiGraph
	snap.WatermarkLineage = lineage

	return snap, loaded, nil
}

// loadSnapshotFn is the load every rebuild runs (loadSnapshot, above,
// called from rebuildOnce in engine.go), a variable only so that a test can
// make that load fail or panic on demand -- the same seam, and the same
// reason, as boot.go's readSnapshotFile. Without it a test has to provoke an
// incidental crash instead (a nil pool's own nil-pointer dereference), which
// a later nil guard would silently turn into a test that no longer proves
// anything about the panic path it was written for.
var loadSnapshotFn = loadSnapshot

// loadKinds scans the entire `kind` table -- global, not scoped to any one
// graph_id -- into builder via Builder.SetKinds. It runs before loadNodes so
// the resulting Snapshot's Kinds table is populated regardless of which
// kinds this particular graph's nodes and edges use.
func loadKinds(ctx context.Context, tx pgx.Tx, builder *snapshot.Builder) error {
	rows, err := tx.Query(ctx, "SELECT id, name FROM kind")
	if err != nil {
		return fmt.Errorf("engine: LoadSnapshot: query kinds: %w", err)
	}
	defer rows.Close()

	pairs := make(map[snapshot.KindID]string)
	for rows.Next() {
		var (
			id   snapshot.KindID
			name string
		)
		if err := rows.Scan(&id, &name); err != nil {
			return fmt.Errorf("engine: LoadSnapshot: scan kind: %w", err)
		}
		pairs[id] = name
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("engine: LoadSnapshot: kind rows: %w", err)
	}

	builder.SetKinds(pairs)
	return nil
}

// pendingNode is one row loadNodes has scanned off the wire but not yet
// committed into the Builder: its cheap fields (id, kinds) alongside a
// buffered result channel a parse worker fills in once it has decoded
// propsJSON. See loadNodes's doc for the pipeline this is part of.
type pendingNode struct {
	databaseID uint64
	kindIDs    []snapshot.KindID
	propsJSON  []byte
	result     chan nodePropsParseResult
}

// nodePropsParseResult is a parse worker's outcome for one pendingNode:
// either the parsed property bag, or the error parsing it hit.
type nodePropsParseResult struct {
	parsed snapshot.ParsedProps
	err    error
}

// loadNodes streams every node of graphID, ordered by id, into builder. The
// ascending id order is required: it becomes the Builder's dense NodeID
// assignment.
//
// Each row's properties column (jsonb) is scanned directly into a []byte;
// pgx returns jsonb's wire text verbatim (already valid JSON, and, per
// pgtype's JSON codec, a fresh copy per row -- safe to keep past the next
// Scan call), so nothing needs stripping or re-encoding before it reaches
// snapshot.ParseProps. This keeps the row scan itself single-pass and
// strictly serial (required: it is the one thing here that cannot
// parallelize, since pgx.Rows is a single cursor) -- kinds and properties
// are both read off the same row, with no second query needed to backfill
// property bags.
//
// What *does* parallelize is the CPU-bound half of what used to be one
// synchronous Builder.AddNode call per row: parsing/validating each node's
// JSON property bag (snapshot.ParseProps) is pure and independent
// node-to-node, so it runs on a small worker pool fed by the row scan,
// while Builder.AddParsedNode -- the stateful commit into the Builder's
// shared arena and intern table, which must stay on one goroutine in
// strict ascending-id order -- runs on a single consumer goroutine that
// drains results in the exact order the rows were scanned. Concretely,
// three goroutine roles, wired by errgroup so a failure anywhere cancels
// the rest promptly instead of leaking or deadlocking:
//
//   - the producer (this call's own row-scan loop): for each row, builds a
//     pendingNode carrying a fresh 1-buffered result channel, sends it to
//     the workers via jobs, then sends the same pendingNode to order;
//   - a small pool of parse workers, each draining jobs and calling
//     snapshot.ParseProps on propsJSON, then delivering the outcome on that
//     pendingNode's own result channel (never blocking, since it is
//     1-buffered and only that one pendingNode is ever sent on it);
//   - the consumer: drains order (i.e. rows, in scan order), blocks on each
//     pendingNode's result, and calls builder.AddParsedNode -- the only
//     Builder call in this whole pipeline, so Builder's single-goroutine
//     contract is honored exactly as it was when AddNode was called
//     directly from this loop.
//
// Every one of those three roles runs through goRecovered, and each parse
// job through parseLoadedProps, so a panic anywhere in the pipeline becomes
// this function's error rather than the end of the process: errgroup does
// not propagate panics, and rebuildOnce's own recover only covers its own
// goroutine. See goRecovered for why that is safe here.
func loadNodes(ctx context.Context, tx pgx.Tx, graphID int32, builder *snapshot.Builder) error {
	rows, err := tx.Query(ctx, "SELECT id, kind_ids, properties FROM node WHERE graph_id = $1 ORDER BY id", graphID)
	if err != nil {
		return fmt.Errorf("engine: LoadSnapshot: query nodes: %w", err)
	}
	defer rows.Close()

	workers := runtime.GOMAXPROCS(0)
	if workers < 1 {
		workers = 1
	}
	const queueDepth = 4 // per worker, for both jobs and order

	jobs := make(chan *pendingNode, workers*queueDepth)
	order := make(chan *pendingNode, workers*queueDepth)

	g, gctx := errgroup.WithContext(ctx)

	goRecovered(g, "scanning node rows", func() error {
		defer close(jobs)
		defer close(order)

		for rows.Next() {
			var (
				id        int64
				kindIDs   []snapshot.KindID
				propsJSON []byte
			)
			if err := rows.Scan(&id, &kindIDs, &propsJSON); err != nil {
				return fmt.Errorf("engine: LoadSnapshot: scan node: %w", err)
			}

			p := &pendingNode{
				databaseID: uint64(id),
				kindIDs:    kindIDs,
				propsJSON:  propsJSON,
				result:     make(chan nodePropsParseResult, 1),
			}
			select {
			case jobs <- p:
			case <-gctx.Done():
				return gctx.Err()
			}
			select {
			case order <- p:
			case <-gctx.Done():
				return gctx.Err()
			}
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("engine: LoadSnapshot: node rows: %w", err)
		}
		return nil
	})

	for i := 0; i < workers; i++ {
		goRecovered(g, "parsing node properties", func() error {
			for p := range jobs {
				p.result <- parseLoadedProps(p.propsJSON)
			}
			return nil
		})
	}

	goRecovered(g, "staging nodes", func() error {
		for p := range order {
			res := <-p.result
			if res.err != nil {
				return fmt.Errorf("engine: LoadSnapshot: add node: databaseID %d: %w", p.databaseID, res.err)
			}
			if err := builder.AddParsedNode(p.databaseID, p.kindIDs, res.parsed); err != nil {
				return fmt.Errorf("engine: LoadSnapshot: add node: %w", err)
			}
		}
		return nil
	})

	return g.Wait()
}

// goRecovered runs fn as a member of g, turning a panic in it into the error
// g.Wait returns instead of letting it end the process.
//
// errgroup does not do this itself, and says why it will not (x/sync
// v0.22.0, errgroup.Go's own comment: a propagated panic is delayed,
// loses its stack, and can deadlock). For loadNodes those objections are
// answered by where it sits: a load runs on a background goroutine of the
// engine's own, under rebuildOnce, whose recover (recoverRebuildPanic,
// engine.go) already turns a panic in the load into a failed rebuild that
// retries and, for a write-driven one, a FALLBACK -- but only for a panic on
// ITS goroutine. A panic on one of this group's goroutines bypassed that and
// ended BloodHound. The stack is not lost: it is captured here, in the
// panicking goroutine, and carried in the error (panicLoadError).
//
// Deadlock is answered by each role's own shape rather than by this wrapper:
// fn's own defers run before this recover, so the producer still closes jobs
// and order on its way out; an error from the consumer cancels gctx, which
// is what the producer's sends select on; and a parse worker never panics
// with a node's result undelivered, because its panic is recovered one job
// at a time (parseLoadedProps) and delivered as that node's own parse error.
// So whichever role panics, the other two still reach the end of their
// channels and Wait returns.
func goRecovered(g *errgroup.Group, what string, fn func() error) {
	g.Go(func() (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = panicLoadError(what, r)
			}
		}()
		return fn()
	})
}

// parseLoadedProps is one parse worker's work for a single node: ParseProps,
// with a panic in it recovered into that node's own parse result rather than
// left to unwind the worker. The result is what keeps the pipeline from
// deadlocking: the consumer blocks on each node's result channel, so a
// worker that died mid-job would strand it (and the group's Wait with it)
// even with goRecovered above. Delivered as an error, the panic reaches the
// consumer, which returns it as this load's error exactly as a parse failure
// does.
func parseLoadedProps(propsJSON []byte) (res nodePropsParseResult) {
	defer func() {
		if r := recover(); r != nil {
			res = nodePropsParseResult{err: panicLoadError("parsing node properties", r)}
		}
	}()
	res.parsed, res.err = snapshot.ParseProps(propsJSON)
	return res
}

// loadPanicError is what a panic a load goroutine recovered becomes
// (panicLoadError): an ordinary load error for every caller that only
// reports or wraps it, and a DISTINGUISHABLE one for the retry cadence,
// which must not treat it as the transient failure it otherwise looks like.
//
// A panic recovered here never reaches recoverRebuildPanic (engine.go) --
// that is the point: it is returned, not re-raised -- so nothing sets
// Engine.rebuildPanicked for it, and without a type to test on
// loadRetryDelayAfter (boot.go) would put the most likely data-dependent
// panic of all, one in ParseProps or AddParsedNode, back on the doubling
// schedule: a whole snapshot load and a whole stack log every 30 s for the
// life of the process, which is exactly what that interval exists to stop.
// Callers wrap this with %w (loadSnapshot's own returns, rebuildOnce's
// "engine: RebuildNow: %w"), so errors.As finds it however deep it sits.
type loadPanicError struct {
	what  string
	value any
	stack []byte
}

// Error keeps the message panicLoadError always produced: what panicked, the
// panic value, and the stack of the goroutine it happened on.
func (e *loadPanicError) Error() string {
	return fmt.Sprintf("engine: LoadSnapshot: panic %s: %v\n%s", e.what, e.value, e.stack)
}

// panicLoadError describes a panic a load goroutine recovered, with the
// stack of the goroutine it happened on (captured here, while that stack is
// still live). It is an ordinary load error from there on, which is what
// every caller already knows how to handle -- see loadPanicError for the one
// caller that needs to know more than that.
func panicLoadError(what string, r any) error {
	return &loadPanicError{what: what, value: r, stack: debug.Stack()}
}

// loadEdges streams every edge of graphID into builder. Edges may arrive in
// any order; the Builder resolves and sorts them at Build time, dropping
// (and counting in the resulting Snapshot's DroppedEdges) any edge whose
// endpoint doesn't resolve to a node loadNodes staged.
func loadEdges(ctx context.Context, tx pgx.Tx, graphID int32, builder *snapshot.Builder) error {
	rows, err := tx.Query(ctx, "SELECT id, start_id, end_id, kind_id FROM edge WHERE graph_id = $1", graphID)
	if err != nil {
		return fmt.Errorf("engine: LoadSnapshot: query edges: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			id, start, end int64
			kind           snapshot.KindID
		)
		if err := rows.Scan(&id, &start, &end, &kind); err != nil {
			return fmt.Errorf("engine: LoadSnapshot: scan edge: %w", err)
		}
		builder.AddEdge(uint64(id), uint64(start), uint64(end), kind)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("engine: LoadSnapshot: edge rows: %w", err)
	}

	return nil
}

// probeMultiGraph reports whether the database holds two or more distinct
// graphs that each have at least one node, for Snapshot.MultiGraph. The
// LIMIT 2 keeps this O(1) regardless of how many graphs or nodes exist: the
// query stops as soon as a second qualifying graph is found, so it never
// scans the full `graph` or `node` tables on a large, single-graph database.
func probeMultiGraph(ctx context.Context, tx pgx.Tx) (bool, error) {
	rows, err := tx.Query(ctx, "SELECT g.id FROM graph g WHERE EXISTS (SELECT 1 FROM node n WHERE n.graph_id = g.id) LIMIT 2")
	if err != nil {
		return false, fmt.Errorf("engine: LoadSnapshot: query multi-graph probe: %w", err)
	}
	defer rows.Close()

	graphsWithNodes := 0
	for rows.Next() {
		var id int32
		if err := rows.Scan(&id); err != nil {
			return false, fmt.Errorf("engine: LoadSnapshot: scan multi-graph probe: %w", err)
		}
		graphsWithNodes++
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("engine: LoadSnapshot: multi-graph probe rows: %w", err)
	}

	return graphsWithNodes >= 2, nil
}
