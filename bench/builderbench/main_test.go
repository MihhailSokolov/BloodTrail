// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/specterops/dawgs/graph"
)

// TestRatioOf table-tests ratioOf's pure delegated/served speed-ratio
// arithmetic, including its guarded (never divide-by-zero) edge cases --
// see ratioOf's doc.
func TestRatioOf(t *testing.T) {
	cases := []struct {
		name      string
		btP50     time.Duration
		pgP50     time.Duration
		want      float64
		wantIsInf bool
	}{
		{name: "typical 5x", btP50: 10 * time.Millisecond, pgP50: 50 * time.Millisecond, want: 5},
		{name: "typical 1.2x", btP50: 100 * time.Millisecond, pgP50: 120 * time.Millisecond, want: 1.2},
		{name: "equal is 1x", btP50: 10 * time.Millisecond, pgP50: 10 * time.Millisecond, want: 1},
		{name: "both zero declared equal", btP50: 0, pgP50: 0, want: 1},
		{name: "bt zero pg positive is +Inf", btP50: 0, pgP50: 10 * time.Millisecond, wantIsInf: true},
		{name: "bt negative treated like zero", btP50: -1, pgP50: 10 * time.Millisecond, wantIsInf: true},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := ratioOf(c.btP50, c.pgP50)
			if c.wantIsInf {
				if !math.IsInf(got, 1) {
					t.Fatalf("ratioOf(%s, %s) = %v, want +Inf", c.btP50, c.pgP50, got)
				}
				return
			}
			if got != c.want {
				t.Fatalf("ratioOf(%s, %s) = %v, want %v", c.btP50, c.pgP50, got, c.want)
			}
		})
	}
}

// TestEvaluateShape table-tests evaluateShape, -enforce's pure per-shape
// decision function, across every branch its doc describes: an uncapped
// shape's ratio and match checks (independently and together), the
// per-shape threshold difference (fetch_directed_graph_memberof's 1.2x vs
// the standard 5x), and the pg_capped path's absolute-bound-only judgment
// that skips ratio/match entirely.
func TestEvaluateShape(t *testing.T) {
	fiveX := shapeThreshold{minRatio: 5.0, engineAbsoluteCap: 5 * time.Second}
	oneTwoX := shapeThreshold{minRatio: 1.2, engineAbsoluteCap: 15 * time.Second}

	const ms = time.Millisecond

	cases := []struct {
		name         string
		btP50, pgP50 time.Duration
		matchChecked bool
		match        bool
		pgCapped     bool
		th           shapeThreshold
		wantOK       bool
		wantReasons  int
	}{
		{
			name:  "uncapped: passes at exactly the ratio bar and matching sizes",
			btP50: 10 * ms, pgP50: 50 * ms, matchChecked: true, match: true, pgCapped: false,
			th: fiveX, wantOK: true, wantReasons: 0,
		},
		{
			name:  "uncapped: fails when ratio is just below the bar",
			btP50: 10 * ms, pgP50: 49 * ms, matchChecked: true, match: true, pgCapped: false,
			th: fiveX, wantOK: false, wantReasons: 1,
		},
		{
			name:  "uncapped: fails on size mismatch even with a comfortable ratio",
			btP50: 10 * ms, pgP50: 100 * ms, matchChecked: true, match: false, pgCapped: false,
			th: fiveX, wantOK: false, wantReasons: 1,
		},
		{
			name:  "uncapped: fails on both mismatch and ratio, reporting both reasons",
			btP50: 10 * ms, pgP50: 10 * ms, matchChecked: true, match: false, pgCapped: false,
			th: fiveX, wantOK: false, wantReasons: 2,
		},
		{
			name:  "uncapped: matchChecked false fails even when match is (spuriously) true",
			btP50: 10 * ms, pgP50: 50 * ms, matchChecked: false, match: true, pgCapped: false,
			th: fiveX, wantOK: false, wantReasons: 1,
		},
		{
			name:  "uncapped: fetch_directed_graph's 1.2x bar passes where 5x would have failed",
			btP50: 100 * ms, pgP50: 130 * ms, matchChecked: true, match: true, pgCapped: false,
			th: oneTwoX, wantOK: true, wantReasons: 0,
		},
		{
			name:  "uncapped: fetch_directed_graph still fails below its own 1.2x bar",
			btP50: 100 * ms, pgP50: 110 * ms, matchChecked: true, match: true, pgCapped: false,
			th: oneTwoX, wantOK: false, wantReasons: 1,
		},
		{
			name:  "capped: passes when engine p50 clears the absolute cap, ignoring match/ratio entirely",
			btP50: 3 * time.Second, pgP50: 0, matchChecked: false, match: false, pgCapped: true,
			th: fiveX, wantOK: true, wantReasons: 0,
		},
		{
			name:  "capped: passes even if match/ratio inputs look bad -- they must be ignored",
			btP50: 3 * time.Second, pgP50: 1 * ms, matchChecked: true, match: false, pgCapped: true,
			th: fiveX, wantOK: true, wantReasons: 0,
		},
		{
			name:  "capped: fails when engine p50 exceeds the absolute cap",
			btP50: 6 * time.Second, pgP50: 0, matchChecked: false, match: false, pgCapped: true,
			th: fiveX, wantOK: false, wantReasons: 1,
		},
		{
			name:  "capped: exactly at the cap passes (strict greater-than only)",
			btP50: 5 * time.Second, pgP50: 0, matchChecked: false, match: false, pgCapped: true,
			th: fiveX, wantOK: true, wantReasons: 0,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ok, reasons := evaluateShape(c.btP50, c.pgP50, c.matchChecked, c.match, c.pgCapped, c.th)
			if ok != c.wantOK {
				t.Errorf("ok = %v, want %v (reasons: %v)", ok, c.wantOK, reasons)
			}
			if len(reasons) != c.wantReasons {
				t.Errorf("len(reasons) = %d, want %d (reasons: %v)", len(reasons), c.wantReasons, reasons)
			}
		})
	}
}

// TestThresholdFor confirms every shape actually benchmarked by execute
// (the shapes slice) has a shapeThresholds entry -- catching the
// maintenance bug thresholdFor's own doc warns about (a shapeSpec added
// without a matching threshold) at test time rather than via a stderr
// warning at run time.
func TestThresholdFor_KnownShapesHaveEntries(t *testing.T) {
	knownShapes := []string{
		"fetch_directed_graph_memberof",
		"group_members_bfs",
		"node_count_user",
		"node_fetchids_user",
		"delete_transit_edges_admin_to",
	}
	for _, name := range knownShapes {
		if _, ok := shapeThresholds[name]; !ok {
			t.Errorf("shapeThresholds has no entry for %q", name)
		}
	}
}

func TestThresholdFor_UnknownShapeFallsBackSafely(t *testing.T) {
	th := thresholdFor("some_shape_that_does_not_exist")
	if th != defaultShapeThreshold {
		t.Errorf("thresholdFor(unknown) = %+v, want defaultShapeThreshold %+v", th, defaultShapeThreshold)
	}
}

// TestFetchDirectedGraphHasWeakerBar locks in this shape's deliberately
// weaker bar: fetch_directed_graph_memberof's minRatio must be less than
// every other shape's, and specifically 1.2x while the others stay at
// enforceRatio (5x).
func TestFetchDirectedGraphHasWeakerBar(t *testing.T) {
	fdg, ok := shapeThresholds["fetch_directed_graph_memberof"]
	if !ok {
		t.Fatal("fetch_directed_graph_memberof missing from shapeThresholds")
	}
	if fdg.minRatio != fetchDirectedGraphMinRatio {
		t.Errorf("fetch_directed_graph_memberof.minRatio = %v, want %v", fdg.minRatio, fetchDirectedGraphMinRatio)
	}

	for name, th := range shapeThresholds {
		if name == "fetch_directed_graph_memberof" {
			continue
		}
		if th.minRatio != enforceRatio {
			t.Errorf("%s.minRatio = %v, want the standard enforceRatio %v", name, th.minRatio, enforceRatio)
		}
		if th.minRatio <= fdg.minRatio {
			t.Errorf("%s.minRatio (%v) should be strictly greater than fetch_directed_graph_memberof's (%v)", name, th.minRatio, fdg.minRatio)
		}
	}
}

// TestMeasuredEngineAbsoluteCaps pins the two evidence-based caps for the
// traversal-heavy shapes to their documented worst-case-p50 x ~1.75 values
// (see shapeThresholds' doc and the README's cap-rationale table). These
// values are measurement-derived; changing one requires fresh 5M-scale
// evidence recorded alongside the change, so a silent edit fails here.
func TestMeasuredEngineAbsoluteCaps(t *testing.T) {
	want := map[string]time.Duration{
		"fetch_directed_graph_memberof": 80 * time.Second, // worst measured bt p50 45.3s x ~1.75
		"group_members_bfs":             95 * time.Second, // worst measured bt p50 54.2s x ~1.75
	}
	for name, cap := range want {
		th, ok := shapeThresholds[name]
		if !ok {
			t.Fatalf("%s missing from shapeThresholds", name)
		}
		if th.engineAbsoluteCap != cap {
			t.Errorf("%s.engineAbsoluteCap = %v, want %v", name, th.engineAbsoluteCap, cap)
		}
	}
}

// TestRunPGCapped_CutsOffASlowQuery exercises runPGCapped's core contract
// -- a query exceeding pgCap is cut off and reported as capped=true,
// nil-err -- without a database: run's closure below simulates a slow pg
// query by blocking on ctx.Done() or a timer, exactly the shape a real
// dawgs pg call takes (the caller's context determines whether the call
// returns early). oracle is left nil since this run func never touches it
// -- graph.Database is an interface, so a nil value is safe to pass
// through unused.
func TestRunPGCapped_CutsOffASlowQuery(t *testing.T) {
	slowRun := func(ctx context.Context, _ graph.Database) (int64, error) {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(200 * time.Millisecond):
			return 99, nil
		}
	}

	d, size, capped, err := runPGCapped(context.Background(), nil, slowRun, 20*time.Millisecond)
	if err != nil {
		t.Fatalf("runPGCapped returned err %v, want nil (a capped run is not an error)", err)
	}
	if !capped {
		t.Fatalf("capped = false, want true (pgCap should have cut the 200ms run off at 20ms)")
	}
	if d != 0 || size != 0 {
		t.Errorf("d=%s size=%d, want both zero-valued on a capped run", d, size)
	}
}

// TestRunPGCapped_FastQueryIsNotCapped confirms a query that finishes
// comfortably inside pgCap is reported normally: capped=false, and its
// real duration/size come through.
func TestRunPGCapped_FastQueryIsNotCapped(t *testing.T) {
	fastRun := func(ctx context.Context, _ graph.Database) (int64, error) {
		return 42, nil
	}

	d, size, capped, err := runPGCapped(context.Background(), nil, fastRun, time.Second)
	if err != nil {
		t.Fatalf("runPGCapped returned err %v, want nil", err)
	}
	if capped {
		t.Fatalf("capped = true, want false (the run finished well inside pgCap)")
	}
	if size != 42 {
		t.Errorf("size = %d, want 42", size)
	}
	if d < 0 {
		t.Errorf("d = %s, want a non-negative duration", d)
	}
}

// TestRunPGCapped_GenuineErrorPropagates confirms a real failure (anything
// other than the cap's own context deadline) is still returned as an error
// -- runPGCapped must not swallow a genuine driver error just because it
// happens to look at ctx.Err() first.
func TestRunPGCapped_GenuineErrorPropagates(t *testing.T) {
	boom := errors.New("boom: a genuine driver failure, not a timeout")
	failingRun := func(ctx context.Context, _ graph.Database) (int64, error) {
		return 0, boom
	}

	_, _, capped, err := runPGCapped(context.Background(), nil, failingRun, time.Second)
	if capped {
		t.Fatalf("capped = true, want false: this failure has nothing to do with the wall-clock cap")
	}
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want it to wrap %v", err, boom)
	}
}

// TestRunPGCapped_ParentCancellationIsNotMistakenForACap confirms that
// runPGCapped distinguishes its own pgCap deadline from the caller's
// ctx being canceled for an unrelated reason: canceling ctx up front
// (rather than letting pgCap itself expire) must propagate as a genuine
// error, not report capped=true, since evaluateShape's pg_capped path
// changes what gets judged and by which bound -- conflating "someone
// canceled the whole benchmark" with "this shape's pg baseline was too
// slow" would misreport why a run stopped.
func TestRunPGCapped_ParentCancellationIsNotMistakenForACap(t *testing.T) {
	parentCtx, cancel := context.WithCancel(context.Background())
	cancel() // already canceled before runPGCapped even starts

	run := func(ctx context.Context, _ graph.Database) (int64, error) {
		<-ctx.Done()
		return 0, ctx.Err()
	}

	_, _, capped, err := runPGCapped(parentCtx, nil, run, time.Second)
	if capped {
		t.Fatalf("capped = true, want false: the parent context was canceled, not runPGCapped's own pgCap deadline")
	}
	if err == nil {
		t.Fatal("err = nil, want the propagated cancellation error")
	}
}

// TestRunBTCapped_CutsOffASlowRunAndFails exercises runBTCapped's core
// contract -- unlike runPGCapped, an engine-side run exceeding btCap is
// never a graceful, recordable outcome: it is the failure mode a 2026-09
// benchmark incident exposed (a decline silently delegating to an unbounded
// PostgreSQL query, which then ran for 17.5 hours before being killed by
// hand), so runBTCapped must return a hard, descriptive error
// naming both the shape and -bt-cap itself, not a "capped" bool a caller
// could mistake for a data point.
func TestRunBTCapped_CutsOffASlowRunAndFails(t *testing.T) {
	slowRun := func(ctx context.Context, _ graph.Database) (int64, error) {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(200 * time.Millisecond):
			return 99, nil
		}
	}

	d, size, err := runBTCapped(context.Background(), nil, slowRun, 20*time.Millisecond, "some_shape")
	if err == nil {
		t.Fatal("runBTCapped returned nil err, want a hard failure (bt-cap must abort the run, never silently record a capped data point)")
	}
	if d != 0 || size != 0 {
		t.Errorf("d=%s size=%d, want both zero-valued on a bt-cap failure", d, size)
	}
	if !strings.Contains(err.Error(), "some_shape") {
		t.Errorf("err = %q, want it to name the shape %q", err.Error(), "some_shape")
	}
	if !strings.Contains(err.Error(), "bt-cap") {
		t.Errorf("err = %q, want it to mention -bt-cap", err.Error())
	}
}

// TestRunBTCapped_FastRunIsNotCapped confirms a run that finishes
// comfortably inside btCap is reported normally: no error, real
// duration/size passed through.
func TestRunBTCapped_FastRunIsNotCapped(t *testing.T) {
	fastRun := func(ctx context.Context, _ graph.Database) (int64, error) {
		return 42, nil
	}

	d, size, err := runBTCapped(context.Background(), nil, fastRun, time.Second, "some_shape")
	if err != nil {
		t.Fatalf("runBTCapped returned err %v, want nil", err)
	}
	if size != 42 {
		t.Errorf("size = %d, want 42", size)
	}
	if d < 0 {
		t.Errorf("d = %s, want a non-negative duration", d)
	}
}

// TestRunBTCapped_GenuineErrorPropagates confirms a real failure (anything
// other than btCap's own context deadline) is still returned as itself --
// runBTCapped must not rewrite a genuine driver error into the misleading
// "engine likely declined" bt-cap message just because it happens to look
// at ctx.Err() first.
func TestRunBTCapped_GenuineErrorPropagates(t *testing.T) {
	boom := errors.New("boom: a genuine driver failure, not a timeout")
	failingRun := func(ctx context.Context, _ graph.Database) (int64, error) {
		return 0, boom
	}

	_, _, err := runBTCapped(context.Background(), nil, failingRun, time.Second, "some_shape")
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want it to wrap %v", err, boom)
	}
}

// TestRunBTCapped_ParentCancellationIsNotMistakenForACap confirms that
// runBTCapped distinguishes its own btCap deadline from the caller's ctx
// being canceled for an unrelated reason (e.g. the whole process shutting
// down): canceling ctx up front must propagate as the genuine
// cancellation, not the "-bt-cap exceeded" message -- misreporting an
// unrelated shutdown as "the engine likely declined" would send whoever
// reads it chasing the wrong thing. Mirrors runPGCapped's own
// identically-purposed test.
func TestRunBTCapped_ParentCancellationIsNotMistakenForACap(t *testing.T) {
	parentCtx, cancel := context.WithCancel(context.Background())
	cancel() // already canceled before runBTCapped even starts

	run := func(ctx context.Context, _ graph.Database) (int64, error) {
		<-ctx.Done()
		return 0, ctx.Err()
	}

	_, _, err := runBTCapped(parentCtx, nil, run, time.Second, "some_shape")
	if err == nil {
		t.Fatal("err = nil, want the propagated cancellation error")
	}
	if strings.Contains(err.Error(), "bt-cap") {
		t.Errorf("err = %q, want the plain cancellation error, not the bt-cap message (the parent context was canceled, not runBTCapped's own btCap deadline)", err.Error())
	}
}
