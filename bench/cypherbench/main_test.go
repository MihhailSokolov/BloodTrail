// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/specterops/dawgs/graph"
)

// TestRatioOf table-tests ratioOf's pure delegated/served speed-ratio
// arithmetic, including its guarded (never divide-by-zero) edge cases --
// mirroring bench/builderbench's identical test for its own ratioOf.
func TestRatioOf(t *testing.T) {
	cases := []struct {
		name      string
		btP50     time.Duration
		pgP50     time.Duration
		want      float64
		wantIsInf bool
	}{
		{name: "typical 5x", btP50: 10 * time.Millisecond, pgP50: 50 * time.Millisecond, want: 5},
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
// per-shape threshold difference (a 5x bar vs. objectid_point_lookup's 1x),
// and the pg_capped path's absolute-bound-only judgment that skips
// ratio/match entirely -- mirroring bench/builderbench's identically
// structured TestEvaluateShape for its own pg-capped evaluateShape.
func TestEvaluateShape(t *testing.T) {
	fiveX := shapeThreshold{minRatio: 5.0, engineAbsoluteCap: 5 * time.Second}
	oneX := shapeThreshold{minRatio: 1.0, engineAbsoluteCap: 1 * time.Second}

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
			name:  "uncapped: passes at exactly the ratio bar and matching counts",
			btP50: 10 * ms, pgP50: 50 * ms, matchChecked: true, match: true, pgCapped: false,
			th: fiveX, wantOK: true, wantReasons: 0,
		},
		{
			name:  "uncapped: fails when ratio is just below the bar",
			btP50: 10 * ms, pgP50: 49 * ms, matchChecked: true, match: true, pgCapped: false,
			th: fiveX, wantOK: false, wantReasons: 1,
		},
		{
			name:  "uncapped: fails on count mismatch even with a comfortable ratio",
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
			name:  "uncapped: objectid point lookup's 1x bar passes where 5x would have failed",
			btP50: 10 * ms, pgP50: 12 * ms, matchChecked: true, match: true, pgCapped: false,
			th: oneX, wantOK: true, wantReasons: 0,
		},
		{
			name:  "uncapped: objectid point lookup still fails below its own 1x bar",
			btP50: 10 * ms, pgP50: 9 * ms, matchChecked: true, match: true, pgCapped: false,
			th: oneX, wantOK: false, wantReasons: 1,
		},
		{
			name:  "uncapped: exactly at the ratio bar passes (>=, not strictly greater)",
			btP50: 10 * ms, pgP50: 10 * ms, matchChecked: true, match: true, pgCapped: false,
			th: oneX, wantOK: true, wantReasons: 0,
		},
		{
			name:  "capped: passes when engine p50 clears the absolute cap, ignoring match/ratio entirely",
			btP50: 500 * ms, pgP50: 0, matchChecked: false, match: false, pgCapped: true,
			th: fiveX, wantOK: true, wantReasons: 0,
		},
		{
			name:  "capped: passes even if match/ratio inputs look bad -- they must be ignored",
			btP50: 500 * ms, pgP50: 1 * ms, matchChecked: true, match: false, pgCapped: true,
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

// TestThresholdForKnownShapesHaveEntries confirms every shape execute
// actually benchmarks has a shapeThresholds entry -- the maintenance bug
// thresholdFor's own doc warns about, caught at test time rather than via a
// stderr warning at run time (mirroring bench/builderbench's identical
// TestThresholdFor_KnownShapesHaveEntries).
func TestThresholdForKnownShapesHaveEntries(t *testing.T) {
	knownShapes := []string{
		shapeRIDSuffixScan,
		shapeFlagScan,
		shapeObjectIDPointLookup,
		shapeShortestPathPrebuilt,
		shapeCollectAntiJoinPrebuilt,
	}
	for _, name := range knownShapes {
		if _, ok := shapeThresholds[name]; !ok {
			t.Errorf("shapeThresholds has no entry for %q", name)
		}
	}
}

// TestThresholdForUnknownShapeFallsBackSafely mirrors
// bench/builderbench's identical test: a shape name absent from
// shapeThresholds (a maintenance bug, not a runtime condition normal
// operation should reach) must fall back to defaultShapeThreshold rather
// than panicking mid-report.
func TestThresholdForUnknownShapeFallsBackSafely(t *testing.T) {
	th := thresholdFor("some_shape_that_does_not_exist")
	if th != defaultShapeThreshold {
		t.Errorf("thresholdFor(unknown) = %+v, want defaultShapeThreshold %+v", th, defaultShapeThreshold)
	}
}

// TestPerShapeRatioBars locks in the measured per-shape intent:
// objectid_point_lookup needs at least 1x (a point lookup is fast on both
// sides -- see the package doc); rid_suffix_scan and flag_scan carry their
// own measured-physics bars (see ridSuffixScanMinRatio's and
// flagScanMinRatio's docs -- both derived from worst-case honest 5M
// measurements, both still requiring the engine to be strictly faster);
// and the two pre-built traversal shapes need exactly the standard 5x.
// These are measurement-derived values: changing one requires fresh
// 5M-scale evidence recorded alongside the change.
func TestPerShapeRatioBars(t *testing.T) {
	pointLookup, ok := shapeThresholds[shapeObjectIDPointLookup]
	if !ok {
		t.Fatal("objectid_point_lookup missing from shapeThresholds")
	}
	if pointLookup.minRatio < 1.0 {
		t.Errorf("shapeThresholds[objectid_point_lookup].minRatio = %v, want >= 1.0", pointLookup.minRatio)
	}

	perShape := map[string]float64{
		shapeRIDSuffixScan: ridSuffixScanMinRatio, // measured 1.31x idle / 1.38-2.03x loaded
		shapeFlagScan:      flagScanMinRatio,      // measured 1.87x idle / 1.29-2.29x loaded
	}
	for name, want := range perShape {
		th, ok := shapeThresholds[name]
		if !ok {
			t.Fatalf("%s missing from shapeThresholds", name)
		}
		if th.minRatio != want {
			t.Errorf("shapeThresholds[%s].minRatio = %v, want %v", name, th.minRatio, want)
		}
		if th.minRatio <= 1.0 {
			t.Errorf("shapeThresholds[%s].minRatio = %v; a measured-physics bar must still require the engine to be strictly faster (> 1.0)", name, th.minRatio)
		}
	}

	for name, th := range shapeThresholds {
		switch name {
		case shapeObjectIDPointLookup, shapeRIDSuffixScan, shapeFlagScan:
			continue
		default:
			if th.minRatio != enforceRatio {
				t.Errorf("%s minRatio = %v, want the standard enforceRatio %v", name, th.minRatio, enforceRatio)
			}
		}
	}
}

// fakeResult is a minimal graph.Result test double for
// runPGCypherCapped's fast-query test: rows counts down from n to 0 across
// successive Next() calls, mimicking runCypherOnce's row-drain loop without
// a real query. The embedded nil graph.Result means every method besides
// Next/Error/Close panics (a nil-interface method call) if ever reached --
// runCypherOnce never calls them, so a panic here would mean this fake
// leaked into a code path it wasn't meant to cover.
type fakeResult struct {
	graph.Result
	rows int
	err  error
}

func (r *fakeResult) Next() bool {
	if r.rows > 0 {
		r.rows--
		return true
	}
	return false
}

func (r *fakeResult) Error() error { return r.err }
func (r *fakeResult) Close()       {}

// fakeTransaction is a minimal graph.Transaction test double: Query returns
// a fixed graph.Result regardless of its arguments, since these tests only
// care about the row count runCypherOnce derives from draining it. Every
// other method is left unimplemented via the embedded nil
// graph.Transaction, matching fakeResult's rationale above.
type fakeTransaction struct {
	graph.Transaction
	result graph.Result
}

func (f fakeTransaction) Query(string, map[string]any) graph.Result { return f.result }

// fakeOracle is a minimal graph.Database test double for
// runPGCypherCapped's tests: only ReadTransaction is exercised by
// runCypherOnce (the function runPGCypherCapped wraps), via the injected
// readTransaction closure -- exactly the role builderbench's own test
// closures (passed straight to its generic runPGCapped) play, one layer
// down here since cypherbench's runPGCypherCapped has no injectable run
// parameter of its own (see its doc) and always calls runCypherOnce
// internally. Every other graph.Database method is left unimplemented via
// the embedded nil interface, matching fakeResult/fakeTransaction's
// rationale.
type fakeOracle struct {
	graph.Database
	readTransaction func(ctx context.Context, txDelegate graph.TransactionDelegate) error
}

func (f fakeOracle) ReadTransaction(ctx context.Context, txDelegate graph.TransactionDelegate, _ ...graph.TransactionOption) error {
	return f.readTransaction(ctx, txDelegate)
}

// TestRunPGCypherCapped_CutsOffASlowQuery exercises runPGCypherCapped's
// core contract -- a query exceeding pgCap is cut off and reported as
// capped=true, nil-err -- without a database: fakeOracle's readTransaction
// closure below simulates a slow pg query by blocking on ctx.Done() or a
// timer, exactly the shape a real dawgs pg call takes (the caller's context
// determines whether the call returns early). Mirrors
// bench/builderbench's TestRunPGCapped_CutsOffASlowQuery.
func TestRunPGCypherCapped_CutsOffASlowQuery(t *testing.T) {
	oracle := fakeOracle{readTransaction: func(ctx context.Context, _ graph.TransactionDelegate) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
			return nil
		}
	}}

	size, d, capped, err := runPGCypherCapped(context.Background(), oracle, "MATCH (n) RETURN n", 20*time.Millisecond)
	if err != nil {
		t.Fatalf("runPGCypherCapped returned err %v, want nil (a capped run is not an error)", err)
	}
	if !capped {
		t.Fatalf("capped = false, want true (pgCap should have cut the 200ms run off at 20ms)")
	}
	if size != 0 || d != 0 {
		t.Errorf("size=%d d=%s, want both zero-valued on a capped run", size, d)
	}
}

// TestRunPGCypherCapped_FastQueryIsNotCapped confirms a query that finishes
// comfortably inside pgCap is reported normally: capped=false, and its real
// duration/row count come through -- fakeOracle's readTransaction here
// actually invokes the txDelegate against a 3-row fakeResult, so size's
// pass-through is checked against a real, non-zero value rather than just
// its zero-ness.
func TestRunPGCypherCapped_FastQueryIsNotCapped(t *testing.T) {
	oracle := fakeOracle{readTransaction: func(_ context.Context, txDelegate graph.TransactionDelegate) error {
		return txDelegate(fakeTransaction{result: &fakeResult{rows: 3}})
	}}

	size, d, capped, err := runPGCypherCapped(context.Background(), oracle, "MATCH (n) RETURN n", time.Second)
	if err != nil {
		t.Fatalf("runPGCypherCapped returned err %v, want nil", err)
	}
	if capped {
		t.Fatalf("capped = true, want false (the run finished well inside pgCap)")
	}
	if size != 3 {
		t.Errorf("size = %d, want 3", size)
	}
	if d < 0 {
		t.Errorf("d = %s, want a non-negative duration", d)
	}
}

// TestRunPGCypherCapped_GenuineErrorPropagates confirms a real failure
// (anything other than the cap's own context deadline) is still returned
// as an error -- runPGCypherCapped must not swallow a genuine driver error
// just because it happens to look at ctx.Err() first.
func TestRunPGCypherCapped_GenuineErrorPropagates(t *testing.T) {
	boom := errors.New("boom: a genuine driver failure, not a timeout")
	oracle := fakeOracle{readTransaction: func(context.Context, graph.TransactionDelegate) error {
		return boom
	}}

	_, _, capped, err := runPGCypherCapped(context.Background(), oracle, "MATCH (n) RETURN n", time.Second)
	if capped {
		t.Fatalf("capped = true, want false: this failure has nothing to do with the wall-clock cap")
	}
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want it to wrap %v", err, boom)
	}
}

// TestRunPGCypherCapped_ParentCancellationIsNotMistakenForACap confirms
// that runPGCypherCapped distinguishes its own pgCap deadline from the
// caller's ctx being canceled for an unrelated reason: canceling ctx up
// front (rather than letting pgCap itself expire) must propagate as a
// genuine error, not report capped=true, since evaluateShape's pg_capped
// path changes what gets judged and by which bound -- conflating "someone
// canceled the whole benchmark" with "this shape's pg baseline was too
// slow" would misreport why a run stopped.
func TestRunPGCypherCapped_ParentCancellationIsNotMistakenForACap(t *testing.T) {
	parentCtx, cancel := context.WithCancel(context.Background())
	cancel() // already canceled before runPGCypherCapped even starts

	oracle := fakeOracle{readTransaction: func(ctx context.Context, _ graph.TransactionDelegate) error {
		<-ctx.Done()
		return ctx.Err()
	}}

	_, _, capped, err := runPGCypherCapped(parentCtx, oracle, "MATCH (n) RETURN n", time.Second)
	if capped {
		t.Fatalf("capped = true, want false: the parent context was canceled, not runPGCypherCapped's own pgCap deadline")
	}
	if err == nil {
		t.Fatal("err = nil, want the propagated cancellation error")
	}
}

// fakeBT is a minimal graph.Database test double for runCypherOnceCapped's
// tests -- structurally identical to fakeOracle (same embedded-nil-panics-
// if-reached rationale), named separately so a reader sees at a glance
// which side of the bt/oracle distinction a given test's fake stands in
// for.
type fakeBT struct {
	graph.Database
	readTransaction func(ctx context.Context, txDelegate graph.TransactionDelegate) error
}

func (f fakeBT) ReadTransaction(ctx context.Context, txDelegate graph.TransactionDelegate, _ ...graph.TransactionOption) error {
	return f.readTransaction(ctx, txDelegate)
}

// TestRunCypherOnceCapped_CutsOffASlowRunAndFails exercises
// runCypherOnceCapped's core contract -- unlike runPGCypherCapped, an
// engine-side run exceeding btCap is never a graceful, recordable outcome:
// it is the failure mode a 2026-09 benchmark incident exposed (a decline
// silently delegating to an unbounded PostgreSQL query, which then ran for
// 17.5 hours before being killed by hand), so runCypherOnceCapped must
// return a hard, descriptive error naming both the shape and -bt-cap
// itself, not a "capped" bool a caller could mistake for a data point.
func TestRunCypherOnceCapped_CutsOffASlowRunAndFails(t *testing.T) {
	bt := fakeBT{readTransaction: func(ctx context.Context, _ graph.TransactionDelegate) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
			return nil
		}
	}}

	d, size, err := runCypherOnceCapped(context.Background(), bt, "MATCH (n) RETURN n", 20*time.Millisecond, "some_shape")
	if err == nil {
		t.Fatal("runCypherOnceCapped returned nil err, want a hard failure (bt-cap must abort the run, never silently record a capped data point)")
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

// TestRunCypherOnceCapped_FastRunIsNotCapped confirms a run that finishes
// comfortably inside btCap is reported normally: no error, real
// duration/size passed through.
func TestRunCypherOnceCapped_FastRunIsNotCapped(t *testing.T) {
	bt := fakeBT{readTransaction: func(_ context.Context, txDelegate graph.TransactionDelegate) error {
		return txDelegate(fakeTransaction{result: &fakeResult{rows: 3}})
	}}

	d, size, err := runCypherOnceCapped(context.Background(), bt, "MATCH (n) RETURN n", time.Second, "some_shape")
	if err != nil {
		t.Fatalf("runCypherOnceCapped returned err %v, want nil", err)
	}
	if size != 3 {
		t.Errorf("size = %d, want 3", size)
	}
	if d < 0 {
		t.Errorf("d = %s, want a non-negative duration", d)
	}
}

// TestRunCypherOnceCapped_GenuineErrorPropagates confirms a real failure
// (anything other than btCap's own context deadline) is still returned as
// itself -- runCypherOnceCapped must not rewrite a genuine driver error
// into the misleading "engine likely declined" bt-cap message just because
// it happens to look at ctx.Err() first.
func TestRunCypherOnceCapped_GenuineErrorPropagates(t *testing.T) {
	boom := errors.New("boom: a genuine driver failure, not a timeout")
	bt := fakeBT{readTransaction: func(context.Context, graph.TransactionDelegate) error {
		return boom
	}}

	_, _, err := runCypherOnceCapped(context.Background(), bt, "MATCH (n) RETURN n", time.Second, "some_shape")
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want it to wrap %v", err, boom)
	}
}

// TestRunCypherOnceCapped_ParentCancellationIsNotMistakenForACap confirms
// that runCypherOnceCapped distinguishes its own btCap deadline from the
// caller's ctx being canceled for an unrelated reason (e.g. the whole
// process shutting down): canceling ctx up front must propagate as the
// genuine cancellation, not the "-bt-cap exceeded" message -- misreporting
// an unrelated shutdown as "the engine likely declined" would send whoever
// reads it chasing the wrong thing. Mirrors runPGCypherCapped's own
// identically-purposed test.
func TestRunCypherOnceCapped_ParentCancellationIsNotMistakenForACap(t *testing.T) {
	parentCtx, cancel := context.WithCancel(context.Background())
	cancel() // already canceled before runCypherOnceCapped even starts

	bt := fakeBT{readTransaction: func(ctx context.Context, _ graph.TransactionDelegate) error {
		<-ctx.Done()
		return ctx.Err()
	}}

	_, _, err := runCypherOnceCapped(parentCtx, bt, "MATCH (n) RETURN n", time.Second, "some_shape")
	if err == nil {
		t.Fatal("err = nil, want the propagated cancellation error")
	}
	if strings.Contains(err.Error(), "bt-cap") {
		t.Errorf("err = %q, want the plain cancellation error, not the bt-cap message (the parent context was canceled, not runCypherOnceCapped's own btCap deadline)", err.Error())
	}
}

// TestExtractRelKinds confirms shape 4's edge-kind list is parsed correctly
// out of shape4Text's own "[:A|B|C*1..]" alternation -- deriving the list
// from the query text itself (rather than hand-transcribing it a second
// time) means the two can never silently drift apart. Locks in the 64-kind
// interpolation shape 4 depends on.
func TestExtractRelKinds(t *testing.T) {
	kinds := extractRelKinds(shape4Text)

	const wantCount = 64
	if len(kinds) != wantCount {
		t.Fatalf("len(kinds) = %d, want %d", len(kinds), wantCount)
	}
	if kinds[0] != "Owns" {
		t.Errorf("kinds[0] = %q, want %q", kinds[0], "Owns")
	}
	if kinds[len(kinds)-1] != "AbuseTGTDelegation" {
		t.Errorf("kinds[last] = %q, want %q", kinds[len(kinds)-1], "AbuseTGTDelegation")
	}

	want := map[string]bool{"MemberOf": true, "AdminTo": true, "HasSession": true, "GenericAll": true, "WriteDacl": true, "AddMember": true}
	got := make(map[string]bool, len(kinds))
	for _, k := range kinds {
		got[k] = true
	}
	for k := range want {
		if !got[k] {
			t.Errorf("extractRelKinds(shape4Text) missing expected kind %q", k)
		}
	}
}

// TestExtractRelKindsNoMatch confirms the helper degrades to nil rather than
// panicking on text with no "[:...*" alternation -- extractRelKinds is only
// ever called on a known-good embedded constant today, but its own contract
// should still be safe against a text that doesn't match.
func TestExtractRelKindsNoMatch(t *testing.T) {
	if got := extractRelKinds("MATCH (n) RETURN n"); got != nil {
		t.Errorf("extractRelKinds(no-match) = %v, want nil", got)
	}
}

// TestShape4And5TextsAreVerbatim locks in the two embedded pre-built corpus
// texts against testdata/prebuilt/agt.json's own "Shortest paths to Domain
// Admins" and "Domain Admins logons to non-Domain Controllers" entries,
// catching any accidental retyping drift -- see loadAGTQuery's doc.
func TestShape4And5TextsAreVerbatim(t *testing.T) {
	got4 := loadAGTQuery(t, "Shortest paths to Domain Admins")
	if got4 != shape4Text {
		t.Errorf("shape4Text does not match testdata/prebuilt/agt.json's \"Shortest paths to Domain Admins\" entry verbatim\ngot agt.json:\n%s\nwant (shape4Text):\n%s", got4, shape4Text)
	}

	got5 := loadAGTQuery(t, "Domain Admins logons to non-Domain Controllers")
	if got5 != shape5Text {
		t.Errorf("shape5Text does not match testdata/prebuilt/agt.json's \"Domain Admins logons to non-Domain Controllers\" entry verbatim\ngot agt.json:\n%s\nwant (shape5Text):\n%s", got5, shape5Text)
	}
}

// agtEntry mirrors one testdata/prebuilt/agt.json array element -- only the
// two fields loadAGTQuery needs, not the full commonSearchEntry shape
// prebuilt_corpus_integration_test.go already defines (that file lives in
// the repo-root package, not this one, and pulling in a whole extra
// dependency just to read two fields here would be more coupling than this
// test needs).
type agtEntry struct {
	Name  string `json:"name"`
	Query string `json:"query"`
}

// loadAGTQuery reads ../../testdata/prebuilt/agt.json (this package lives at
// bench/cypherbench, two directories below the repo root) and returns the
// query text of the entry named name, failing the test if the file can't be
// read/parsed or no entry matches. Used only to prove shape4Text/shape5Text
// were copied verbatim from the corpus, not to build cypherbench itself
// (which embeds the two texts as Go constants -- see the package doc for why
// a benchmark binary shouldn't read testdata files at run time).
func loadAGTQuery(t *testing.T, name string) string {
	t.Helper()

	data, err := os.ReadFile("../../testdata/prebuilt/agt.json")
	if err != nil {
		t.Fatalf("read testdata/prebuilt/agt.json: %v", err)
	}

	var entries []agtEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		t.Fatalf("unmarshal testdata/prebuilt/agt.json: %v", err)
	}

	for _, e := range entries {
		if e.Name == name {
			return e.Query
		}
	}
	t.Fatalf("testdata/prebuilt/agt.json: no entry named %q", name)
	return ""
}
