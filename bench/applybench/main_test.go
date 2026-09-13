// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"log/slog"
	"math"
	"os"
	"strings"
	"testing"
	"time"
)

// TestOverheadPercent table-tests overheadPercent, (a)'s pure
// engine-on-vs-off overhead arithmetic, including its guarded (never
// divide-by-zero) edge cases -- mirroring bench/cypherbench's/
// bench/builderbench's identical treatment of their own ratioOf.
func TestOverheadPercent(t *testing.T) {
	cases := []struct {
		name          string
		onP50, offP50 time.Duration
		want          float64
		wantIsInf     bool
	}{
		{name: "no overhead", onP50: 10 * time.Second, offP50: 10 * time.Second, want: 0},
		{name: "25 percent over", onP50: 125 * time.Millisecond, offP50: 100 * time.Millisecond, want: 25},
		{name: "on cheaper than off is negative", onP50: 80 * time.Millisecond, offP50: 100 * time.Millisecond, want: -20},
		{name: "both zero is zero", onP50: 0, offP50: 0, want: 0},
		{name: "off zero on positive is +Inf", onP50: 10 * time.Millisecond, offP50: 0, wantIsInf: true},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := overheadPercent(c.onP50, c.offP50)
			if c.wantIsInf {
				if !math.IsInf(got, 1) {
					t.Fatalf("overheadPercent(%s, %s) = %v, want +Inf", c.onP50, c.offP50, got)
				}
				return
			}
			if math.Abs(got-c.want) > 1e-9 {
				t.Fatalf("overheadPercent(%s, %s) = %v, want %v", c.onP50, c.offP50, got, c.want)
			}
		})
	}
}

// TestMeasuredEngineOverheadCaps pins the two evidence-based caps derived
// from three 5M-scale runs (2026-09, see this package's README
// cap-rationale table and both constants' own docs in main.go) to their
// measured-worst x ~1.75 values -- mirroring bench/builderbench's
// TestMeasuredEngineAbsoluteCaps pinning its own measured caps. These
// values are measurement-derived; changing either requires fresh 5M-scale
// evidence recorded alongside the change, in the README table and here
// together, so a silent edit fails this test.
func TestMeasuredEngineOverheadCaps(t *testing.T) {
	if applyOverheadMaxPct != 60.0 {
		t.Fatalf("applyOverheadMaxPct = %v, want 60.0 -- worst measured overhead (33.52%%) x ~1.75, rounded; update this pin and the README's cap-table doc together", applyOverheadMaxPct)
	}
	if idleP95Multiplier != 1.75 {
		t.Fatalf("idleP95Multiplier = %v, want 1.75 -- worst measured delta/idle p95 ratio (0.98) x ~1.75, rounded; update this pin and the README's cap-table doc together", idleP95Multiplier)
	}
}

// TestLatencyWithinMultiplier table-tests latencyWithinMultiplier, (b)'s
// pure decision function, including the degenerate zero-idle-baseline case
// its own doc calls out.
func TestLatencyWithinMultiplier(t *testing.T) {
	cases := []struct {
		name               string
		duringP95, idleP95 time.Duration
		multiplier         float64
		want               bool
	}{
		{name: "well within bound", duringP95: 20 * time.Millisecond, idleP95: 10 * time.Millisecond, multiplier: 3.0, want: true},
		{name: "exactly at bound", duringP95: 30 * time.Millisecond, idleP95: 10 * time.Millisecond, multiplier: 3.0, want: true},
		{name: "just over bound", duringP95: 31 * time.Millisecond, idleP95: 10 * time.Millisecond, multiplier: 3.0, want: false},
		{name: "degenerate zero idle baseline passes", duringP95: 5 * time.Millisecond, idleP95: 0, multiplier: 3.0, want: true},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := latencyWithinMultiplier(c.duringP95, c.idleP95, c.multiplier); got != c.want {
				t.Fatalf("latencyWithinMultiplier(%s, %s, %v) = %t, want %t", c.duringP95, c.idleP95, c.multiplier, got, c.want)
			}
		})
	}
}

// TestPercentile table-tests percentile's nearest-rank arithmetic --
// mirroring the sibling benches' own identical duplicated function's
// implicit coverage (this package's own copy has never been directly
// tested until now).
func TestPercentile(t *testing.T) {
	durations := []time.Duration{
		5 * time.Millisecond, 1 * time.Millisecond, 3 * time.Millisecond, 2 * time.Millisecond, 4 * time.Millisecond,
	}

	if got := percentile(nil, 0.50); got != 0 {
		t.Fatalf("percentile(nil, 0.50) = %s, want 0", got)
	}
	if got := percentile(durations, 0.50); got != 3*time.Millisecond {
		t.Fatalf("percentile(durations, 0.50) = %s, want 3ms", got)
	}
	if got := percentile(durations, 1.0); got != 5*time.Millisecond {
		t.Fatalf("percentile(durations, 1.0) = %s, want 5ms", got)
	}
	// The input slice must be left untouched (percentile sorts a copy).
	if durations[0] != 5*time.Millisecond {
		t.Fatalf("percentile mutated its input slice: %v", durations)
	}
}

// TestLogCapture exercises logCapture directly (no live engine/database
// needed): Handle records message/Time/duration-attribute triples, count/
// durations/latest read them back, and a record with no "duration"
// attribute is tracked (for count/latest) without polluting durations.
func TestLogCapture(t *testing.T) {
	c := newLogCapture()

	rec := func(msg string, withDuration bool, d time.Duration, at time.Time) {
		var attrs []slog.Attr
		if withDuration {
			attrs = append(attrs, slog.Duration("duration", d))
		}
		r := slog.NewRecord(at, slog.LevelInfo, msg, 0)
		r.AddAttrs(attrs...)
		if err := c.Handle(context.Background(), r); err != nil {
			t.Fatalf("Handle: %v", err)
		}
	}

	t0 := time.Now()
	rec("bloodtrail: compaction finished", true, 10*time.Millisecond, t0)
	rec("bloodtrail: compaction finished", true, 20*time.Millisecond, t0.Add(time.Second))
	rec("bloodtrail: snapshot file loaded", false, 0, t0.Add(2*time.Second))

	if got := c.count("bloodtrail: compaction finished"); got != 2 {
		t.Fatalf("count(compaction finished) = %d, want 2", got)
	}
	if got := c.count("bloodtrail: snapshot file loaded"); got != 1 {
		t.Fatalf("count(snapshot file loaded) = %d, want 1", got)
	}
	if got := c.count("never logged"); got != 0 {
		t.Fatalf("count(never logged) = %d, want 0", got)
	}

	durs := c.durations("bloodtrail: compaction finished")
	if len(durs) != 2 || durs[0] != 10*time.Millisecond || durs[1] != 20*time.Millisecond {
		t.Fatalf("durations(compaction finished) = %v, want [10ms 20ms]", durs)
	}

	ev, ok := c.latest("bloodtrail: snapshot file loaded")
	if !ok {
		t.Fatal("latest(snapshot file loaded) reported not found")
	}
	if ev.hasDur {
		t.Fatal("latest(snapshot file loaded).hasDur = true, want false (no duration attr recorded)")
	}
	if !ev.at.Equal(t0.Add(2 * time.Second)) {
		t.Fatalf("latest(snapshot file loaded).at = %v, want %v", ev.at, t0.Add(2*time.Second))
	}

	if _, ok := c.latest("never logged"); ok {
		t.Fatal("latest(never logged) reported found")
	}

	// firstAfter finds the earliest record at or after a cut-off, which is
	// how (b) ignores an Apply that happened during its warm-up.
	at, ok := c.firstAfter("bloodtrail: compaction finished", t0.Add(500*time.Millisecond))
	if !ok || !at.Equal(t0.Add(time.Second)) {
		t.Fatalf("firstAfter(compaction finished, t0+500ms) = (%v, %t), want (%v, true)", at, ok, t0.Add(time.Second))
	}
	if at, ok := c.firstAfter("bloodtrail: compaction finished", t0); !ok || !at.Equal(t0) {
		t.Fatalf("firstAfter(compaction finished, t0) = (%v, %t), want (%v, true) -- the cut-off is inclusive", at, ok, t0)
	}
	if _, ok := c.firstAfter("bloodtrail: compaction finished", t0.Add(time.Hour)); ok {
		t.Fatal("firstAfter(compaction finished, t0+1h) reported found, want not found")
	}
}

// TestLogCaptureForwardsToPreviousHandler pins the tee installLogCapture
// relies on: a capture must not silence whatever handler it replaced, or
// installing one would blind an operator to the driver's own warnings for
// as long as a measurement runs.
func TestLogCaptureForwardsToPreviousHandler(t *testing.T) {
	var forwarded strings.Builder
	c := newLogCapture()
	c.next = slog.NewTextHandler(&forwarded, &slog.HandlerOptions{Level: slog.LevelWarn})

	warn := slog.NewRecord(time.Now(), slog.LevelWarn, "loud", 0)
	debug := slog.NewRecord(time.Now(), slog.LevelDebug, "quiet", 0)
	for _, r := range []slog.Record{warn, debug} {
		if err := c.Handle(context.Background(), r); err != nil {
			t.Fatalf("Handle: %v", err)
		}
	}

	// Both are captured (logCapture.Enabled is unconditional) ...
	if got := c.count("loud") + c.count("quiet"); got != 2 {
		t.Fatalf("captured %d records, want 2", got)
	}
	// ... but only the one the wrapped handler's own level accepts is forwarded.
	out := forwarded.String()
	if !strings.Contains(out, "loud") {
		t.Fatalf("the Warn record was not forwarded to the previous handler:\n%s", out)
	}
	if strings.Contains(out, "quiet") {
		t.Fatalf("a Debug record was forwarded to a Warn-level handler:\n%s", out)
	}
}

// TestSplitByFirstApply table-tests (b)'s delta-population split: the pure
// function deciding which samples actually queried a populated delta and
// which ran before the window's first write-through Apply and so measured
// the base snapshot -- the distinction that made the difference between
// (b) reporting a number about write-through and reporting one about
// roughly 83% empty-delta queries.
func TestSplitByFirstApply(t *testing.T) {
	t0 := time.Now()
	samples := []latencySample{
		{at: t0, d: 1 * time.Millisecond},
		{at: t0.Add(time.Second), d: 2 * time.Millisecond},
		{at: t0.Add(2 * time.Second), d: 3 * time.Millisecond},
	}

	empty, populated := splitByFirstApply(samples, t0.Add(time.Second), true)
	if len(empty) != 1 || empty[0] != 1*time.Millisecond {
		t.Fatalf("empty-delta half = %v, want [1ms]", empty)
	}
	if len(populated) != 2 || populated[0] != 2*time.Millisecond || populated[1] != 3*time.Millisecond {
		t.Fatalf("delta-populated half = %v, want [2ms 3ms] (the cut-off itself counts as populated)", populated)
	}

	// No Apply observed at all: every sample is honestly empty-delta.
	empty, populated = splitByFirstApply(samples, time.Time{}, false)
	if len(empty) != 3 || len(populated) != 0 {
		t.Fatalf("with no Apply observed: empty=%d populated=%d, want 3 and 0", len(empty), len(populated))
	}

	if empty, populated := splitByFirstApply(nil, t0, true); empty != nil || populated != nil {
		t.Fatalf("splitByFirstApply(nil, ...) = (%v, %v), want (nil, nil)", empty, populated)
	}
}

// TestResolveRunID pins both halves of the objectid-namespacing fix: an
// explicit -run-id is used verbatim (so an operator can correlate two runs
// deliberately), and an absent one produces a fresh value per invocation
// (so two runs against the same un-wiped database do not silently upsert
// over each other -- see resolveRunID's own doc).
func TestResolveRunID(t *testing.T) {
	if got := resolveRunID("fixed"); got != "fixed" {
		t.Fatalf("resolveRunID(%q) = %q, want it used verbatim", "fixed", got)
	}

	first := resolveRunID("")
	if first == "" {
		t.Fatal("resolveRunID(\"\") returned an empty id")
	}
	time.Sleep(time.Millisecond)
	if second := resolveRunID(""); second == first {
		t.Fatalf("two resolveRunID(\"\") calls returned the same id %q, want distinct per invocation", first)
	}
}

// TestAllDurations pins the timestamp-dropping helper (b)'s full-window
// reporting uses.
func TestAllDurations(t *testing.T) {
	t0 := time.Now()
	got := allDurations([]latencySample{{at: t0, d: time.Millisecond}, {at: t0, d: 2 * time.Millisecond}})
	if len(got) != 2 || got[0] != time.Millisecond || got[1] != 2*time.Millisecond {
		t.Fatalf("allDurations = %v, want [1ms 2ms]", got)
	}
	if got := allDurations(nil); len(got) != 0 {
		t.Fatalf("allDurations(nil) = %v, want empty", got)
	}
}

// TestLogMessagesMatchEngineSource guards msgCompactionFinished/
// msgSnapshotWritten against silently drifting out of sync with the
// literal strings internal/engine actually logs (compact.go/persist.go):
// this package has no other way to observe those events (see logCapture's
// own doc), so a rename on the engine side that this file's constants
// missed would make every measurement using them fail confusingly (a
// perpetual "count=0"/timeout) rather than obviously. Mirrors
// bench/cypherbench's TestShape4And5TextsAreVerbatim in spirit: pin a
// cross-package string dependency with a direct file read, rather than
// trust it to stay in sync by convention alone.
func TestLogMessagesMatchEngineSource(t *testing.T) {
	cases := []struct {
		message string
		file    string
	}{
		{msgCompactionFinished, "../../internal/engine/compact.go"},
		{msgSnapshotRebuilt, "../../internal/engine/engine.go"},
		{msgSnapshotWritten, "../../internal/engine/persist.go"},
		{msgSnapshotFileLoaded, "../../internal/engine/boot.go"},
		{msgSnapshotFileRejected, "../../internal/engine/boot.go"},
		{msgWriteThroughApplied, "../../internal/engine/apply.go"},
		{msgPathEngineServed, "../../internal/engine/engine.go"},
		// Not messages but pinned by the same discipline: (e)'s accepted
		// rejection reasons must stay the engine's own literals -- the
		// uncovered-gap reason, the adoption path's poisoned-buffer prefix
		// bootReplayOverflowPrefix is assembled on, and both of the
		// buffer's own cap-overflow reasons that prefix has to match
		// (TestBootReplayOverflowPrefixMatchesEngineReasons pins the
		// assembly itself).
		{bootReplayGapReason, "../../internal/engine/boot.go"},
		{bootReplayPoisonPrefix, "../../internal/engine/boot.go"},
		{bootReplayOverflowReasons[0], "../../internal/engine/bootgap.go"},
		{bootReplayOverflowReasons[1], "../../internal/engine/bootgap.go"},
	}

	for _, c := range cases {
		t.Run(c.message, func(t *testing.T) {
			data, err := os.ReadFile(c.file)
			if err != nil {
				t.Fatalf("read %s: %v", c.file, err)
			}
			if !strings.Contains(string(data), `"`+c.message+`"`) {
				t.Fatalf("%s not found verbatim (quoted) in %s -- this package's copy has drifted out of sync with the engine source", c.message, c.file)
			}
		})
	}
}

// bootReplayOverflowReasons are the boot gap buffer's two cap-overflow
// poison reasons (internal/engine/bootgap.go's append), copied here the
// way the message literals are and pinned against that source by
// TestLogMessagesMatchEngineSource.
var bootReplayOverflowReasons = []string{
	"overflow: too many buffered writes",
	"overflow: too many buffered keys",
}

// TestBootReplayOverflowPrefixMatchesEngineReasons pins what the literal
// pins above cannot: that bootReplayOverflowPrefix matches the rejection
// the adoption path actually logs for EACH cap overflow -- its
// poisoned-buffer prefix followed by the buffer's own reason (boot.go's
// reject("boot write buffer poisoned: " + poisoned)). A typo in the
// assembled constant, or an overflow reason drifting off the "overflow: "
// stem, would pass every literal pin yet turn a legitimate
// OVERFLOW-REJECTED boot into a run-aborting unknown reason.
func TestBootReplayOverflowPrefixMatchesEngineReasons(t *testing.T) {
	for _, reason := range bootReplayOverflowReasons {
		logged := bootReplayPoisonPrefix + reason
		if !strings.HasPrefix(logged, bootReplayOverflowPrefix) {
			t.Errorf("bootReplayOverflowPrefix = %q does not match the logged overflow rejection %q", bootReplayOverflowPrefix, logged)
		}
	}
}
