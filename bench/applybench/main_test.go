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

// TestApplyOverheadMaxPctIsProvisional pins applyOverheadMaxPct's current
// PROVISIONAL value so a change to it is a deliberate, reviewed edit (e.g.
// the follow-up task that measures at 5M and replaces it with an
// evidence-based cap per the m4.5 measured-physics convention), not an
// accidental one -- mirroring bench/builderbench's
// TestMeasuredEngineAbsoluteCaps pinning its own measured caps.
func TestApplyOverheadMaxPctIsProvisional(t *testing.T) {
	if applyOverheadMaxPct != 25.0 {
		t.Fatalf("applyOverheadMaxPct = %v, want 25.0 -- if this changed deliberately, update this pin and the README's cap-table doc together", applyOverheadMaxPct)
	}
}

// TestIdleP95MultiplierIsProvisional is applyOverheadMaxPct's pin, for
// idleP95Multiplier.
func TestIdleP95MultiplierIsProvisional(t *testing.T) {
	if idleP95Multiplier != 3.0 {
		t.Fatalf("idleP95Multiplier = %v, want 3.0 -- if this changed deliberately, update this pin and the README's cap-table doc together", idleP95Multiplier)
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
		{msgSnapshotWritten, "../../internal/engine/persist.go"},
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
