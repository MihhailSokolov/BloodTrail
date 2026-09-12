// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

// Boot gap buffer size caps. Exceeding either poisons the buffer (the file
// is then rejected at adoption and boot falls through to the pg rebuild, the
// pre-buffer behavior), so these bound memory without ever risking a wrong
// adoption. The entry cap bounds the slice; the key cap bounds what the
// entries' ChangeSets collectively name, since one batch flush can record
// thousands of keys in a single entry. Both are sized for the real window
// they cover -- the seconds a snapshot-file load takes at boot, during which
// BloodHound's startup analysis writes land, plus adoption's bounded settle
// wait (bootGapSettleTimeout, boot.go) -- not for an unbounded ingest
// backlog, which is exactly the case the overflow poison exists to hand to
// the rebuild path honestly.
const (
	maxBootGapEntries = 1024
	maxBootGapKeys    = 1 << 18
)

// bootGapEntry is one write the boot gap buffer accounts for: the pg
// watermark counter its eager bump committed, and the ChangeSet to replay
// for it -- nil for a counter-only entry, i.e. a write that advanced the
// counter but left nothing to replay (a bumped write that produced no
// committed effect, ResolveAbandonedWrite, or one whose ChangeSet was
// empty).
type bootGapEntry struct {
	counter uint64
	cs      *ChangeSet
}

// bootGapBuffer records the writes that arrive while the engine has no View
// yet, so that a snapshot-file boot can adopt the file DESPITE those writes:
// at adoption time their counters prove they are exactly the writes between
// the file's stamped watermark and the attempt's frozen pg target
// (bootGapCoveredAt) -- waiting out any still in flight
// (adoptSnapshotFileView's settle-wait) -- and their ChangeSets are replayed
// onto the file-loaded snapshot through the same read-back/segment machinery
// Apply uses (adoptSnapshotFileView, boot.go). Without it, BloodHound's own startup analysis -- which writes to
// the graph within milliseconds of every boot -- supersedes the file on
// every restart whose load takes longer than the first write, which at
// production scale is every restart.
//
// The active flag is checked first, unlocked, on every observe call: the
// buffer only ever matters between Start and the boot file attempt's
// conclusion, and after deactivation each Apply pays one atomic load and
// nothing else. Everything behind the flag is guarded by mu -- Apply's
// observe calls already hold applyMu, but ResolveAbandonedWrite's do not,
// and take() must see a stable buffer regardless of which of the two raced
// it last.
//
// Poisoning is one-way and remembers only the first reason: any write the
// buffer cannot faithfully account for (a nil scope, a fallback-carrying
// ChangeSet, an unbumped write that still recorded changes, overflow) makes
// the whole buffer unusable, because replay correctness is an
// all-or-nothing claim -- one unaccounted write is exactly a stale replica.
// A poisoned buffer keeps accepting counters harmlessly (they are never
// read), and the adoption path rejects the file when it takes the buffer.
type bootGapBuffer struct {
	active atomic.Bool

	mu       sync.Mutex
	entries  []bootGapEntry
	keyCount int
	poisoned string
}

// activate arms the buffer. Called once, from Start (boot.go), before the
// boot-load goroutine -- and therefore before any possible observe call in
// production, since a driver caller cannot write before Open returns and
// Open calls Start first.
func (b *bootGapBuffer) activate() {
	b.active.Store(true)
}

// deactivate disarms the buffer and frees whatever it held. Idempotent, and
// safe to call whether or not activate ever ran. Called when the buffer can
// no longer be useful: the boot file attempt concluded without adopting
// (runBootLoad), any View was adopted through the rebuild path
// (adoptRebuiltView -- a pg rebuild reads post-write state, so the buffered
// writes are already in it), or the adoption path consumed it (take).
func (b *bootGapBuffer) deactivate() {
	b.active.Store(false)
	b.mu.Lock()
	b.entries = nil
	b.keyCount = 0
	b.mu.Unlock()
}

// observe records one committed write's scope, exactly as Apply saw it --
// called from Apply (apply.go), right after the watermark bookkeeping and
// before ANY of Apply's early returns, so a bumped write whose ChangeSet is
// empty (which Apply drops before it ever reaches the no-snapshot branch)
// still gets its counter accounted; a hole in the counter sequence is a
// rejected file, not a smaller buffer.
//
// A scope the replay cannot faithfully reproduce poisons the buffer instead
// of being recorded: a nil scope (nothing names what changed), a ChangeSet
// carrying a fallback record (the write's effect cannot be expressed as a
// delta -- the same judgment Apply itself makes), and the
// belt-and-braces case of an unbumped scope that still recorded changes
// (ensureBumped pairs a failed bump with a fallback record, so this should
// be unreachable; if the pairing ever decays, failing closed here keeps the
// file from adopting over an uncounted write). An unbumped scope with an
// empty ChangeSet touched nothing and advanced nothing: skipped.
func (b *bootGapBuffer) observe(scope *WriteScope) {
	if !b.active.Load() {
		return
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if scope == nil {
		b.poison("nil write scope")
		return
	}

	cs := scope.Changes()
	if hasFallback, reasons := cs.HasFallback(); hasFallback {
		b.poison("fallback write during boot: " + strings.Join(reasons, "; "))
		return
	}

	counter, bumped := scope.Watermark()
	if !bumped {
		if cs.Empty() {
			return
		}
		b.poison("unbumped write recorded changes")
		return
	}

	if cs.Empty() {
		cs = nil
	}
	b.append(counter, cs)
}

// observeAbandoned records the counter of a bumped write that is known to
// have produced no committed effect at all -- ResolveAbandonedWrite's case
// (watermark.go), and the same premise the driver's error branches already
// bet the replica's correctness on by skipping Apply. Counter-only: there is
// nothing to replay, but without the counter a rolled-back boot write would
// leave a hole in the sequence and force an unnecessary rejection. An
// unbumped scope left no trace in the counter either and is skipped -- its
// failed-bump bookkeeping (if any) is the settledDirtyGen check's business
// at adoption time, not this buffer's.
func (b *bootGapBuffer) observeAbandoned(scope *WriteScope) {
	if !b.active.Load() || scope == nil {
		return
	}

	counter, bumped := scope.Watermark()
	if !bumped {
		return
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	b.append(counter, nil)
}

// append records one entry, enforcing both caps. Callers hold mu.
func (b *bootGapBuffer) append(counter uint64, cs *ChangeSet) {
	if len(b.entries) >= maxBootGapEntries {
		b.poison("overflow: too many buffered writes")
		return
	}
	if cs != nil {
		next := b.keyCount + cs.keyCount()
		if next > maxBootGapKeys {
			b.poison("overflow: too many buffered keys")
			return
		}
		b.keyCount = next
	}
	b.entries = append(b.entries, bootGapEntry{counter: counter, cs: cs})
}

// poison marks the buffer unusable, keeping the FIRST reason (the later
// ones are consequences or repeats, and the first is what an operator needs
// to see). Callers hold mu.
func (b *bootGapBuffer) poison(reason string) {
	if b.poisoned == "" {
		b.poisoned = reason
	}
}

// take consumes the buffer for the adoption decision: it returns every
// recorded entry plus the poison reason ("" when clean), and deactivates the
// buffer -- adoption is the buffer's one consumer, and whatever the decision
// turns out to be, nothing after it can ever use these entries again (an
// adopted file already contains them via replay; a rejected file falls
// through to a pg rebuild that reads post-write state anyway).
//
// An observe racing this call lands either before the deactivation (its
// entry is returned) or after (its entry goes to the freed buffer,
// unread and harmless -- and its counter being absent from the returned set
// makes the gap check reject, which is the conservative outcome the design
// requires for a write the decision did not see).
func (b *bootGapBuffer) take() (entries []bootGapEntry, poisoned string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.active.Store(false)
	entries, poisoned = b.entries, b.poisoned
	b.entries = nil
	b.keyCount = 0
	return entries, poisoned
}

// peek returns a copy of the recorded entries plus the poison reason,
// WITHOUT consuming or deactivating anything: the settle-wait adoption loop
// (adoptSnapshotFileView, boot.go) checks coverage attempt after attempt
// while the buffer keeps observing the very Applies it is waiting for, and
// only the final, covered attempt calls take. The entries are copied so the
// caller reads a stable snapshot while later observes keep appending to the
// live slice.
func (b *bootGapBuffer) peek() (entries []bootGapEntry, poisoned string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return append([]bootGapEntry(nil), b.entries...), b.poisoned
}

// bootGapCoveredAt reports whether a snapshot file stamped fileWatermark may
// be adopted given the decision's FROZEN pg counter pgSnapshot and the
// counters of every write the boot gap buffer has accounted for so far:
// exactly when the counters at or below pgSnapshot are precisely
// {fileWatermark+1, ..., pgSnapshot}, each once. Extracted as a pure
// function so the decision has a direct unit test, mirroring
// watermarkTrustedFor's identical treatment (watermark.go). An empty gap
// (pgSnapshot == fileWatermark, no counters) is the degenerate cover, and
// is still what a quiet restart produces.
//
// The reasoning is the watermark protocol's own (BumpWatermark,
// watermark.go): every mutating write bumps the single pg counter eagerly,
// before its own effect, and each bump's value is unique and strictly
// increasing. SaveSnapshot stamps the file with the counter that was live
// when it decided to proceed, so every write the file could be missing AS
// OF THE FROZEN TARGET carries a counter in (fileWatermark, pgSnapshot]. If
// the buffer holds exactly those counters, then every such write was seen
// by THIS process's own observers -- replayable from its ChangeSet, or
// known to have committed nothing -- and file + replay reproduces
// PostgreSQL's state at pgSnapshot. A hole means some write landed that
// nothing accounted for (another process, or a crash-window write from the
// previous one), and the file must be rejected exactly as the plain
// equality check this generalizes rejected any advance at all. The
// settle-wait design (adoptSnapshotFileView) exists because a hole can
// also be transient -- an Apply still in flight -- which waiting, not
// rejecting, resolves; this predicate stays time-blind and answers only
// "is the target covered RIGHT NOW".
//
// Counters ABOVE pgSnapshot are permitted and simply ignored here: they
// belong to writes that landed after the target was frozen, whose safety
// is the adoption path's argument (observed ones ride the replay; parked
// ones land as ordinary deltas after publish -- both stage read-back
// truth, so order cannot matter). What is NOT tolerated, above or below:
// a duplicate counter (the protocol guarantees uniqueness, so a duplicate
// means the accounting itself is wrong) or a counter at or below
// fileWatermark (a write the file already contains was somehow
// re-observed) -- both "something is wrong, don't trust it". A file AHEAD
// of pg (pgSnapshot < fileWatermark) should never happen under a monotonic
// counter, and is refused outright rather than reasoned about.
//
// counters is sorted in place; callers pass a slice they own.
func bootGapCoveredAt(fileWatermark, pgSnapshot uint64, counters []uint64) bool {
	if pgSnapshot < fileWatermark {
		return false
	}

	sort.Slice(counters, func(i, j int) bool { return counters[i] < counters[j] })

	inRange := 0
	for i, c := range counters {
		if i > 0 && c == counters[i-1] {
			return false
		}
		if c > pgSnapshot {
			continue // sorted: everything from here up is post-freeze, but keep scanning for duplicates
		}
		if c != fileWatermark+1+uint64(inRange) {
			return false
		}
		inRange++
	}
	return uint64(inRange) == pgSnapshot-fileWatermark
}
