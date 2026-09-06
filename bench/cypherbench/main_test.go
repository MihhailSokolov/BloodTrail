// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"math"
	"os"
	"testing"
	"time"
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
// decision function: independently and jointly, a result-count mismatch and
// a below-threshold p50 ratio must each surface their own reason, and a
// shape's own minRatio (5x for four shapes, 1x for the objectid point
// lookup -- see minRatioFor's doc) is respected rather than a single global
// bar.
func TestEvaluateShape(t *testing.T) {
	const ms = time.Millisecond

	cases := []struct {
		name         string
		btP50, pgP50 time.Duration
		match        bool
		minRatio     float64
		wantOK       bool
		wantReasons  int
	}{
		{
			name:  "passes at exactly the ratio bar and matching counts",
			btP50: 10 * ms, pgP50: 50 * ms, match: true, minRatio: 5.0,
			wantOK: true, wantReasons: 0,
		},
		{
			name:  "fails when ratio is just below the bar",
			btP50: 10 * ms, pgP50: 49 * ms, match: true, minRatio: 5.0,
			wantOK: false, wantReasons: 1,
		},
		{
			name:  "fails on count mismatch even with a comfortable ratio",
			btP50: 10 * ms, pgP50: 100 * ms, match: false, minRatio: 5.0,
			wantOK: false, wantReasons: 1,
		},
		{
			name:  "fails on both mismatch and ratio, reporting both reasons",
			btP50: 10 * ms, pgP50: 10 * ms, match: false, minRatio: 5.0,
			wantOK: false, wantReasons: 2,
		},
		{
			name:  "objectid point lookup's 1x bar passes where 5x would have failed",
			btP50: 10 * ms, pgP50: 12 * ms, match: true, minRatio: 1.0,
			wantOK: true, wantReasons: 0,
		},
		{
			name:  "objectid point lookup still fails below its own 1x bar",
			btP50: 10 * ms, pgP50: 9 * ms, match: true, minRatio: 1.0,
			wantOK: false, wantReasons: 1,
		},
		{
			name:  "exactly at the ratio bar passes (>=, not strictly greater)",
			btP50: 10 * ms, pgP50: 10 * ms, match: true, minRatio: 1.0,
			wantOK: true, wantReasons: 0,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ok, reasons := evaluateShape(c.btP50, c.pgP50, c.match, c.minRatio)
			if ok != c.wantOK {
				t.Errorf("ok = %v, want %v (reasons: %v)", ok, c.wantOK, reasons)
			}
			if len(reasons) != c.wantReasons {
				t.Errorf("len(reasons) = %d, want %d (reasons: %v)", len(reasons), c.wantReasons, reasons)
			}
		})
	}
}

// TestMinRatioForKnownShapesHaveEntries confirms every shape execute
// actually benchmarks has a shapeMinRatio entry -- the maintenance bug
// minRatioFor's own doc warns about, caught at test time rather than via a
// stderr warning at run time (mirroring builderbench's identical
// TestThresholdFor_KnownShapesHaveEntries).
func TestMinRatioForKnownShapesHaveEntries(t *testing.T) {
	knownShapes := []string{
		shapeRIDSuffixScan,
		shapeFlagScan,
		shapeObjectIDPointLookup,
		shapeShortestPathPrebuilt,
		shapeCollectAntiJoinPrebuilt,
	}
	for _, name := range knownShapes {
		if _, ok := shapeMinRatio[name]; !ok {
			t.Errorf("shapeMinRatio has no entry for %q", name)
		}
	}
}

// TestObjectIDPointLookupHasWeakerBar locks in the spec's specific
// requirement: every shape needs 5x except the objectid point lookup, which
// only needs 1x (a point lookup is fast on both sides -- see the package
// doc).
func TestObjectIDPointLookupHasWeakerBar(t *testing.T) {
	got, ok := shapeMinRatio[shapeObjectIDPointLookup]
	if !ok {
		t.Fatal("objectid_point_lookup missing from shapeMinRatio")
	}
	if got != pointLookupMinRatio {
		t.Errorf("shapeMinRatio[objectid_point_lookup] = %v, want %v", got, pointLookupMinRatio)
	}

	for name, ratio := range shapeMinRatio {
		if name == shapeObjectIDPointLookup {
			continue
		}
		if ratio != enforceRatio {
			t.Errorf("%s minRatio = %v, want the standard enforceRatio %v", name, ratio, enforceRatio)
		}
	}
}

// TestExtractRelKinds confirms shape 4's edge-kind list is parsed correctly
// out of shape4Text's own "[:A|B|C*1..]" alternation -- deriving the list
// from the query text itself (rather than hand-transcribing it a second
// time) means the two can never silently drift apart. Locks in the "64-kind
// interpolation" the task brief calls out by name.
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
