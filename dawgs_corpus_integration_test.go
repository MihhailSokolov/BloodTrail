// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

// Adapted from specterops/dawgs integration/cypher_test.go (v0.8.0) to run
// against the bloodtrail driver; case files and datasets are read from the
// Go module cache.

//go:build integration

// # File placement: root package, not internal/engine
//
// This file needs same-package access to Driver's own unexported `engine`
// field (see staleness_integration_test.go's identically-reasoned doc) so it
// can force a deterministic rebuild with d.engine.RebuildNow before running
// any case, rather than depending on the timing of Start's own asynchronous
// boot-load goroutine (engine/boot.go) or of the automatic fallback-recovery
// rebuild described below. That access requires living in package
// bloodtrail, not bloodtrail_test.
//
// # What this runs
//
// dawgs' own integration test suite ships a Cypher conformance corpus --
// integration/testdata/cases/*.json (30 files, ~375 cases covering
// aggregation, expansion, pattern predicates, quantifiers, shortest paths,
// temporal values, updates, and more) alongside the datasets they run
// against (integration/testdata/*.json) -- exercised by
// integration/cypher_test.go's TestCypher. That file carries a
// manual_integration build tag and lives in package integration, so it
// cannot be imported directly; this file adapts it to run the same corpus
// against *bloodtrail.Driver instead of a bare pg/neo4j driver.
//
// Per dataset group, ClearGraph and LoadDataset write through the wrapped
// driver (db, not a raw pg.Driver) before any case runs. ClearGraph's own
// delete (tx.Nodes().Delete(), no Filter call at all) is a shape
// nodeIDsFromCriteria never recognizes (write_observer.go: it needs exactly
// one InIDs-shaped criteria), so every ClearGraph call unconditionally
// records a ChangeSet fallback and trips the engine into fallback, starting
// an automatic background recovery rebuild (apply.go's enterFallback/
// runFallbackRebuild) before LoadDataset writes a single node. A manual
// d.engine.RebuildNow call still runs before every read-only case below --
// not to be the first thing that makes the engine servable (that automatic
// recovery already raced to do so), but to make the snapshot every case
// runs against deterministic rather than however far that race happened to
// get.
//
// This suite used to run every read-only case (no "fixture" field) twice per
// dataset group: once immediately after LoadDataset, on the theory that no
// rebuild had happened yet and every query would therefore delegate to
// PostgreSQL ("delegating" subtests), and once more after the manual
// RebuildNow, once internal/engine/interpret's Plan-accepted shapes could
// actually be served from a fresh snapshot ("engine" subtests) -- the
// expectation being that a failure surfacing only under "engine" points at a
// real serving bug, since "delegating" would have shown the same case
// passing against PostgreSQL.
//
// That premise stopped holding once write-through shipped, and measurably
// so: ClearGraph's own fallback trip (above) already starts a recovery
// rebuild before LoadDataset even begins, and that automatic rebuild reads
// the same committed PostgreSQL state the later manual RebuildNow does,
// since nothing writes in between. Polling d.engine.Fresh()/RebuildCount
// immediately after LoadDataset returns, and logging each case's served/
// declined outcome under both the old "delegating" and "engine" labels
// (repeated runs, every dataset group), showed the automatic recovery had
// already adopted a fresh snapshot every time before "delegating" ran a
// single query, and that the exact same set of cases -- by name, not just by
// count -- was served under both labels. The two passes were exercising the
// identical freshly-rebuilt-from-PostgreSQL state twice, not a
// write-through-applied state versus a rebuilt one, so running every
// read-only case once, against the state the manual RebuildNow produces, is
// the whole suite: the removed first pass was not exercising anything the
// remaining one doesn't already cover. A failure in a case here means the
// in-memory path engine answers that case differently than the corpus's
// own fixed expectation -- a real serving bug (internal/engine) if the case
// was actually served (cypherServedMarker fired), or a bad adaptation
// (mismapped assertion, bad path, schema gap) if it was not -- see the doc
// on TestDAWGSCorpus below.
//
// Fixture cases (a "fixture" field) run inside a rolled-back write
// transaction (Session.WithRollbackFixture): Driver.WriteTransaction only
// calls d.engine.Apply on success (driver.go), and the rollback sentinel
// withRollback returns makes the underlying call fail, so a fixture's write
// never reaches Apply and never disturbs the snapshot every read-only case
// above runs against. More fundamentally, a write transaction's own Query
// calls always run directly against the live tx (only wrappedTransaction.
// Query, built for ReadTransaction, ever consults the engine) -- delegation
// by construction, not by snapshot staleness -- so there is nothing a second
// pass could exercise that a first did not. Each fixture case therefore
// runs exactly once, under its own "fixture" subtest.
package bloodtrail

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/specterops/dawgs"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/integration"
	"github.com/specterops/dawgs/opengraph"
	"github.com/specterops/dawgs/util/size"

	"github.com/MihhailSokolov/BloodTrail/internal/graphtest"
)

// caseFile represents one JSON test case file.
//
// Verbatim from upstream's integration/cypher_test.go.
type caseFile struct {
	Dataset string     `json:"dataset"`
	Cases   []testCase `json:"cases"`
}

// testCase is a single test: a Cypher query and an assertion on its result.
// Cases with a "fixture" field run in a write transaction that rolls back,
// so the inline data doesn't persist.
//
// Verbatim from upstream's integration/cypher_test.go.
type testCase struct {
	Name    string           `json:"name"`
	Cypher  string           `json:"cypher"`
	Params  map[string]any   `json:"params,omitempty"`
	Assert  json.RawMessage  `json:"assert"`
	Fixture *opengraph.Graph `json:"fixture,omitempty"`
}

// locateDAWGSModuleDir finds the on-disk directory of the specterops/dawgs
// module dependency this project already pins (go.mod), the same way `go`
// itself resolves it, so the corpus and dataset files below are read
// straight from the module cache rather than a vendored or hand-copied
// duplicate that could drift from the pinned version.
func locateDAWGSModuleDir(t *testing.T) string {
	t.Helper()

	out, err := exec.Command("go", "list", "-m", "-f", "{{.Dir}}", "github.com/specterops/dawgs").Output()
	if err != nil {
		t.Skipf("locate github.com/specterops/dawgs module directory: %v", err)
	}

	dir := strings.TrimSpace(string(out))
	if dir == "" {
		t.Skip("github.com/specterops/dawgs module directory is empty")
	}
	return dir
}

// corpusSchema parses every named dataset (via datasetPath) and returns a
// graph.Schema declaring the union of their node and edge kinds under
// graphtest.GraphName. This mirrors driver_integration_test.go's
// schemaFromDatasets/harness.go's buildSchema: dataset loading below goes
// through opengraph.Load, whose edges are written via db.BatchOperation
// (opengraph/load.go), and the batch relationship path maps kind names to
// already-declared kind ids rather than lazily defining them the way
// tx.CreateNode/tx.CreateRelationshipByIDs do (see graphtest.LoadRandom's
// doc) -- so every kind a dataset uses must be asserted before it loads.
// Fixture cases are a separate concern with their own separate fix: see
// fixtureKinds' doc, unioned into this schema at its one call site below.
func corpusSchema(t *testing.T, datasetNames []string, datasetPath func(string) string) graph.Schema {
	t.Helper()

	var nodeKinds, edgeKinds graph.Kinds
	for _, name := range datasetNames {
		f, err := os.Open(datasetPath(name))
		if err != nil {
			t.Fatalf("open dataset %q: %v", name, err)
		}
		doc, err := opengraph.ParseDocument(f)
		_ = f.Close()
		if err != nil {
			t.Fatalf("parse dataset %q: %v", name, err)
		}
		nk, ek := doc.Graph.Kinds()
		nodeKinds = nodeKinds.Add(nk...)
		edgeKinds = edgeKinds.Add(ek...)
	}

	return graph.Schema{
		Graphs:       []graph.Graph{{Name: graphtest.GraphName, Nodes: nodeKinds, Edges: edgeKinds}},
		DefaultGraph: graph.Graph{Name: graphtest.GraphName},
	}
}

// fixtureKinds returns the union of every node/edge kind carried by the
// Fixture graph of every fixture case across files -- every parsed case
// file in the corpus, not just one dataset group's.
//
// corpusSchema's doc explains why dataset kinds must be pre-declared before
// opengraph.Load's batch edge-write path will accept them; fixture cases
// avoid that specific problem by writing through opengraph.WriteGraphTx's
// lazily-asserting transaction methods instead. But a fixture case's own
// Cypher can still pattern-match against a kind (e.g. a label predicate
// like "n:Computer") that its own Fixture graph never actually creates --
// the case is testing that the pattern correctly matches nothing, not that
// the kind exists in this fixture. Reading a kind out of a MATCH pattern
// during Cypher-to-SQL translation goes through SchemaManager.MapKind,
// which -- unlike AssertKinds -- never defines a kind, only looks one up,
// so it fails unless some kind, somewhere, already caused that name to be
// asserted. Kind rows persist once asserted (SchemaManager.AssertKinds runs
// in its own, separate, non-rolled-back write transaction, even when called
// from inside a fixture case's rolled-back one), so another fixture case
// earlier in the corpus that does create a "Computer" node would normally
// carry the kind forward -- but that only works if case order happens to
// cooperate, which nothing guarantees. Declaring every fixture kind up
// front, alongside every dataset kind, removes the ordering dependency
// entirely: by the time any case runs, every kind any case could reference
// already exists.
func fixtureKinds(files []caseFile) (nodeKinds, edgeKinds graph.Kinds) {
	for _, cf := range files {
		for _, tc := range cf.Cases {
			if tc.Fixture == nil {
				continue
			}

			nk, ek := tc.Fixture.Kinds()
			nodeKinds = nodeKinds.Add(nk...)
			edgeKinds = edgeKinds.Add(ek...)
		}
	}

	return nodeKinds, edgeKinds
}

// dawgsCorpusServedFloor is this suite's served floor: pinned at 70 (~90% of
// observed, rounded down), just below the 78 read-only cases (across every
// dataset group's run, summed) this corpus's fixed case files actually
// served as of 2026-09-09 (`go test -tags integration -run TestDAWGSCorpus
// -v`, totalEngineServed logged at the end of TestDAWGSCorpus), deterministic
// run to run since RebuildNow and the corpus itself carry no randomness.
// Most of the corpus's ~375 cases are still shapes interpret.Plan declines
// outright (aggregation variants, temporal values, updates, and more -- see
// this file's package doc) or that translateGateOK's own second-guess
// rejects, so 78 (not "most of the corpus") is the correct, already-measured
// baseline, not a bug in this count. A regression that makes interpret.Plan/
// translateGateOK decline far more broadly than expected would still leave
// every individual case's own correctness assertion green (a declined case
// simply delegates to PostgreSQL, which is always correct -- see this file's
// package doc) -- silently defeating the entire point of tracking which
// cases the engine actually serves. This floor catches that silent
// regression the per-case assertions cannot. Retune both this constant and
// its comment together if the corpus (a specterops/dawgs dependency bump) or
// the interpreter's accepted subset changes enough to move the observed
// count.
const dawgsCorpusServedFloor = 70

// TestDAWGSCorpus is the "DAWGS integration corpus green" exit criterion:
// dawgs' own Cypher conformance corpus, run against *bloodtrail.Driver
// instead of a bare driver, against the deterministically-rebuilt snapshot
// described in this file's package doc.
//
// A failure in a case means either this adaptation itself is wrong (a
// mismapped assertion, a bad path, a schema gap) or the in-memory path
// engine answers a recognized shape differently than the corpus's own fixed
// expectation says it should -- see the failing case's decline/serve status
// (cypherServedMarker) to tell which: a case the engine actually served is a
// real serving bug to fix in the engine (internal/engine), with a regression
// test; a case that delegated to PostgreSQL and still failed is a bad
// adaptation, not something to paper over here.
//
// This also tallies every dataset group's served-case count
// (totalEngineServed) and asserts dawgsCorpusServedFloor at the end -- see
// that constant's own doc.
func TestDAWGSCorpus(t *testing.T) {
	dawgsDir := locateDAWGSModuleDir(t)

	caseDir := filepath.Join(dawgsDir, "integration", "testdata", "cases")
	files, err := filepath.Glob(filepath.Join(caseDir, "*.json"))
	if err != nil {
		t.Fatalf("glob case files in %s: %v", caseDir, err)
	}
	if len(files) == 0 {
		t.Fatalf("no case files found in %s", caseDir)
	}

	datasetPath := func(name string) string {
		return filepath.Join(dawgsDir, "integration", "testdata", name+".json")
	}

	// Parse every case file and group by dataset -- verbatim grouping logic
	// from upstream's TestCypher, save for reading from files (an absolute
	// path list from filepath.Glob above) instead of a relative "testdata/
	// cases/*.json" glob.
	type group struct {
		dataset string
		files   []caseFile
	}
	var (
		groups       = map[string]*group{}
		datasetNames []string
		allCaseFiles []caseFile
	)

	for _, path := range files {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}

		var cf caseFile
		if err := json.Unmarshal(raw, &cf); err != nil {
			t.Fatalf("decode %s: %v", path, err)
		}
		allCaseFiles = append(allCaseFiles, cf)

		ds := cf.Dataset
		if ds == "" {
			ds = "base"
		}

		if groups[ds] == nil {
			groups[ds] = &group{dataset: ds}
			datasetNames = append(datasetNames, ds)
		}
		groups[ds].files = append(groups[ds].files, cf)
	}
	// Deterministic subtest/group ordering (map iteration above is not).
	sort.Strings(datasetNames)

	dsn := graphtest.PGAvailable(t)

	// Must be installed before dawgs.Open constructs the bloodtrail driver
	// below -- see installLogCapture's own doc (staleness_integration_test.go)
	// -- so the engine-mode served-floor tally (dawgsCorpusServedFloor's
	// own doc) can count cypherServedMarker occurrences.
	buf := installLogCapture(t)

	// graphtest.OpenPG asserts a bare default-graph schema on a throwaway
	// *pg.Driver purely to get a *pgxpool.Pool sized and configured the way
	// every other integration test's pool is; the pgDriver return is unused
	// here (corpusSchema below asserts the real, kind-populated schema
	// through the wrapped driver itself, and every dataset group clears the
	// graph before loading, so no upfront wipe is needed either).
	_, pool := graphtest.OpenPG(t, dsn)

	ctx := context.Background()
	cfg := dawgs.Config{ConnectionString: dsn, GraphQueryMemoryLimit: size.Gibibyte, Pool: pool}

	// Hand-construct the driver via dawgs.Open with this package's own
	// DriverName, exactly as driver_integration_test.go does -- deliberately
	// not integration.Open/SetupDB, whose connection-string scheme map
	// (harness.go's DriverFromConnectionString) only knows postgresql/neo4j
	// schemes and would never resolve to bloodtrail.
	rawBT, err := dawgs.Open(ctx, DriverName, cfg)
	if err != nil {
		t.Fatalf("open bloodtrail: %v", err)
	}
	defer func() { _ = rawBT.Close(ctx) }()

	d, ok := rawBT.(*Driver)
	if !ok {
		t.Fatalf("expected *Driver, got %T", rawBT)
	}
	db := graph.Database(d)

	schema := corpusSchema(t, datasetNames, datasetPath)

	// Fixture cases carry their own inline node/edge kinds, and some of
	// those kinds never appear in any dataset file -- see fixtureKinds' doc
	// for why they still need declaring up front rather than left to the
	// lazy definition WriteGraphTx's transaction methods would otherwise
	// provide.
	fixtureNodeKinds, fixtureEdgeKinds := fixtureKinds(allCaseFiles)
	schema.Graphs[0].Nodes = schema.Graphs[0].Nodes.Add(fixtureNodeKinds...)
	schema.Graphs[0].Edges = schema.Graphs[0].Edges.Add(fixtureEdgeKinds...)

	if err := d.AssertSchema(ctx, schema); err != nil {
		t.Fatalf("assert schema: %v", err)
	}

	// Start's boot-load goroutine (driver.go) is launched asynchronously by
	// dawgs.Open above. Waiting for it to settle here ensures the test's
	// RebuildCount and serving baselines are deterministic, not dependent on
	// the boot-load's own one-shot rebuild timing.
	waitForBootLoad(t, d)

	// Hand-constructed per the brief above: integration.Open's scheme
	// detection excludes bloodtrail, but Session itself is a plain struct
	// with exported fields, and WithRollbackFixture (harness.go) needs
	// nothing else.
	session := &integration.Session{DB: db, Ctx: ctx}

	// totalEngineServed accumulates a cypherServedMarker delta around every
	// dataset group's read-only pass below -- this suite's served floor
	// (dawgsCorpusServedFloor's own doc), a suite-wide anti-vacuity tally
	// distinct from any single case's own pass/fail: a regression that made
	// interpret.Plan/translateGateOK decline far more broadly could still
	// leave every individual case green (delegation to PostgreSQL is always
	// correct -- see this file's package doc), silently defeating the
	// entire point of tracking which cases the engine actually serves.
	var totalEngineServed int

	for _, ds := range datasetNames {
		g := groups[ds]

		t.Run(ds, func(t *testing.T) {
			// ClearGraph/LoadDataset both write through the wrapped driver
			// (db, not a raw pg.Driver) -- see this file's package doc for
			// why ClearGraph's own delete trips the engine into fallback and
			// starts an automatic recovery rebuild before LoadDataset even
			// begins.
			integration.ClearGraph(t, db, ctx)
			idMap := session.LoadDataset(t, datasetPath(ds))

			// Force a deterministic snapshot rebuild from this group's data
			// before running any case below: RebuildNow is a manual,
			// on-demand call, independent of both Start's own one-shot
			// boot-load goroutine (engine/boot.go) and the automatic
			// recovery rebuild ClearGraph's own fallback trip already
			// started (see the package doc for why the two end up reading
			// the same committed state either way, and why this call exists
			// for determinism rather than to be the first thing that makes
			// the engine servable here).
			if err := d.engine.RebuildNow(ctx, "manual"); err != nil {
				t.Fatalf("RebuildNow (dataset %q): %v", ds, err)
			}

			// Every shape internal/engine/interpret's Plan actually accepts
			// gets a real chance to be served from the snapshot RebuildNow
			// just produced; the rest of the corpus is shapes Plan declines
			// and TryCypher delegates to PostgreSQL instead. The
			// cypherServedMarker delta around this loop counts every case
			// this dataset group actually served (each served case logs the
			// marker exactly once, from TryCypher's own single tx.Query
			// call per case -- see runReadOnly), folded into
			// totalEngineServed for the suite-wide floor checked after the
			// outer loop.
			servedBefore := markerCount(buf, cypherServedMarker)
			for _, cf := range g.files {
				for _, tc := range cf.Cases {
					if tc.Fixture != nil {
						continue
					}

					tc := tc
					t.Run(tc.Name, func(t *testing.T) {
						defer func() {
							if r := recover(); r != nil {
								t.Fatalf("panic: %v", r)
							}
						}()

						check := parseAssertion(t, tc.Assert)
						runReadOnly(t, ctx, db, idMap, tc, check)
					})
				}
			}
			totalEngineServed += markerCount(buf, cypherServedMarker) - servedBefore

			// Fixture cases: each runs once, in a rolled-back write
			// transaction that never reaches the engine either way (see
			// this file's package doc).
			t.Run("fixture", func(t *testing.T) {
				for _, cf := range g.files {
					for _, tc := range cf.Cases {
						if tc.Fixture == nil {
							continue
						}

						tc := tc
						t.Run(tc.Name, func(t *testing.T) {
							defer func() {
								if r := recover(); r != nil {
									t.Fatalf("panic: %v", r)
								}
							}()

							check := parseAssertion(t, tc.Assert)
							runWithFixture(t, ctx, db, tc, check)
						})
					}
				}
			})
		})
	}

	t.Logf("DAWGS corpus: engine mode served %d read-only cases across every dataset group", totalEngineServed)
	if totalEngineServed < dawgsCorpusServedFloor {
		t.Errorf("DAWGS corpus engine mode served only %d cases, want >= %d (see dawgsCorpusServedFloor's doc) -- a regression may have made the interpreter decline far more broadly than expected", totalEngineServed, dawgsCorpusServedFloor)
	}
}

// parseAssertion converts a JSON assertion value into a function that checks
// a query result. Supports:
//
//	"non_empty"                           — at least one row
//	"empty"                               — zero rows
//	"no_error"                            — drains result, checks no error
//	"query_error"                         — drains result, expects an error
//	{"keys": ["a", "b"]}                  — every returned row has exactly these result keys, preserving order
//	{"row_count": N}                      — exactly N rows
//	{"at_least_int": N}                   — first scalar >= N
//	{"exact_int": N}                      — first scalar == N
//	{"scalar_values": [V...]}             — exact multiset of first scalar values, order-independent
//	{"ordered_scalar_values": [V...]}     — exact first scalar values, preserving row order
//	{"row_values": [[V...]]}              — exact multiset of scalar row values, order-independent
//	{"ordered_row_values": [[V...]]}      — exact scalar row values, preserving row order
//	{"contains_node_with_prop": [K, V]}   — some row has a node with property K=V
//	{"contains_node_with_props": {K: V}}  — some row has a node with all listed properties
//	{"contains_edge": {start,end,kind,props}} — some row/path has a relationship matching all listed fields
//	{"node_ids": ["a", "b"]}              — exact multiset of returned fixture node IDs, order-independent
//	{"node_id_set": ["a", "b"]}           — exact set of returned fixture node IDs, order-independent
//	{"ordered_node_ids": ["a", "b"]}      — first returned node ID per row, preserving row order
//	{"node_list_ids": [["a", "b"]]}       — exact multiset of returned node-list ID sequences
//	{"path_node_ids": [["a", "b"]]}       — exact multiset of returned path node ID sequences
//	{"path_lengths": [N...]}              — exact multiset of returned path edge counts
//	{"path_edge_kinds": [["K"...]]}       — exact multiset of returned path edge kind sequences
//	{"relationship_list_kinds": [["K"...]]} — exact multiset of returned relationship-list kind sequences
//
// Object assertions may combine multiple keys; every assertion must pass.
//
// Verbatim from upstream's integration/cypher_test.go.
func parseAssertion(t *testing.T, raw json.RawMessage) caseAssertion {
	t.Helper()

	// Try as a simple string first.
	var str string
	if err := json.Unmarshal(raw, &str); err == nil {
		switch str {
		case "non_empty":
			return caseAssertion{check: assertNonEmpty}
		case "empty":
			return caseAssertion{check: assertEmpty}
		case "no_error":
			return caseAssertion{check: assertNoError}
		case "query_error":
			return caseAssertion{expectQueryError: true}
		default:
			t.Fatalf("unknown string assertion: %q", str)
		}
	}

	// Otherwise it's an object with one or more assertions.
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		t.Fatalf("failed to parse assertion: %v", err)
	}

	var assertions []resultAssertion
	for key, val := range obj {
		switch key {
		case "keys":
			assertions = append(assertions, assertKeys(decodeAssertionValue[[]string](t, key, val)))

		case "row_count":
			assertions = append(assertions, assertRowCount(decodeAssertionValue[int](t, key, val)))

		case "at_least_int":
			assertions = append(assertions, assertAtLeastInt64(decodeAssertionValue[int64](t, key, val)))

		case "exact_int":
			assertions = append(assertions, assertExactInt64(decodeAssertionValue[int64](t, key, val)))

		case "scalar_values":
			assertions = append(assertions, assertScalarValues(decodeAssertionValue[[]any](t, key, val), false))

		case "ordered_scalar_values":
			assertions = append(assertions, assertScalarValues(decodeAssertionValue[[]any](t, key, val), true))

		case "row_values":
			assertions = append(assertions, assertRowValues(decodeAssertionValue[[][]any](t, key, val), false))

		case "ordered_row_values":
			assertions = append(assertions, assertRowValues(decodeAssertionValue[[][]any](t, key, val), true))

		case "contains_node_with_prop":
			pair := decodeAssertionValue[[2]string](t, key, val)
			assertions = append(assertions, assertContainsNodeWithProp(pair[0], pair[1]))

		case "contains_node_with_props":
			assertions = append(assertions, assertContainsNodeWithProps(decodeAssertionValue[map[string]any](t, key, val)))

		case "contains_edge":
			assertions = append(assertions, assertContainsEdge(decodeAssertionValue[edgeExpectation](t, key, val)))

		case "node_ids":
			assertions = append(assertions, assertNodeIDs(decodeAssertionValue[[]string](t, key, val), false))

		case "node_id_set":
			assertions = append(assertions, assertNodeIDs(decodeAssertionValue[[]string](t, key, val), true))

		case "ordered_node_ids":
			assertions = append(assertions, assertOrderedNodeIDs(decodeAssertionValue[[]string](t, key, val)))

		case "node_list_ids":
			assertions = append(assertions, assertNodeListIDs(decodeAssertionValue[[][]string](t, key, val)))

		case "path_node_ids":
			assertions = append(assertions, assertPathNodeIDs(decodeAssertionValue[[][]string](t, key, val)))

		case "path_lengths":
			assertions = append(assertions, assertPathLengths(decodeAssertionValue[[]int](t, key, val)))

		case "path_edge_kinds":
			assertions = append(assertions, assertPathEdgeKinds(decodeAssertionValue[[][]string](t, key, val)))

		case "relationship_list_kinds":
			assertions = append(assertions, assertRelationshipListKinds(decodeAssertionValue[[][]string](t, key, val)))

		default:
			t.Fatalf("unknown assertion key: %q", key)
		}
	}

	if len(assertions) == 0 {
		t.Fatal("empty assertion object")
	}

	return caseAssertion{
		check: func(t *testing.T, result queryResult, ctx assertionContext) {
			t.Helper()

			for _, assertion := range assertions {
				assertion(t, result, ctx)
			}
		},
	}
}

// runReadOnly executes a test case against the pre-loaded dataset.
//
// Verbatim from upstream's integration/cypher_test.go.
func runReadOnly(t *testing.T, ctx context.Context, db graph.Database, idMap opengraph.IDMap, tc testCase, assertion caseAssertion) {
	t.Helper()

	var (
		queryErrorObserved = false
		err                = db.ReadTransaction(ctx, func(tx graph.Transaction) error {
			result := tx.Query(tc.Cypher, tc.Params)
			defer result.Close()
			assertion.checkResult(t, result, newAssertionContext(idMap))
			if assertion.expectQueryError {
				queryErrorObserved = true
			}
			return nil
		})
	)

	if err != nil {
		if assertion.expectQueryError && queryErrorObserved {
			return
		}

		t.Fatalf("transaction failed: %v", err)
	}
}

// runWithFixture creates inline fixture data in a write transaction, runs the
// query, checks the assertion, then rolls back so the data doesn't persist.
//
// Verbatim from upstream's integration/cypher_test.go, save for constructing
// integration.Session directly (DB/Ctx only) instead of via integration.Open.
func runWithFixture(t *testing.T, ctx context.Context, db graph.Database, tc testCase, assertion caseAssertion) {
	t.Helper()

	queryErrorObserved := false
	session := &integration.Session{DB: db, Ctx: ctx}
	err := session.WithRollbackFixture(t, tc.Fixture, true, func(tx graph.Transaction, idMap opengraph.IDMap) error {
		result := tx.Query(tc.Cypher, tc.Params)
		defer result.Close()
		assertion.checkResult(t, result, newAssertionContext(idMap))
		if assertion.expectQueryError {
			queryErrorObserved = true
		}

		return nil
	})

	if assertion.expectQueryError && queryErrorObserved && err != nil {
		return
	}

	if err != nil {
		t.Fatalf("unexpected transaction error: %v", err)
	}
}

// --- Assertion implementations ---
//
// Everything below is verbatim from upstream's integration/cypher_test.go.

type caseAssertion struct {
	check            resultAssertion
	expectQueryError bool
}

type resultAssertion func(*testing.T, queryResult, assertionContext)

func (s caseAssertion) checkResult(t *testing.T, result graph.Result, ctx assertionContext) {
	t.Helper()

	if s.expectQueryError {
		assertQueryError(t, result)
		return
	}

	if s.check == nil {
		t.Fatal("assertion has no result check")
	}

	s.check(t, collectResult(t, result), ctx)
}

type assertionContext struct {
	fixtureIDByID map[graph.ID]string
}

func newAssertionContext(idMap opengraph.IDMap) assertionContext {
	ctx := assertionContext{
		fixtureIDByID: make(map[graph.ID]string, len(idMap)),
	}

	for fixtureID, dbID := range idMap {
		ctx.fixtureIDByID[dbID] = fixtureID
	}

	return ctx
}

func (s assertionContext) fixtureID(t *testing.T, dbID graph.ID) string {
	t.Helper()

	if fixtureID, found := s.fixtureIDByID[dbID]; found {
		return fixtureID
	}

	t.Fatalf("database node ID %d was not found in the assertion fixture ID map", dbID)
	return ""
}

type resultRow struct {
	keys   []string
	values []any
}

type queryResult struct {
	rows   []resultRow
	mapper graph.ValueMapper
}

func collectResult(t *testing.T, result graph.Result) queryResult {
	t.Helper()

	collected := queryResult{
		mapper: result.Mapper(),
	}

	for result.Next() {
		collected.rows = append(collected.rows, resultRow{
			keys:   append([]string(nil), result.Keys()...),
			values: append([]any(nil), result.Values()...),
		})
	}

	if err := result.Error(); err != nil {
		t.Fatalf("query error: %v", err)
	}

	return collected
}

func assertQueryError(t *testing.T, result graph.Result) {
	t.Helper()

	for result.Next() {
	}

	if err := result.Error(); err == nil {
		t.Fatal("expected query error but query completed successfully")
	}
}

func decodeAssertionValue[T any](t *testing.T, key string, raw json.RawMessage) T {
	t.Helper()

	var value T
	if err := json.Unmarshal(raw, &value); err != nil {
		t.Fatalf("failed to decode assertion %q: %v", key, err)
	}

	return value
}

func assertNonEmpty(t *testing.T, result queryResult, _ assertionContext) {
	t.Helper()
	if len(result.rows) == 0 {
		t.Fatal("expected non-empty result set")
	}
}

func assertEmpty(t *testing.T, result queryResult, _ assertionContext) {
	t.Helper()
	if len(result.rows) > 0 {
		t.Fatalf("expected empty result set but got %d rows", len(result.rows))
	}
}

func assertNoError(t *testing.T, _ queryResult, _ assertionContext) {
	t.Helper()
}

func assertKeys(expected []string) resultAssertion {
	return func(t *testing.T, result queryResult, _ assertionContext) {
		t.Helper()

		if len(result.rows) == 0 {
			t.Fatal("key assertion expected at least one row")
		}

		want := strings.Join(expected, "\x00")
		for rowIdx, row := range result.rows {
			if got := strings.Join(row.keys, "\x00"); got != want {
				t.Fatalf("row %d keys mismatch:\n  got:  %v\n  want: %v", rowIdx, row.keys, expected)
			}
		}
	}
}

func assertRowCount(n int) resultAssertion {
	return func(t *testing.T, result queryResult, _ assertionContext) {
		t.Helper()
		if count := len(result.rows); count != n {
			t.Fatalf("row count: got %d, want %d", count, n)
		}
	}
}

func assertAtLeastInt64(min int64) resultAssertion {
	return func(t *testing.T, result queryResult, _ assertionContext) {
		t.Helper()
		if len(result.rows) == 0 {
			t.Fatal("no rows returned")
		}
		val, ok := asInt64(firstScalarValue(t, result))
		if !ok {
			value := firstScalarValue(t, result)
			t.Fatalf("expected integer, got %T: %v", value, value)
		}
		if val < min {
			t.Fatalf("got %d, want >= %d", val, min)
		}
	}
}

func assertExactInt64(expected int64) resultAssertion {
	return func(t *testing.T, result queryResult, _ assertionContext) {
		t.Helper()
		if len(result.rows) != 1 {
			t.Fatalf("exact integer assertion expected one row, got %d", len(result.rows))
		}
		val, ok := asInt64(firstScalarValue(t, result))
		if !ok {
			value := firstScalarValue(t, result)
			t.Fatalf("expected integer, got %T: %v", value, value)
		}
		if val != expected {
			t.Fatalf("got %d, want %d", val, expected)
		}
	}
}

func assertScalarValues(expected []any, ordered bool) resultAssertion {
	return func(t *testing.T, result queryResult, _ assertionContext) {
		t.Helper()

		got := make([]string, 0, len(result.rows))
		for rowIdx, row := range result.rows {
			if len(row.values) == 0 {
				t.Fatalf("row %d has no values", rowIdx)
			}

			got = append(got, scalarSignature(row.values[0]))
		}

		want := make([]string, len(expected))
		for idx, expectedValue := range expected {
			want[idx] = scalarSignature(expectedValue)
		}

		if ordered {
			if strings.Join(got, "\x00") != strings.Join(want, "\x00") {
				t.Fatalf("ordered scalar values mismatch:\n  got:  %v\n  want: %v", got, want)
			}
		} else {
			assertStringMultiset(t, got, want, "scalar values")
		}
	}
}

func assertRowValues(expected [][]any, ordered bool) resultAssertion {
	return func(t *testing.T, result queryResult, _ assertionContext) {
		t.Helper()

		got := make([]string, 0, len(result.rows))
		for _, row := range result.rows {
			got = append(got, rowScalarSignature(row.values))
		}

		want := make([]string, len(expected))
		for idx, expectedRow := range expected {
			want[idx] = rowScalarSignature(expectedRow)
		}

		if ordered {
			if strings.Join(got, "\x00") != strings.Join(want, "\x00") {
				t.Fatalf("ordered row values mismatch:\n  got:  %v\n  want: %v", got, want)
			}
		} else {
			assertStringMultiset(t, got, want, "row values")
		}
	}
}

func firstScalarValue(t *testing.T, result queryResult) any {
	t.Helper()

	if len(result.rows) == 0 {
		t.Fatal("no rows returned")
	}

	if len(result.rows[0].values) == 0 {
		t.Fatal("first row has no values")
	}

	return result.rows[0].values[0]
}

func asInt64(value any) (int64, bool) {
	switch typedValue := value.(type) {
	case int:
		return int64(typedValue), true
	case int8:
		return int64(typedValue), true
	case int16:
		return int64(typedValue), true
	case int32:
		return int64(typedValue), true
	case int64:
		return typedValue, true
	case uint:
		if uint64(typedValue) <= math.MaxInt64 {
			return int64(typedValue), true
		}
	case uint8:
		return int64(typedValue), true
	case uint16:
		return int64(typedValue), true
	case uint32:
		return int64(typedValue), true
	case uint64:
		if typedValue <= math.MaxInt64 {
			return int64(typedValue), true
		}
	case float32:
		if math.Trunc(float64(typedValue)) == float64(typedValue) {
			return int64(typedValue), true
		}
	case float64:
		if math.Trunc(typedValue) == typedValue {
			return int64(typedValue), true
		}
	}

	return 0, false
}

func rowScalarSignature(values []any) string {
	parts := make([]string, len(values))
	for idx, value := range values {
		parts[idx] = scalarSignature(value)
	}

	encoded, err := json.Marshal(parts)
	if err != nil {
		return strings.Join(parts, "\x00")
	}

	return string(encoded)
}

func scalarSignature(value any) string {
	if value == nil {
		return "null:"
	}

	if number, ok := asFloat64(value); ok {
		return fmt.Sprintf("number:%g", number)
	}

	switch typedValue := value.(type) {
	case string:
		return "string:" + typedValue
	case bool:
		return fmt.Sprintf("bool:%t", typedValue)
	default:
		if encoded, err := json.Marshal(typedValue); err == nil {
			if signature, ok := jsonNumberSignature(encoded); ok {
				return signature
			}

			return fmt.Sprintf("json:%s", encoded)
		}

		return fmt.Sprintf("%T:%v", typedValue, typedValue)
	}
}

func jsonNumberSignature(encoded []byte) (string, bool) {
	decoder := json.NewDecoder(strings.NewReader(string(encoded)))
	decoder.UseNumber()

	var decoded any
	if err := decoder.Decode(&decoded); err != nil {
		return "", false
	}

	number, ok := decoded.(json.Number)
	if !ok {
		return "", false
	}

	value, err := number.Float64()
	if err != nil {
		return "", false
	}

	return fmt.Sprintf("number:%g", value), true
}

func assertContainsNodeWithProp(key, expected string) resultAssertion {
	return func(t *testing.T, result queryResult, _ assertionContext) {
		t.Helper()
		for _, row := range result.rows {
			for _, rawVal := range row.values {
				var node graph.Node
				if result.mapper.Map(rawVal, &node) {
					if s, err := node.Properties.Get(key).String(); err == nil && s == expected {
						return
					}
				}
			}
		}
		t.Fatalf("no row contains a node with %s = %q", key, expected)
	}
}

func assertContainsNodeWithProps(expected map[string]any) resultAssertion {
	return func(t *testing.T, result queryResult, _ assertionContext) {
		t.Helper()

		for _, row := range result.rows {
			for _, rawVal := range row.values {
				var node graph.Node
				if result.mapper.Map(rawVal, &node) && propertiesMatch(node.Properties, expected) {
					return
				}

				var path graph.Path
				if result.mapper.Map(rawVal, &path) {
					for _, pathNode := range path.Nodes {
						if pathNode != nil && propertiesMatch(pathNode.Properties, expected) {
							return
						}
					}
				}
			}
		}

		t.Fatalf("no row contains a node with properties %v", expected)
	}
}

type edgeExpectation struct {
	Start string         `json:"start,omitempty"`
	End   string         `json:"end,omitempty"`
	Kind  string         `json:"kind,omitempty"`
	Props map[string]any `json:"props,omitempty"`
}

func assertContainsEdge(expected edgeExpectation) resultAssertion {
	return func(t *testing.T, result queryResult, ctx assertionContext) {
		t.Helper()

		for _, relationship := range collectRelationships(t, result) {
			if relationshipMatches(t, relationship, expected, ctx) {
				return
			}
		}

		t.Fatalf("no row contains an edge matching %+v", expected)
	}
}

func assertNodeIDs(expected []string, unique bool) resultAssertion {
	return func(t *testing.T, result queryResult, ctx assertionContext) {
		t.Helper()

		got := collectNodeIDs(t, result, ctx, unique)
		assertStringMultiset(t, got, expected, "node IDs")
	}
}

func assertOrderedNodeIDs(expected []string) resultAssertion {
	return func(t *testing.T, result queryResult, ctx assertionContext) {
		t.Helper()

		got := make([]string, 0, len(result.rows))
		for rowIdx, row := range result.rows {
			var found bool
			for _, rawVal := range row.values {
				var node graph.Node
				if result.mapper.Map(rawVal, &node) {
					got = append(got, ctx.fixtureID(t, node.ID))
					found = true
					break
				}
			}

			if !found {
				t.Fatalf("row %d did not contain a node value", rowIdx)
			}
		}

		if strings.Join(got, "\x00") != strings.Join(expected, "\x00") {
			t.Fatalf("ordered node IDs mismatch:\n  got:  %v\n  want: %v", got, expected)
		}
	}
}

func assertNodeListIDs(expected [][]string) resultAssertion {
	return func(t *testing.T, result queryResult, ctx assertionContext) {
		t.Helper()

		got := make([]string, 0, len(result.rows))
		for _, row := range result.rows {
			for _, rawVal := range row.values {
				var nodes []*graph.Node
				if result.mapper.Map(rawVal, &nodes) {
					got = append(got, nodeListIDSignature(t, nodes, ctx))
				}
			}
		}

		want := make([]string, len(expected))
		for idx, nodeIDs := range expected {
			want[idx] = strings.Join(nodeIDs, "->")
		}

		assertStringMultiset(t, got, want, "node-list ID sequences")
	}
}

func collectNodeIDs(t *testing.T, result queryResult, ctx assertionContext, unique bool) []string {
	t.Helper()

	var (
		ids  = make([]string, 0, len(result.rows))
		seen = map[string]bool{}
	)

	for _, row := range result.rows {
		for _, rawVal := range row.values {
			var node graph.Node
			if result.mapper.Map(rawVal, &node) {
				fixtureID := ctx.fixtureID(t, node.ID)
				if unique {
					if seen[fixtureID] {
						continue
					}
					seen[fixtureID] = true
				}

				ids = append(ids, fixtureID)
			}
		}
	}

	return ids
}

func nodeListIDSignature(t *testing.T, nodes []*graph.Node, ctx assertionContext) string {
	t.Helper()

	nodeIDs := make([]string, len(nodes))
	for idx, node := range nodes {
		if node == nil {
			t.Fatalf("node list contains nil node at index %d", idx)
		}

		nodeIDs[idx] = ctx.fixtureID(t, node.ID)
	}

	return strings.Join(nodeIDs, "->")
}

func assertPathNodeIDs(expected [][]string) resultAssertion {
	return func(t *testing.T, result queryResult, ctx assertionContext) {
		t.Helper()

		got := make([]string, 0, len(result.rows))
		for _, row := range result.rows {
			for _, rawVal := range row.values {
				var path graph.Path
				if result.mapper.Map(rawVal, &path) {
					got = append(got, pathNodeIDSignature(t, path, ctx))
				}
			}
		}

		want := make([]string, len(expected))
		for idx, pathNodeIDs := range expected {
			want[idx] = strings.Join(pathNodeIDs, "->")
		}

		assertStringMultiset(t, got, want, "path node ID sequences")
	}
}

func assertPathLengths(expected []int) resultAssertion {
	return func(t *testing.T, result queryResult, _ assertionContext) {
		t.Helper()

		got := make([]string, 0, len(result.rows))
		for _, path := range collectPaths(t, result) {
			got = append(got, fmt.Sprintf("%d", len(path.Edges)))
		}

		want := make([]string, len(expected))
		for idx, expectedLength := range expected {
			want[idx] = fmt.Sprintf("%d", expectedLength)
		}

		assertStringMultiset(t, got, want, "path lengths")
	}
}

func assertPathEdgeKinds(expected [][]string) resultAssertion {
	return func(t *testing.T, result queryResult, _ assertionContext) {
		t.Helper()

		got := make([]string, 0, len(result.rows))
		for _, path := range collectPaths(t, result) {
			got = append(got, pathEdgeKindSignature(t, path))
		}

		want := make([]string, len(expected))
		for idx, expectedKinds := range expected {
			want[idx] = strings.Join(expectedKinds, "->")
		}

		assertStringMultiset(t, got, want, "path edge kind sequences")
	}
}

func assertRelationshipListKinds(expected [][]string) resultAssertion {
	return func(t *testing.T, result queryResult, _ assertionContext) {
		t.Helper()

		got := make([]string, 0, len(result.rows))
		for _, row := range result.rows {
			for _, rawVal := range row.values {
				var relationshipPointers []*graph.Relationship
				if result.mapper.Map(rawVal, &relationshipPointers) {
					got = append(got, relationshipListKindSignature(t, relationshipPointers))
					continue
				}

				var relationships []graph.Relationship
				if result.mapper.Map(rawVal, &relationships) {
					got = append(got, relationshipValueListKindSignature(t, relationships))
				}
			}
		}

		want := make([]string, len(expected))
		for idx, expectedKinds := range expected {
			want[idx] = strings.Join(expectedKinds, "->")
		}

		assertStringMultiset(t, got, want, "relationship-list kind sequences")
	}
}

func pathNodeIDSignature(t *testing.T, path graph.Path, ctx assertionContext) string {
	t.Helper()

	nodeIDs := make([]string, len(path.Nodes))
	for idx, node := range path.Nodes {
		if node == nil {
			t.Fatalf("path contains nil node at index %d", idx)
		}

		nodeIDs[idx] = ctx.fixtureID(t, node.ID)
	}

	return strings.Join(nodeIDs, "->")
}

func pathEdgeKindSignature(t *testing.T, path graph.Path) string {
	t.Helper()

	edgeKinds := make([]string, len(path.Edges))
	for idx, edge := range path.Edges {
		if edge == nil {
			t.Fatalf("path contains nil edge at index %d", idx)
		}

		if edge.Kind == nil {
			t.Fatalf("path edge at index %d has nil kind", idx)
		}

		edgeKinds[idx] = edge.Kind.String()
	}

	return strings.Join(edgeKinds, "->")
}

func relationshipListKindSignature(t *testing.T, relationships []*graph.Relationship) string {
	t.Helper()

	edgeKinds := make([]string, len(relationships))
	for idx, relationship := range relationships {
		if relationship == nil {
			t.Fatalf("relationship list contains nil relationship at index %d", idx)
		}

		if relationship.Kind == nil {
			t.Fatalf("relationship list item at index %d has nil kind", idx)
		}

		edgeKinds[idx] = relationship.Kind.String()
	}

	return strings.Join(edgeKinds, "->")
}

func relationshipValueListKindSignature(t *testing.T, relationships []graph.Relationship) string {
	t.Helper()

	edgeKinds := make([]string, len(relationships))
	for idx, relationship := range relationships {
		if relationship.Kind == nil {
			t.Fatalf("relationship list item at index %d has nil kind", idx)
		}

		edgeKinds[idx] = relationship.Kind.String()
	}

	return strings.Join(edgeKinds, "->")
}

func collectPaths(t *testing.T, result queryResult) []graph.Path {
	t.Helper()

	var paths []graph.Path
	for _, row := range result.rows {
		for _, rawVal := range row.values {
			var path graph.Path
			if result.mapper.Map(rawVal, &path) {
				paths = append(paths, path)
			}
		}
	}

	return paths
}

func collectRelationships(t *testing.T, result queryResult) []graph.Relationship {
	t.Helper()

	var relationships []graph.Relationship
	for _, row := range result.rows {
		for _, rawVal := range row.values {
			var relationship graph.Relationship
			if result.mapper.Map(rawVal, &relationship) {
				relationships = append(relationships, relationship)
			}

			var path graph.Path
			if result.mapper.Map(rawVal, &path) {
				for _, pathRelationship := range path.Edges {
					if pathRelationship != nil {
						relationships = append(relationships, *pathRelationship)
					}
				}
			}
		}
	}

	return relationships
}

func relationshipMatches(t *testing.T, relationship graph.Relationship, expected edgeExpectation, ctx assertionContext) bool {
	t.Helper()

	if expected.Start != "" && ctx.fixtureID(t, relationship.StartID) != expected.Start {
		return false
	}

	if expected.End != "" && ctx.fixtureID(t, relationship.EndID) != expected.End {
		return false
	}

	if expected.Kind != "" {
		if relationship.Kind == nil || relationship.Kind.String() != expected.Kind {
			return false
		}
	}

	return propertiesMatch(relationship.Properties, expected.Props)
}

func propertiesMatch(properties *graph.Properties, expected map[string]any) bool {
	if len(expected) == 0 {
		return true
	}

	if properties == nil {
		return false
	}

	for key, expectedValue := range expected {
		actualValue := properties.Get(key).Any()
		if !valuesEqual(actualValue, expectedValue) {
			return false
		}
	}

	return true
}

func valuesEqual(actual, expected any) bool {
	if actualNumber, actualIsNumber := asFloat64(actual); actualIsNumber {
		if expectedNumber, expectedIsNumber := asFloat64(expected); expectedIsNumber {
			return actualNumber == expectedNumber
		}
	}

	return reflect.DeepEqual(actual, expected)
}

func asFloat64(value any) (float64, bool) {
	switch typedValue := value.(type) {
	case int:
		return float64(typedValue), true
	case int8:
		return float64(typedValue), true
	case int16:
		return float64(typedValue), true
	case int32:
		return float64(typedValue), true
	case int64:
		return float64(typedValue), true
	case uint:
		return float64(typedValue), true
	case uint8:
		return float64(typedValue), true
	case uint16:
		return float64(typedValue), true
	case uint32:
		return float64(typedValue), true
	case uint64:
		return float64(typedValue), true
	case float32:
		return float64(typedValue), true
	case float64:
		return typedValue, true
	default:
		return 0, false
	}
}

func assertStringMultiset(t *testing.T, got, expected []string, label string) {
	t.Helper()

	got = append([]string(nil), got...)
	expected = append([]string(nil), expected...)

	sort.Strings(got)
	sort.Strings(expected)

	if len(got) != len(expected) {
		t.Fatalf("%s count: got %d, want %d\n  got:  %v\n  want: %v", label, len(got), len(expected), got, expected)
	}

	for idx := range got {
		if got[idx] != expected[idx] {
			t.Fatalf("%s mismatch at index %d:\n  got:  %v\n  want: %v", label, idx, got, expected)
		}
	}
}
