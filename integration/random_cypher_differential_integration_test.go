// SPDX-License-Identifier: Apache-2.0

//go:build integration

// This file is a second randomized differential family, alongside
// internal/engine/random_differential_integration_test.go's path-query
// suite (TestRandomDifferential, which exercises TryAllShortestPaths over
// graphtest.LoadRandom's plain, property-free 60-node graphs) and
// prebuilt_corpus_integration_test.go's fixed, hand-curated corpus
// (TestPrebuiltCorpusDifferential). Here the query text itself is randomly
// generated -- single- and two-Part read Cypher over a small, deliberately
// adversarial property fixture -- and checked against the plain pg driver,
// both via the *bloodtrail.Driver wrapper's own engine-then-delegate
// serving policy.
//
// # File placement: package bloodtrail, not internal/engine
//
// Running through the bloodtrail driver -- rather than the engine package
// directly, the way internal/engine/random_differential_integration_test.go
// does -- exercises the wrapper's own ReadTransaction -> wrappedTransaction.
// Query -> TryCypher-then-fallback policy (transaction.go), which needs the
// actual *bloodtrail.Driver type, one internal/engine cannot import (this
// root package already imports internal/engine; the reverse would be a
// cycle). Every existing
// suite with the identical need -- a live engine/oracle pair compared
// through the real driver, with a deterministic bloodtrail.TestingEngine(d).RebuildNow and the
// cypherServedMarker log-capture plumbing -- already lives here instead
// (dawgs_corpus_integration_test.go, builder_differential_matrix_
// integration_test.go, prebuilt_corpus_integration_test.go's own placement
// doc explains the same reasoning at length). This file follows that same
// precedent.
package integration

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/specterops/dawgs"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"
	"github.com/specterops/dawgs/util/size"

	"github.com/MihhailSokolov/BloodTrail/internal/graphtest"

	bloodtrail "github.com/MihhailSokolov/BloodTrail"
)

// --- Adversarial property fixture -----------------------------------------
//
// randomCypherFixtureNodeCount nodes, three kinds (A/B/C -- a subset of
// graphtest.RandomNodeKinds' own alphabet, reused for naming consistency
// even though this fixture is seeded independently of LoadRandom), each
// carrying up to four properties (str/val/flag/tags) whose presence,
// nullness, and value are chosen deterministically by node index so every
// run of this suite sees exactly the same fixture -- see
// randomCypherNodeProperties' doc for the exact trap coverage.

const randomCypherFixtureNodeCount = 40

// randomCypherNodeKindNames and randomCypherEdgeKindNames are this fixture's
// own fixed kind alphabets -- three node kinds, three edge kinds, small
// enough that the ~120 general-connectivity edges below produce plenty of
// same-kind and cross-kind collisions between any two of the 40 nodes.
var (
	randomCypherNodeKindNames = []string{"A", "B", "C"}
	randomCypherEdgeKindNames = []string{"R1", "R2", "R3"}
)

// randomCypherStringPool covers every string trap this suite targets:
// a '%' and a '_' (SQL LIKE metacharacters CONTAINS/STARTS WITH/ENDS WITH
// must match literally, never as wildcards), a backslash, a value leading
// with '{', '[', or '"' (decodeScalarString's own double-decode trigger --
// serve_cypher.go), mixed/lower/upper case (for toLower/toUpper coverage),
// and an empty string.
var randomCypherStringPool = []string{
	"%wildcard%",
	"under_score",
	`back\slash`,
	`{"embedded":1}`,
	`["a","b"]`,
	`"quoted"`,
	"MixedCase",
	"lowercase",
	"UPPERCASE",
	"",
}

// randomCypherNumberPool covers 0, negatives, floats, and plain integers.
var randomCypherNumberPool = []float64{0, -1, -3.5, 2.25, 7, 100, -100, 0.5, 3, -0.001}

var randomCypherBoolPool = []bool{true, false}

// randomCypherArrayPool covers a plain string array, one whose elements
// themselves carry metacharacters, an empty array, a singleton, and a
// case-varied pair -- exercised by the `... IN n.tags` template's own
// randomCypherPickTagCandidate.
var randomCypherArrayPool = [][]string{
	{"a", "b"},
	{"a%b", "c_d"},
	{},
	{"solo"},
	{"UP", "low"},
}

// randomCypherNodeProperties builds node i's properties: each of the four
// keys independently cycles through "missing entirely" / "present as an
// explicit JSON null" / "present with a pool value", using a different
// modulus (and modulus offset) per key so the four keys' missing/null/
// present patterns don't all coincide on the same nodes. A missing key and
// an explicit Set(key, nil) both are real, distinct traps: the former
// never reaches PostgreSQL's jsonb column at all, decodeJSONValue material
// never sees an entry for it; the latter stores a genuine JSON `null` the
// property store and the pg driver's own decodeJSONValue must each resolve
// to Cypher NULL the same way.
//
// i's domain is NOT bounded to [0, randomCypherFixtureNodeCount): every
// modulus/switch below is well-defined for any non-negative i, and
// applyRandomCypherMutation's "create" case calls this with
// len(*idsPtr) -- the running node-id catalog's own current size, which
// grows past randomCypherFixtureNodeCount every time an earlier mutation
// in the sweep creates a node. That is by design (a later create's
// properties keep cycling through the same trap coverage a fixture node's
// would, never falling back to some narrower or degenerate shape once i
// exceeds the fixture's own original 40), not a bug in either this
// function or its caller -- documented here so a future reader does not
// "fix" loadRandomCypherFixture's own call site to somehow clamp i instead.
func randomCypherNodeProperties(i int) *graph.Properties {
	props := graph.NewProperties()

	switch i % 5 {
	case 0:
		// "str" omitted entirely.
	case 1:
		props.Set("str", nil)
	default:
		props.Set("str", randomCypherStringPool[i%len(randomCypherStringPool)])
	}

	switch (i + 1) % 5 {
	case 0:
		// "val" omitted entirely.
	case 1:
		props.Set("val", nil)
	default:
		props.Set("val", randomCypherNumberPool[i%len(randomCypherNumberPool)])
	}

	switch (i + 2) % 4 {
	case 0:
		// "flag" omitted entirely.
	default:
		props.Set("flag", randomCypherBoolPool[i%len(randomCypherBoolPool)])
	}

	switch (i + 3) % 6 {
	case 0:
		// "tags" omitted entirely.
	case 1:
		props.Set("tags", nil)
	default:
		props.Set("tags", randomCypherArrayPool[i%len(randomCypherArrayPool)])
	}

	return props
}

// loadRandomCypherFixture seeds randomCypherFixtureNodeCount nodes (kinds
// A/B/C, properties per randomCypherNodeProperties) and their edges through
// oracle -- writing through the plain pg driver, not the bloodtrail driver
// under test, exactly matching TestPrebuiltCorpusDifferential's own reasoning
// (its "seed via the oracle instance" comment): the very next step
// (bloodtrail.TestingEngine(d).RebuildNow) reads straight from PostgreSQL regardless of which
// driver wrote the rows.
//
// Edges are 120 general-connectivity edges chosen independently and
// uniformly over the 40 nodes and 3 kinds (graphtest.LoadRandom's own
// "self-loops and parallel edges occur naturally" reasoning applies at this
// node/edge ratio too), plus explicit self-loops on nodes 0/7/15 and explicit
// same-pair, different-kind parallel edges between nodes (2,3) and (5,6) --
// guaranteeing both trap shapes regardless of what the random draw alone
// would have produced, so the var-length chain templates always have at
// least one of each to traverse.
//
// Returns the database ids of the 40 generated nodes, in generation order.
func loadRandomCypherFixture(t *testing.T, bt *bloodtrail.Driver, oracle *pg.Driver) []graph.ID {
	t.Helper()
	ctx := context.Background()

	nodeKinds := make(graph.Kinds, len(randomCypherNodeKindNames))
	for i, name := range randomCypherNodeKindNames {
		nodeKinds[i] = graph.StringKind(name)
	}
	edgeKinds := make(graph.Kinds, len(randomCypherEdgeKindNames))
	for i, name := range randomCypherEdgeKindNames {
		edgeKinds[i] = graph.StringKind(name)
	}

	// DefaultGraph must be set here, not left to whatever graphtest.OpenPG
	// asserted on its own separate throwaway driver instance: AssertSchema's
	// default-graph selection is per-driver in-memory state (pg.Driver.
	// SetDefaultGraph/AssertDefaultGraph), not database-wide, and neither bt
	// nor oracle (both opened fresh via dawgs.Open) has asserted one of its
	// own yet -- see internal/graphtest/corpusfixture.go's CorpusSchema doc
	// for the identical reasoning.
	schema := graph.Schema{
		Graphs:       []graph.Graph{{Name: graphtest.GraphName, Nodes: nodeKinds, Edges: edgeKinds}},
		DefaultGraph: graph.Graph{Name: graphtest.GraphName},
	}

	// Asserted on bt's own embedded *pg.Driver instance too, not just
	// oracle: each instance keeps its own in-memory kind-id cache (same
	// reasoning as TestPrebuiltCorpusDifferential's identical two-instance
	// AssertSchema call).
	if err := bt.AssertSchema(ctx, schema); err != nil {
		t.Fatalf("assert adversarial fixture schema on bloodtrail driver: %v", err)
	}
	if err := oracle.AssertSchema(ctx, schema); err != nil {
		t.Fatalf("assert adversarial fixture schema on oracle driver: %v", err)
	}

	// Start's boot-load goroutine (driver.go) races the node/edge creation
	// below and TestRandomCypherDifferential's own RebuildNow otherwise:
	// every write below goes through oracle, the raw pg driver, so nothing
	// bumps applyEpoch, and a still-in-flight boot-load LoadSnapshot could
	// adopt AFTER that RebuildNow and silently overwrite the freshly
	// created fixture with whatever (possibly empty) graph existed before
	// it.
	waitForBootLoad(t, bt)

	ids := make([]graph.ID, randomCypherFixtureNodeCount)
	if err := oracle.WriteTransaction(ctx, func(tx graph.Transaction) error {
		for i := 0; i < randomCypherFixtureNodeCount; i++ {
			kind := nodeKinds[i%len(nodeKinds)]
			node, err := tx.CreateNode(randomCypherNodeProperties(i), kind)
			if err != nil {
				return err
			}
			ids[i] = node.ID
		}
		return nil
	}); err != nil {
		t.Fatalf("create adversarial fixture nodes: %v", err)
	}

	if err := oracle.BatchOperation(ctx, func(batch graph.Batch) error {
		// A fixed, local seed -- unrelated to any query-generation seed --
		// so this fixture is exactly reproducible run to run.
		rng := rand.New(rand.NewSource(20260905))
		const generalEdgeCount = 120
		for i := 0; i < generalEdgeCount; i++ {
			start := ids[rng.Intn(randomCypherFixtureNodeCount)]
			end := ids[rng.Intn(randomCypherFixtureNodeCount)]
			kind := edgeKinds[rng.Intn(len(edgeKinds))]
			if err := batch.CreateRelationshipByIDs(start, end, kind, graph.NewProperties()); err != nil {
				return err
			}
		}

		for _, i := range []int{0, 7, 15} {
			kind := edgeKinds[i%len(edgeKinds)]
			if err := batch.CreateRelationshipByIDs(ids[i], ids[i], kind, graph.NewProperties()); err != nil {
				return err
			}
		}

		for _, pair := range [][2]int{{2, 3}, {5, 6}} {
			for _, kind := range edgeKinds {
				if err := batch.CreateRelationshipByIDs(ids[pair[0]], ids[pair[1]], kind, graph.NewProperties()); err != nil {
					return err
				}
			}
		}

		return nil
	}); err != nil {
		t.Fatalf("create adversarial fixture edges: %v", err)
	}

	return ids
}

// --- Cypher literal rendering ------------------------------------------

// cypherStringLiteral renders s as a single-quoted Cypher string literal,
// escaping backslash and single-quote per the Cypher grammar's EscapedChar
// production (cypher/grammar/Cypher.g4@dawgs v0.8.0: `\\` -> backslash, `\'`
// -> quote) -- the exact escaping interpret/eval.go's decodeCypherStringLiteral
// (a deliberate line-for-line port of dawgs' own translator-side decoder)
// and dawgs' pgsql translator both expect on the way back out, so a value
// round-trips through either engine identically regardless of which
// metacharacters it contains. No other character needs escaping: a leading
// '{', '[', or '"' is just an ordinary character inside a single-quoted
// literal.
func cypherStringLiteral(s string) string {
	var b strings.Builder
	b.WriteByte('\'')
	for _, r := range s {
		switch r {
		case '\\':
			b.WriteString(`\\`)
		case '\'':
			b.WriteString(`\'`)
		default:
			b.WriteRune(r)
		}
	}
	b.WriteByte('\'')
	return b.String()
}

// cypherNumberLiteral renders f as a Cypher REAL literal (grammar's
// RegularDecimalReal: digits, a '.', at least one more digit) --
// deliberately never a bare DecimalInteger, even for a whole number like
// -100: dawgs' pg translator infers a property comparison's SQL cast type
// from the *literal's own* Cypher type (translate.Translate, discovered via
// this suite's own run against a heterogeneous "val" property mixing floats
// and whole numbers under one key), not from the property's actual stored
// values -- an integer literal (`n.val <> -100`) compiles to a `::bigint`
// cast that then fails at runtime with "invalid input syntax for type
// bigint" the instant any candidate row's own "val" happens to be a float
// (e.g. -3.5), on the oracle side only (the in-memory interpreter has no
// notion of pg's per-literal cast inference and just compares float64s
// uniformly, so it disagrees with what PostgreSQL itself would have
// produced for the exact same query against this exact data). Always
// emitting a real literal routes every comparison through pg's
// `::double precision` cast instead, which accepts every value this
// fixture ever stores (int-shaped or not), sidestepping the mismatch
// entirely -- a fix to this suite's own literal rendering (a template-
// design fix), not to the interpreter or the gate.
func cypherNumberLiteral(f float64) string {
	s := strconv.FormatFloat(f, 'f', -1, 64)
	if !strings.ContainsRune(s, '.') {
		s += ".0"
	}
	return s
}

func cypherBoolLiteral(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

func cypherStringListLiteral(values []string) string {
	parts := make([]string, len(values))
	for i, v := range values {
		parts[i] = cypherStringLiteral(v)
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

func cypherNumberListLiteral(values []float64) string {
	parts := make([]string, len(values))
	for i, v := range values {
		parts[i] = cypherNumberLiteral(v)
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

// --- Random query generation ---------------------------------------------

// randomCypherPickKind returns a uniformly random node kind name from
// randomCypherNodeKindNames.
func randomCypherPickKind(rng *rand.Rand) string {
	return randomCypherNodeKindNames[rng.Intn(len(randomCypherNodeKindNames))]
}

// randomCypherPickString returns a random string literal candidate: three
// times out of four, a value from randomCypherStringPool (the same pool the
// fixture's own "str" property cycles through, so predicates frequently
// match at least one node); one time out of four, a value guaranteed absent
// from both the pool and every fixture node's "str"/"tags" values, so the
// generator also regularly exercises the "no rows" case.
func randomCypherPickString(rng *rand.Rand) string {
	if rng.Intn(4) == 0 {
		return fmt.Sprintf("nowhere-in-fixture-%d", rng.Intn(1_000_000))
	}
	return randomCypherStringPool[rng.Intn(len(randomCypherStringPool))]
}

// randomCypherPickNumber is randomCypherPickString's numeric counterpart,
// drawing from randomCypherNumberPool (three times out of four) or a value
// far outside that pool's range (one time out of four).
func randomCypherPickNumber(rng *rand.Rand) float64 {
	if rng.Intn(4) == 0 {
		return 123456.789 + float64(rng.Intn(1000))
	}
	return randomCypherNumberPool[rng.Intn(len(randomCypherNumberPool))]
}

// randomCypherTagPool flattens every distinct string appearing anywhere in
// randomCypherArrayPool, for randomCypherPickTagCandidate below. Kept
// separate from randomCypherStringPool: the two pools are otherwise
// entirely disjoint (no element of one appears in the other), which made
// the `... IN n.tags` template (below) permanently unsatisfiable before
// this fix -- randomCypherPickString could only ever produce a value that
// is not, and never was, an element of any node's actual "tags" array, so
// that predicate always evaluated false for every row regardless of the
// RNG draw, silently never exercising the true-membership code path at
// all.
var randomCypherTagPool = func() []string {
	seen := map[string]bool{}
	var out []string
	for _, arr := range randomCypherArrayPool {
		for _, s := range arr {
			if !seen[s] {
				seen[s] = true
				out = append(out, s)
			}
		}
	}
	return out
}()

// randomCypherPickTagCandidate is randomCypherPickString's counterpart for
// the `... IN n.tags` template: three times out of four, a value actually
// present in some node's "tags" array (randomCypherTagPool), so the
// membership predicate regularly matches at least one node; one time out
// of four, a value guaranteed absent from every tags array, exercising the
// "no rows" case too -- the identical 3-in-4/1-in-4 split
// randomCypherPickString/randomCypherPickNumber already use, just drawing
// from the pool this specific predicate can actually be satisfied against.
func randomCypherPickTagCandidate(rng *rand.Rand) string {
	if rng.Intn(4) == 0 {
		return fmt.Sprintf("nowhere-in-tags-%d", rng.Intn(1_000_000))
	}
	return randomCypherTagPool[rng.Intn(len(randomCypherTagPool))]
}

// Negative literals in `=`/`<>` templates (history): this suite used to
// restrict `=`/`<>` number literals to non-negative values only, via a
// since-removed randomCypherPickNonNegativeNumber helper (math.Abs over
// randomCypherPickNumber), sidestepping a genuine, narrow inconsistency in
// dawgs' own pgsql translator: `n.prop = <literal>`/`n.prop <> <literal>`
// compiled to a native jsonb comparison for a bare positive numeric
// literal, but to a text-extraction-then-cast comparison for a *negative*
// one (a cypher.UnaryAddOrSubtractExpression, not a plain cypher.Literal),
// disagreeing on whether a property present as an explicit JSON null (this
// fixture's own "val" trap) counts as a definite "not equal" (native jsonb:
// yes) or NULL (cast: no, same as a missing property) -- confirmed by
// dumping translate.Translate's generated SQL for both shapes.
//
// That workaround is gone: interpret/plan.go's checkComparison now rejects
// (delegates) any `=`/`<>` between a bare property lookup and a negative
// numeric literal at plan time (see its own doc comment for the full
// derivation), so a negative-literal `=`/`<>` query here always falls
// through to plain pg on both the bloodtrail-driver side and the oracle
// side -- the differential assertion holds by construction regardless of
// sign, and every template below draws from the full pool again.

// randomCypherNullableProps names every property randomCypherNodeProperties
// ever sets or omits, for tmplIsNull/tmplIsNotNull's random property choice.
var randomCypherNullableProps = []string{"str", "val", "flag", "tags"}

// randomCypherTemplate builds one Cypher query text from a seeded rand.Rand.
// Every template below is a single- or two-Part read query (Plan's own two
// accepted top-level shapes -- interpret/plan.go's planStages doc: a bare
// SinglePartQuery, however many MATCH clauses it carries, or a
// MultiPartQuery with exactly one WITH boundary), covering every predicate
// family this suite exercises: property =/<>/</>, STARTS/ENDS WITH,
// CONTAINS, a CONTAINS negation, a COALESCE(...)= guard, IN over both text
// and numeric lists (and list membership against a "tags" array property),
// IS [NOT] NULL, toLower/toUpper comparisons, *0..2/*1..3 var-length chains
// with a kind filter on both ends, a DISTINCT projection, and count(...)
// aggregation gated by a WHERE.
type randomCypherTemplate func(rng *rand.Rand) string

var randomCypherTemplates = []randomCypherTemplate{
	// Single-Part: plain property comparisons. `=`/`<>` draw from the full
	// pool including negatives -- interpret/plan.go's checkComparison now
	// rejects (delegates) a bare property lookup compared against a
	// negative numeric literal via `=`/`<>` at plan time, so this query
	// always runs through plain pg on both sides regardless of sign; see
	// the (removed) randomCypherPickNonNegativeNumber's former doc comment,
	// preserved just above, for the divergence this once worked around.
	func(rng *rand.Rand) string {
		return fmt.Sprintf(`MATCH (n:%s) WHERE n.val = %s RETURN n`, randomCypherPickKind(rng), cypherNumberLiteral(randomCypherPickNumber(rng)))
	},
	func(rng *rand.Rand) string {
		return fmt.Sprintf(`MATCH (n:%s) WHERE n.val <> %s RETURN n`, randomCypherPickKind(rng), cypherNumberLiteral(randomCypherPickNumber(rng)))
	},
	func(rng *rand.Rand) string {
		return fmt.Sprintf(`MATCH (n:%s) WHERE n.val < %s RETURN n`, randomCypherPickKind(rng), cypherNumberLiteral(randomCypherPickNumber(rng)))
	},
	func(rng *rand.Rand) string {
		return fmt.Sprintf(`MATCH (n:%s) WHERE n.val > %s RETURN n`, randomCypherPickKind(rng), cypherNumberLiteral(randomCypherPickNumber(rng)))
	},

	// Single-Part: string predicates.
	func(rng *rand.Rand) string {
		return fmt.Sprintf(`MATCH (n:%s) WHERE n.str STARTS WITH %s RETURN n`, randomCypherPickKind(rng), cypherStringLiteral(randomCypherPickString(rng)))
	},
	func(rng *rand.Rand) string {
		return fmt.Sprintf(`MATCH (n:%s) WHERE n.str ENDS WITH %s RETURN n`, randomCypherPickKind(rng), cypherStringLiteral(randomCypherPickString(rng)))
	},
	func(rng *rand.Rand) string {
		return fmt.Sprintf(`MATCH (n:%s) WHERE n.str CONTAINS %s RETURN n`, randomCypherPickKind(rng), cypherStringLiteral(randomCypherPickString(rng)))
	},
	func(rng *rand.Rand) string {
		return fmt.Sprintf(`MATCH (n:%s) WHERE NOT n.str CONTAINS %s RETURN n`, randomCypherPickKind(rng), cypherStringLiteral(randomCypherPickString(rng)))
	},

	// Single-Part: COALESCE guard, IN lists, IS [NOT] NULL, toLower/toUpper.
	func(rng *rand.Rand) string {
		return fmt.Sprintf(`MATCH (n:%s) WHERE COALESCE(n.flag, false) = %s RETURN n`, randomCypherPickKind(rng), cypherBoolLiteral(randomCypherBoolPool[rng.Intn(len(randomCypherBoolPool))]))
	},
	func(rng *rand.Rand) string {
		lits := []string{randomCypherPickString(rng), randomCypherPickString(rng), randomCypherPickString(rng)}
		return fmt.Sprintf(`MATCH (n:%s) WHERE n.str IN %s RETURN n`, randomCypherPickKind(rng), cypherStringListLiteral(lits))
	},
	func(rng *rand.Rand) string {
		lits := []float64{randomCypherPickNumber(rng), randomCypherPickNumber(rng)}
		return fmt.Sprintf(`MATCH (n:%s) WHERE n.val IN %s RETURN n`, randomCypherPickKind(rng), cypherNumberListLiteral(lits))
	},
	func(rng *rand.Rand) string {
		prop := randomCypherNullableProps[rng.Intn(len(randomCypherNullableProps))]
		return fmt.Sprintf(`MATCH (n:%s) WHERE n.%s IS NULL RETURN n`, randomCypherPickKind(rng), prop)
	},
	func(rng *rand.Rand) string {
		prop := randomCypherNullableProps[rng.Intn(len(randomCypherNullableProps))]
		return fmt.Sprintf(`MATCH (n:%s) WHERE n.%s IS NOT NULL RETURN n`, randomCypherPickKind(rng), prop)
	},
	func(rng *rand.Rand) string {
		lit := randomCypherPickString(rng)
		return fmt.Sprintf(`MATCH (n:%s) WHERE toLower(n.str) = %s RETURN n`, randomCypherPickKind(rng), cypherStringLiteral(strings.ToLower(lit)))
	},
	func(rng *rand.Rand) string {
		lit := randomCypherPickString(rng)
		return fmt.Sprintf(`MATCH (n:%s) WHERE toUpper(n.str) = %s RETURN n`, randomCypherPickKind(rng), cypherStringLiteral(strings.ToUpper(lit)))
	},

	// Single-Part: DISTINCT projection, array membership. Every scalar
	// projection here carries an explicit "AS" alias -- without one, a bare
	// expression like `n.flag` or `id(b)` gets an interpreter-assigned key
	// ("n.flag") but PostgreSQL's own driver names an un-aliased SQL
	// projection column "?column?" (pgx's default for an anonymous
	// expression), and extractLiteralSignatures/assertCorpusResultsMatch
	// (prebuilt_corpus_integration_test.go) compares literals by "Key=Value"
	// together -- so an unaliased scalar RETURN would fail on a pure naming
	// mismatch that has nothing to do with the query's actual answer. This
	// was discovered running this exact suite; every corpus query
	// TestPrebuiltCorpusDifferential already runs happens to RETURN a
	// node/path variable (never reaching the Key-sensitive literal branch at
	// all), so this gap was never hit before.
	func(rng *rand.Rand) string {
		return fmt.Sprintf(`MATCH (n:%s) WHERE n.val > %s RETURN DISTINCT n.flag AS flag`, randomCypherPickKind(rng), cypherNumberLiteral(randomCypherPickNumber(rng)))
	},
	func(rng *rand.Rand) string {
		return fmt.Sprintf(`MATCH (n:%s) WHERE %s IN n.tags RETURN n`, randomCypherPickKind(rng), cypherStringLiteral(randomCypherPickTagCandidate(rng)))
	},

	// Single-Part (still a bare SinglePartQuery, per planStages): two MATCH
	// clauses chaining a *0..2/*1..3 var-length relationship, exercising the
	// fixture's self-loops and parallel multi-kind edges.
	func(rng *rand.Rand) string {
		k1, k2 := randomCypherPickKind(rng), randomCypherPickKind(rng)
		edgeKind := randomCypherEdgeKindNames[rng.Intn(len(randomCypherEdgeKindNames))]
		return fmt.Sprintf(`MATCH (a:%s) WHERE a.val > %s MATCH (a)-[:%s*0..2]->(b:%s) RETURN DISTINCT id(b) AS b_id`,
			k1, cypherNumberLiteral(randomCypherPickNumber(rng)), edgeKind, k2)
	},
	func(rng *rand.Rand) string {
		k1, k2 := randomCypherPickKind(rng), randomCypherPickKind(rng)
		edgeKind := randomCypherEdgeKindNames[rng.Intn(len(randomCypherEdgeKindNames))]
		return fmt.Sprintf(`MATCH (a:%s) WHERE a.str CONTAINS %s MATCH (a)-[:%s*1..3]->(b:%s) RETURN DISTINCT id(b) AS b_id`,
			k1, cypherStringLiteral(randomCypherPickString(rng)), edgeKind, k2)
	},

	// Two-Part (a WITH boundary splits the query into exactly two Parts):
	// count(...) aggregation gated by a WHERE. `=`/`<>` draw from the full
	// pool including negatives, same reasoning as the plain property
	// comparisons above (plan-time reject makes the sign-dependent
	// divergence unreachable regardless).
	func(rng *rand.Rand) string {
		op := []string{"=", "<>", "<", ">"}[rng.Intn(4)]
		lit := randomCypherPickNumber(rng)
		return fmt.Sprintf(`MATCH (n:%s) WHERE n.val %s %s WITH count(n) AS cnt RETURN cnt`,
			randomCypherPickKind(rng), op, cypherNumberLiteral(lit))
	},
	func(rng *rand.Rand) string {
		return fmt.Sprintf(`MATCH (n:%s) WHERE n.str CONTAINS %s WITH count(n) AS cnt RETURN cnt`,
			randomCypherPickKind(rng), cypherStringLiteral(randomCypherPickString(rng)))
	},
}

// randomCypherQuery picks a uniformly random template and renders it.
func randomCypherQuery(rng *rand.Rand) string {
	tmpl := randomCypherTemplates[rng.Intn(len(randomCypherTemplates))]
	return tmpl(rng)
}

// --- Interleaved write-through mutations -----------------------------------
//
// Every randomCypherMutationInterval-th completed query across the whole
// seed x query sweep (a global counter spanning all
// randomCypherDifferentialTotalQueries queries, not restarted per seed),
// TestRandomCypherDifferential performs one small mutation batch through bt
// -- create, update, or delete, chosen uniformly by applyRandomCypherMutation
// -- against this suite's own 40-node adversarial catalog, then continues
// comparing the next generated query exactly as before. Each mutation uses
// one of writethrough_differential_integration_test.go's own already-proven
// write-observer-recognized shapes (tx.CreateNode+CreateRelationshipByIDs,
// its class 6; Nodes().Filter(InIDs(...)).Update(...), its class 10; batch.
// DeleteRelationship by id, its class 7) rather than an untested one, so
// this interleaving carries no risk of tripping fallback by accident -- if
// one unexpectedly did anyway, that is a genuine finding (caught by this
// test's own RebuildCount/fallback pin at the end), not something to route
// around.

const (
	// randomCypherMutationIntervalDefault is how often (in completed
	// queries) a mutation batch runs, absent an override -- small enough
	// (every 5th of this suite's 200 total queries, 40 mutation events) to
	// exercise write-through repeatedly across the sweep without dominating
	// its runtime.
	randomCypherMutationIntervalDefault = 5

	// randomCypherMutationIntervalEnv overrides randomCypherMutationIntervalDefault
	// when set to a non-negative integer; set to "0" to disable the
	// mutation phase entirely -- an escape hatch mirroring this package's
	// only other test-file env knob, graphtest.PGAvailable's own
	// BLOODTRAIL_TEST_PG (which gates whether these suites run at all).
	randomCypherMutationIntervalEnv = "BLOODTRAIL_TEST_RANDOM_MUTATION_INTERVAL"
)

// randomCypherMutationInterval resolves the effective interval:
// randomCypherMutationIntervalEnv's value when it parses as a non-negative
// integer, randomCypherMutationIntervalDefault when the env var is unset or
// empty. An env var that IS set but fails to parse (not an integer, or
// negative) fails the test loudly via t.Fatalf instead of silently falling
// back to the default -- a typo'd override (e.g. "5 " with a trailing
// space, or "-1") would otherwise run with a value the caller never
// intended and never learn why, defeating the whole point of an explicit
// override.
func randomCypherMutationInterval(t *testing.T) int {
	t.Helper()

	v := os.Getenv(randomCypherMutationIntervalEnv)
	if v == "" {
		return randomCypherMutationIntervalDefault
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		t.Fatalf("%s=%q: not an integer: %v", randomCypherMutationIntervalEnv, v, err)
	}
	if n < 0 {
		t.Fatalf("%s=%q: must be >= 0 (0 disables the mutation phase)", randomCypherMutationIntervalEnv, v)
	}
	return n
}

// applyRandomCypherMutation performs one small, seeded-deterministic
// mutation batch through bt against the adversarial fixture's own node/edge
// catalog: a create (tx.CreateNode plus one connecting edge to a random
// existing node -- writethrough_differential_integration_test.go's class
// 6 shape), an update (Nodes().Filter(query.InIDs(...)).Update(...),
// merging fresh str/val values onto one existing node -- its class 10
// shape), or a delete (batch.DeleteRelationship on one randomly chosen
// existing edge, discovered live from oracleDB rather than tracked
// separately -- its class 7 shape), chosen uniformly by mutationSeed's own
// draw. *idsPtr is the running node-id catalog randomCypherPickKind's
// callers implicitly assume is non-empty; a create appends its new id so
// later mutation events can also target it.
//
// mutationSeed seeds the choice and every random draw this call makes, so
// a caller can name exactly this seed in a failure message for a
// reproducible mutation -- though this whole suite is already fully
// deterministic end to end (fixed fixture, fixed per-seed query streams,
// and TestRandomCypherDifferential's own fixed mutationSeed formula), so
// simply rerunning the suite already reproduces any given mutation
// bit-for-bit; the returned summary (and the seed within it) is for a
// human reader's benefit, not because rerunning alone would not suffice.
func applyRandomCypherMutation(t *testing.T, ctx context.Context, bt, oracleDB graph.Database, idsPtr *[]graph.ID, mutationSeed int64) string {
	t.Helper()

	rng := rand.New(rand.NewSource(mutationSeed))

	switch rng.Intn(3) {
	case 0: // create: tx.CreateNode + one connecting edge.
		kind := graph.StringKind(randomCypherPickKind(rng))
		edgeKind := graph.StringKind(randomCypherEdgeKindNames[rng.Intn(len(randomCypherEdgeKindNames))])
		peer := (*idsPtr)[rng.Intn(len(*idsPtr))]
		props := randomCypherNodeProperties(len(*idsPtr))

		var newID graph.ID
		if err := bt.WriteTransaction(ctx, func(tx graph.Transaction) error {
			n, err := tx.CreateNode(props, kind)
			if err != nil {
				return err
			}
			newID = n.ID
			_, err = tx.CreateRelationshipByIDs(newID, peer, edgeKind, graph.NewProperties())
			return err
		}); err != nil {
			t.Fatalf("random differential mutation (seed=%d, create): %v", mutationSeed, err)
		}
		*idsPtr = append(*idsPtr, newID)
		return fmt.Sprintf("mutation seed=%d: created node id=%d kind=%s (+%s edge to node %d)", mutationSeed, newID, kind, edgeKind, peer)

	case 1: // update: Nodes().Filter(InIDs(...)).Update(...).
		id := (*idsPtr)[rng.Intn(len(*idsPtr))]
		newProps := graph.NewProperties().Set("str", randomCypherPickString(rng)).Set("val", randomCypherPickNumber(rng))
		if err := bt.WriteTransaction(ctx, func(tx graph.Transaction) error {
			return tx.Nodes().Filter(query.InIDs(query.NodeID(), id)).Update(newProps)
		}); err != nil {
			t.Fatalf("random differential mutation (seed=%d, update): %v", mutationSeed, err)
		}
		return fmt.Sprintf("mutation seed=%d: updated node id=%d (str/val)", mutationSeed, id)

	default: // delete: batch.DeleteRelationship by id.
		// ORDER BY id(r): without it, this scan's own row order (and hence
		// which up-to-200 edges even appear before the LIMIT cuts it off)
		// is unspecified and free to shift on every call as the sweep's own
		// earlier mutations change the table's physical layout -- silently
		// contradicting this suite's "reproducible by seed alone" doc
		// (TestRandomCypherDifferential's own, and applyRandomCypherMutation's),
		// since rng.Intn(len(edgeIDs)) below would then index a
		// run-varying slice even though the RNG draw itself is fixed by
		// mutationSeed.
		result, err := runCorpusQuery(t, ctx, oracleDB, `MATCH ()-[r]->() RETURN r ORDER BY id(r) LIMIT 200`)
		if err != nil {
			t.Fatalf("random differential mutation (seed=%d, delete): locate candidate edge: %v", mutationSeed, err)
		}
		edgeIDs := extractEdgeIDs(result)
		if len(edgeIDs) == 0 {
			return fmt.Sprintf("mutation seed=%d: delete skipped (no edges left)", mutationSeed)
		}
		target := edgeIDs[rng.Intn(len(edgeIDs))]
		if err := bt.BatchOperation(ctx, func(batch graph.Batch) error {
			return batch.DeleteRelationship(target)
		}); err != nil {
			t.Fatalf("random differential mutation (seed=%d, delete): %v", mutationSeed, err)
		}
		return fmt.Sprintf("mutation seed=%d: deleted relationship id=%d", mutationSeed, target)
	}
}

// --- The suite itself -------------------------------------------------

// randomCypherDifferentialSeeds and randomCypherDifferentialQueriesPerSeed
// mirror internal/engine/random_differential_integration_test.go's own
// original sweep convention exactly: 20 seeds, 10 queries per seed, each query
// its own "seed=N/query=M" subtest so a failure reproduces by rerunning just
// that one subtest.
const (
	randomCypherDifferentialSeeds          = 20
	randomCypherDifferentialQueriesPerSeed = 10
	randomCypherDifferentialTotalQueries   = randomCypherDifferentialSeeds * randomCypherDifferentialQueriesPerSeed
	// randomCypherServedFloor is pinned at 170 out of 200 total queries --
	// just below the 186/200 this suite's fixed fixture/template/seed set
	// deterministically serves as of 2026-09-06 (`go test -tags integration
	// -run TestRandomCypherDifferential -v`, "served 186/200 queries",
	// reproduced identically across repeated runs since both the fixture and
	// every seed's query stream are fully deterministic), so a future
	// regression that makes the interpreter decline far more broadly (a
	// translateGateOK tightening, a Plan regression) fails this floor loudly
	// well before the suite's own per-query correctness assertions would
	// happen to catch it, while leaving headroom for the queries this
	// template set already delegates plus a little slack. This dropped from
	// an earlier 199/200 (floor 180) once the `=`/`<>` templates went back
	// to drawing negative number literals (see the removed
	// randomCypherPickNonNegativeNumber's former doc, preserved above):
	// interpret/plan.go's checkComparison now rejects (delegates) a bare
	// property lookup compared against a negative numeric literal via
	// `=`/`<>` at plan time, so roughly a dozen more of these 200 queries
	// correctly fall through to PostgreSQL rather than serve from the
	// in-memory engine -- fewer served, but zero divergence risk, which is
	// the whole point of this suite. Retune both this constant and its
	// comment together if the template set changes enough to move the
	// observed count.
	randomCypherServedFloor = 170
	// randomCypherNonEmptyFloor is this suite's anti-vacuity floor: at
	// least this many of the 200 queries must return at
	// least one row/literal against the independent pg oracle, mirroring
	// dawgs_corpus_integration_test.go's own totalEngineServed floor idiom.
	// Exists specifically because a template drawing its literal candidates
	// from a value pool disjoint from what the property it compares against
	// actually holds is permanently unsatisfiable (always zero rows)
	// regardless of the RNG draw -- exactly what the `... IN n.tags`
	// template did before randomCypherPickTagCandidate replaced its
	// randomCypherPickString draw -- silently never exercising the
	// true-membership code path at all, with nothing before this floor
	// existed to catch a future regression doing the same to some other
	// template. Pinned at 120, comfortably below the 135/200 (67.5%) this
	// suite's fixed fixture/template/seed set deterministically returns as
	// of 2026-09-06 (`go test -tags integration -run
	// TestRandomCypherDifferential -v`, "returned at least one row/literal
	// against the oracle" -- reproduced identically across repeated runs,
	// same determinism argument as randomCypherServedFloor's own doc):
	// several templates deliberately draw a "definitely absent" candidate
	// one time in four (randomCypherPickString/Number/TagCandidate's own
	// 3-in-4/1-in-4 split), plus var-length/count-aggregation templates
	// whose own match rate is lower still, so 100% (or even the low-90s)
	// was never the right target here -- only a large, sudden drop would
	// indicate a newly-unsatisfiable template.
	randomCypherNonEmptyFloor = 120
)

// TestRandomCypherDifferential is an adversarial random Cypher
// differential suite: a fixed, adversarial 40-node/3-kind property fixture
// (loadRandomCypherFixture) seeded once via the plain pg driver, then
// randomCypherDifferentialSeeds*randomCypherDifferentialQueriesPerSeed
// queries -- one per "seed=N/query=M" subtest, generated by
// randomCypherQuery from a rand.Rand seeded identically to N -- each run
// through both the bloodtrail driver (bt, engine-fresh after one
// bloodtrail.TestingEngine(d).RebuildNow) and a raw pg driver oracle (oracle), via the same
// ops.FetchByQuery-based runCorpusQuery/assertCorpusResultsMatch machinery
// prebuilt_corpus_integration_test.go's own differential suite uses: either
// both sides succeed with equal node-id-set/literal-multiset results, or
// both fail with the identical error string.
//
// Every query also has its bloodtrail-served status recorded (a
// cypherServedMarker log delta around the bt-side call, exactly
// TestPrebuiltCorpusDifferential's own idiom) and tallied; the suite fails
// if the total served count drops below randomCypherServedFloor, proving
// this suite actually exercises the interpreter and not just its delegate-
// to-PostgreSQL fallback path.
func TestRandomCypherDifferential(t *testing.T) {
	dsn := graphtest.PGAvailable(t)

	// Must be installed before dawgs.Open constructs the bloodtrail driver
	// below -- see installLogCapture's own doc (staleness_integration_test.go).
	buf := installLogCapture(t)

	ctx := context.Background()

	// A throwaway pg.Driver purely to obtain a sized/configured
	// *pgxpool.Pool, matching dawgs_corpus_integration_test.go/
	// prebuilt_corpus_integration_test.go's identical precedent.
	_, pool := graphtest.OpenPG(t, dsn)
	cfg := dawgs.Config{ConnectionString: dsn, GraphQueryMemoryLimit: size.Gibibyte, Pool: pool}

	rawBT, err := dawgs.Open(ctx, bloodtrail.DriverName, cfg)
	if err != nil {
		t.Fatalf("open bloodtrail: %v", err)
	}
	defer func() { _ = rawBT.Close(ctx) }()

	d, ok := rawBT.(*bloodtrail.Driver)
	if !ok {
		t.Fatalf("expected *bloodtrail.Driver, got %T", rawBT)
	}

	rawOracle, err := dawgs.Open(ctx, pg.DriverName, cfg)
	if err != nil {
		t.Fatalf("open pg oracle: %v", err)
	}
	defer func() { _ = rawOracle.Close(ctx) }()

	oracle, ok := rawOracle.(*pg.Driver)
	if !ok {
		t.Fatalf("expected *pg.Driver, got %T", rawOracle)
	}

	graphtest.WipeGraph(t, oracle)

	ids := loadRandomCypherFixture(t, d, oracle)
	if len(ids) != randomCypherFixtureNodeCount {
		t.Fatalf("loadRandomCypherFixture returned %d ids, want %d", len(ids), randomCypherFixtureNodeCount)
	}

	if err := bloodtrail.TestingEngine(d).RebuildNow(ctx, "manual"); err != nil {
		t.Fatalf("RebuildNow: %v", err)
	}

	bt := graph.Database(d)
	oracleDB := graph.Database(oracle)

	// Captured right after the one rebuild above, before either the query
	// sweep or its interleaved mutations run: everything from here on --
	// every generated query AND every mutation batch applyRandomCypherMutation
	// performs -- must be served by write-through alone (this suite's own
	// exit criterion for being re-based on write-through).
	rebuildsBeforeSweep := bloodtrail.TestingEngine(d).RebuildCount()
	fallbacksBeforeSweep := markerCount(buf, fallbackEnteredMarker)

	mutationInterval := randomCypherMutationInterval(t)

	var (
		servedCount   int
		nonEmptyCount int
		totalCount    int
		globalIter    int
		mutationTrace []string // every mutation summary so far, in order -- see the failure log below
	)

	for seed := int64(1); seed <= randomCypherDifferentialSeeds; seed++ {
		rng := rand.New(rand.NewSource(seed))

		for q := 0; q < randomCypherDifferentialQueriesPerSeed; q++ {
			text := randomCypherQuery(rng)

			globalIter++
			if mutationInterval > 0 && globalIter%mutationInterval == 0 {
				// A fixed offset well clear of any query-generation seed
				// (1..randomCypherDifferentialSeeds) or query index, so this
				// mutation's own rand.Rand draws never correlate with
				// either.
				mutationTrace = append(mutationTrace, applyRandomCypherMutation(t, ctx, bt, oracleDB, &ids, int64(900000+globalIter)))
			}

			t.Run(fmt.Sprintf("seed=%d/query=%d", seed, q), func(t *testing.T) {
				// globalIter and the FULL mutationTrace so far (not just the
				// most recent entry) -- a divergence caused by mutation N-3
				// interacting with mutation N-1 is only reproducible if the
				// failure log names the whole sequence, not just whichever
				// mutation happened to run most recently before this query.
				iterAtFailure, traceAtFailure := globalIter, append([]string(nil), mutationTrace...)
				defer func() {
					if t.Failed() {
						t.Logf("reproduce with: seed=%d query=%d (global iteration %d)\nquery: %s\nmutation trace so far (%d entries):\n%s",
							seed, q, iterAtFailure, text, len(traceAtFailure), strings.Join(traceAtFailure, "\n"))
					}
				}()

				baseline := markerCount(buf, cypherServedMarker)
				gotResult, gotErr := runCorpusQuery(t, ctx, bt, text)
				served := markerCount(buf, cypherServedMarker) > baseline

				wantResult, wantErr := runCorpusQuery(t, ctx, oracleDB, text)

				switch {
				case gotErr != nil || wantErr != nil:
					if gotErr == nil || wantErr == nil {
						t.Fatalf("error mismatch: bloodtrail err=%v, oracle err=%v (query: %s)", gotErr, wantErr, text)
					}
					if gotErr.Error() != wantErr.Error() {
						t.Fatalf("error string mismatch:\n  bloodtrail: %s\n  oracle:     %s\n(query: %s)", gotErr.Error(), wantErr.Error(), text)
					}
				default:
					// sortPackedPathAllowed is unconditionally true here:
					// every randomCypherTemplates entry returns a bare
					// node/relationship/scalar projection (RETURN n /
					// RETURN DISTINCT n.flag AS flag / RETURN cnt / ...),
					// never a path variable (no template ever binds one via
					// "MATCH p = (...)" or "MATCH p = shortestPath(...)")
					// -- see projectsPathVariable's own doc, prebuilt_
					// corpus_integration_test.go, for why that distinction,
					// not len(Paths), is what actually gates whether
					// sortFlatPathNodesAndEdges may run.
					assertCorpusResultsMatch(t, false, true, gotResult, wantResult)
				}

				totalCount++
				if served {
					servedCount++
				}
				// Anti-vacuity, per query template rather than per whole
				// suite (prebuilt_corpus_integration_test.go's own 60%
				// floor is a suite-wide average): the oracle's own result is
				// the independent authority for "did this random draw
				// actually produce a satisfiable predicate", exactly
				// mirroring dawgs_corpus_integration_test.go's
				// totalEngineServed tally/floor idiom, one level down.
				if len(wantResult.Paths) > 0 || len(wantResult.Literals) > 0 {
					nonEmptyCount++
				}
			})
		}
	}

	t.Logf("served %d/%d queries (%.1f%%) via the in-memory engine; the rest delegated to PostgreSQL",
		servedCount, totalCount, 100*float64(servedCount)/float64(totalCount))
	t.Logf("random Cypher differential: %d/%d queries (%.1f%%) returned at least one row/literal against the oracle",
		nonEmptyCount, totalCount, 100*float64(nonEmptyCount)/float64(totalCount))

	if totalCount != randomCypherDifferentialTotalQueries {
		t.Fatalf("ran %d queries, want %d", totalCount, randomCypherDifferentialTotalQueries)
	}
	if servedCount < randomCypherServedFloor {
		t.Errorf("served only %d/%d queries via the engine, want >= %d (see randomCypherServedFloor's doc) -- a regression may have made the interpreter decline far more broadly than expected", servedCount, totalCount, randomCypherServedFloor)
	}
	if nonEmptyCount < randomCypherNonEmptyFloor {
		t.Errorf("only %d/%d queries returned any rows, want >= %d (see randomCypherNonEmptyFloor's doc) -- a template may have become unsatisfiable (e.g. two disjoint value pools, as previously happened to the `IN n.tags` template)", nonEmptyCount, totalCount, randomCypherNonEmptyFloor)
	}

	// This suite's own exit criterion for being re-based on write-through:
	// every interleaved mutation batch above (mutationInterval permitting)
	// must have been served entirely from write-through, never by way of a
	// rebuild or a fallback -- see applyRandomCypherMutation's own doc for why its three
	// shapes are each already-proven write-observer-recognized ones.
	assertRebuildCountUnchanged(t, d, rebuildsBeforeSweep, "random differential sweep (including interleaved mutations)")
	assertNoNewFallback(t, buf, fallbacksBeforeSweep, "random differential sweep (including interleaved mutations)")
}
