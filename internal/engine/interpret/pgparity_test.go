// SPDX-License-Identifier: Apache-2.0

package interpret

import (
	"testing"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

// TestDeclinesShapesPostgresEvaluatesDifferently pins the query shapes this
// evaluator must never serve because dawgs' SQL for the same AST does not
// mean what Cypher means. Each row was checked against the SQL
// translate.Translate actually emits for it.
//
// Serving any of these answers a question stock BloodHound answers
// differently, or cannot answer at all, which is the one thing the engine
// is not allowed to do.
func TestDeclinesShapesPostgresEvaluatesDifferently(t *testing.T) {
	snap := testSnapshot(t, nil)

	runPlanGolden(t, snap, []planTestCase{
		// Chained comparisons. Cypher reads `a op b op c` as a conjunction;
		// dawgs emits left-associative SQL, so `1 < n.val < 5` becomes
		// `(1 < n.val) < 5`, which PostgreSQL refuses outright.
		{"chained relational comparison", `MATCH (n:User) WHERE 1 < n.val < 5 RETURN n`, false},
		{"chained equality comparison", `MATCH (n:User) WHERE n.val = 1 = true RETURN n`, false},
		{"chained mixed comparison", `MATCH (n:User) WHERE n.val > 0 > -1 RETURN n`, false},
		{"single comparison still served", `MATCH (n:User) WHERE n.val > 0 RETURN n`, true},

		// List literals dawgs renders as a one-type PostgreSQL array, so a
		// NULL, a boolean, or a mix of strings and numbers fails to
		// translate and the delegated query errors instead of answering.
		{"list with a null element", `MATCH (n:User) WHERE n.val IN [1, null] RETURN n`, false},
		{"negated list with a null element", `MATCH (n:User) WHERE NOT n.val IN [1, null] RETURN n`, false},
		{"list mixing strings and numbers", `MATCH (n:User) WHERE n.val IN [1, 'a'] RETURN n`, false},
		{"list of booleans", `MATCH (n:User) WHERE n.val IN [true, false] RETURN n`, false},
		{"projected list with a null element", `MATCH (n:User) RETURN [1, null] AS lst`, false},
		{"homogeneous number list still served", `MATCH (n:User) WHERE n.val IN [1, 2] RETURN n`, true},
		{"homogeneous string list still served", `MATCH (n:User) WHERE n.name IN ['a', 'b'] RETURN n`, true},

		// Division and modulo where dawgs casts the operands to int8, so
		// PostgreSQL truncates and this evaluator would not.
		{"integer literal division", `MATCH (n:User) RETURN 1 / 3 AS x`, false},
		{"property divided by integer", `MATCH (n:User) RETURN n.val / 2 AS x`, false},
		{"property divided by integer in where", `MATCH (n:User) WHERE n.val / 2 = 1 RETURN n`, false},
		{"arithmetic divided by integer", `MATCH (n:User) RETURN (n.val + 1) / 3 AS x`, false},
		{"integer modulo", `MATCH (n:User) RETURN n.val % 5 AS x`, false},
		{"division by a float still served", `MATCH (n:User) RETURN n.val / 2.0 AS x`, true},
		{"division by a negative float still served", `MATCH (n:User) RETURN n.val / -2.0 AS x`, true},
		{"float earlier in the chain still served", `MATCH (n:User) RETURN n.val * 1.5 / 2 AS x`, true},
		{"addition still served", `MATCH (n:User) RETURN n.val + 1 AS x`, true},
	})
}

// TestCartesianProductIsChargedBeforeItIsBuilt pins that an oversized row
// product is refused up front rather than reserved and then metered. The
// reserved slice used to be allocated before the first work unit was spent,
// so a two-component pattern over a real graph asked the allocator for
// hundreds of gigabytes -- a fatal out-of-memory that no recover() can turn
// back into a delegated query.
func TestCartesianProductIsChargedBeforeItIsBuilt(t *testing.T) {
	meter := &workMeter{budget: Budgets{MaxWork: 1000, MaxRows: 1000}}
	if err := meter.spendProduct(100000, 100000); err == nil {
		t.Fatal("a product far beyond the budget must be refused")
	}

	// An empty side is free: there is no product to build.
	empty := &workMeter{budget: Budgets{MaxWork: 1, MaxRows: 1}}
	if err := empty.spendProduct(0, 1<<40); err != nil {
		t.Errorf("an empty side must cost nothing: %v", err)
	}

	// A product that fits is charged in full, so the work it represents is
	// visible to every later check.
	fits := &workMeter{budget: Budgets{MaxWork: 1000, MaxRows: 1000}}
	if err := fits.spendProduct(20, 20); err != nil {
		t.Fatalf("a product inside the budget must be allowed: %v", err)
	}
	if fits.work != 400 {
		t.Errorf("work = %d, want 400", fits.work)
	}

	// Sizes whose int64 product would overflow must still be refused, not
	// wrap into something that looks affordable.
	overflow := &workMeter{budget: Budgets{MaxWork: 1 << 40}}
	if err := overflow.spendProduct(1<<40, 1<<40); err == nil {
		t.Error("an overflowing product must be refused")
	}
}

// TestFixedStepsDoNotReuseOneEdge pins Cypher's relationship-uniqueness rule
// across two fixed steps of one pattern: no two steps may resolve to the same
// relationship. dawgs emits `e1.id != e0.id` for every step past the first,
// so PostgreSQL returns nothing for these shapes over a single edge; the
// engine used to return a row, which is how the co-membership shape
// `(u1:User)-[:MemberOf]->(g)<-[:MemberOf]-(u2:User)` paired every user with
// themselves.
func TestFixedStepsDoNotReuseOneEdge(t *testing.T) {
	// Two nodes, one edge between them.
	b := snapshot.NewBuilder(1)
	if err := b.AddNode(1, []snapshot.KindID{1}, []byte(`{"name":"x"}`)); err != nil {
		t.Fatal(err)
	}
	if err := b.AddNode(2, []snapshot.KindID{1}, []byte(`{"name":"y"}`)); err != nil {
		t.Fatal(err)
	}
	b.AddEdge(1, 1, 2, 2)
	b.SetKinds(map[snapshot.KindID]string{1: "User", 2: "MemberOf"})
	snap, err := b.Build()
	if err != nil {
		t.Fatal(err)
	}
	view := snapshot.NewView(snap)

	for _, query := range []string{
		`MATCH (a:User)-[:MemberOf]->(b:User)<-[:MemberOf]-(c:User) RETURN id(a) AS aid, id(c) AS cid`,
		`MATCH (a:User)-[:MemberOf]-(b:User)-[:MemberOf]-(c:User) RETURN id(a) AS aid, id(c) AS cid`,
	} {
		rows := mustExec(t, view, query, generousBudget).Rows
		if len(rows) != 0 {
			t.Errorf("query returned %d rows, want 0 -- one edge cannot satisfy two steps\nquery: %s", len(rows), query)
		}
	}

	// A genuine two-edge path still matches: the rule forbids reusing one
	// edge, not traversing two.
	b2 := snapshot.NewBuilder(1)
	for i := uint64(1); i <= 3; i++ {
		if err := b2.AddNode(i, []snapshot.KindID{1}, nil); err != nil {
			t.Fatal(err)
		}
	}
	b2.AddEdge(1, 1, 2, 2)
	b2.AddEdge(2, 3, 2, 2)
	b2.SetKinds(map[snapshot.KindID]string{1: "User", 2: "MemberOf"})
	snap2, err := b2.Build()
	if err != nil {
		t.Fatal(err)
	}
	rows := mustExec(t, snapshot.NewView(snap2),
		`MATCH (a:User)-[:MemberOf]->(b:User)<-[:MemberOf]-(c:User) RETURN id(a) AS aid, id(c) AS cid`,
		generousBudget).Rows
	if len(rows) != 2 {
		t.Errorf("two distinct edges into one node returned %d rows, want 2", len(rows))
	}
}
