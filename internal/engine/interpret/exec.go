// SPDX-License-Identifier: Apache-2.0

// exec.go implements the executor: turning one Plan-
// compiled Query into a fully materialized ResultSet by scanning/expanding
// each Part's pattern directly over the snapshot's CSR arrays and property
// bags, joining shared variables by binding identity, filtering by WHERE,
// and projecting the RETURN clause.
//
// This file provides the single-Part machinery every Query shape this
// package serves is built from: pattern matching within one Part
// (matchPart/runComponent, fixed-length Steps here, variable-length/
// shortestPath Steps dispatched out to expand.go), WHERE filtering, and
// RETURN projection. pipeline.go's runQuery is what actually drives
// Execute end to end -- chaining Part[0] and Part[1] across a WITH boundary
// via this file's own cartesianJoin/mergeRowInto row-merge primitives,
// applying WITH's grouping/aggregation, and finishing with RETURN DISTINCT/
// ORDER BY/SKIP/LIMIT -- reusing this file's per-Part machinery unchanged
// rather than duplicating it. Everything neither this file nor pipeline.go
// recognizes is declined via the unexported errUnsupportedStep sentinel
// rather than guessed at -- exactly like Plan's own default-deny posture,
// and for the same reason: a caller that receives errUnsupportedStep (or any
// other error out of Execute) is expected to delegate the whole query to
// PostgreSQL, which is always correct.
//
// projectItem has an explicit OutPath case for a bare path-variable RETURN
// item, reading whatever expand.go's variable-length expansion functions or
// this file's assembleChainPathVal bound via Row.SetPathVar (always a
// *PathVal in this package).

package interpret

import (
	"errors"
	"fmt"
	"sort"

	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/graph"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

// ErrBudget is returned by Execute when a query's row or work budget (see
// Budgets) is exceeded during materialization.
var ErrBudget = errors.New("interpret: budget exceeded")

// errUnsupportedStep marks a query shape this file itself does not handle:
// a var-length or shortestPath/allShortestPaths Step (expand.go's territory),
// or a WITH clause / ORDER BY / SKIP / LIMIT / RETURN DISTINCT (pipeline.go's).
// It is unexported deliberately -- a caller only needs to know Execute
// declined and must delegate the whole query to PostgreSQL, not which part of
// the package might later lift the restriction.
var errUnsupportedStep = errors.New("interpret: unsupported step")

// Budgets caps one Execute call's cost. MaxRows caps the number of rows
// admitted into the final result (checked exactly, the moment a row would
// exceed it); MaxWork caps a generic work-unit counter accumulated across
// the whole materialization (+1 per node visited in a scan, +1 per
// adjacency slot inspected during expansion, +1 per row produced anywhere
// -- component rows, cartesian-joined rows, and final rows alike),
// rechecked every 1024 units for efficiency plus once, unconditionally,
// before Execute returns a successful ResultSet (so a query whose total
// work never crosses a 1024-unit boundary is still correctly bounded). A
// zero or negative field means "unlimited" for that dimension.
type Budgets struct {
	MaxRows int
	MaxWork int64
}

// OutKind discriminates OutVal's payload.
type OutKind uint8

const (
	OutNode OutKind = iota
	OutEdge
	OutPath
	OutScalar
)

// OutVal is one projected RETURN column's value for one result row. Exactly
// one of Node/Edge/Path/Scalar is meaningful, per Kind.
//
// ScalarAbsent (meaningful only when Kind == OutScalar) distinguishes an
// absent property lookup (EvalValue's ok == false; Scalar left at its zero
// value, nil) from a genuinely PRESENT JSON null (EvalValue's (nil, true) --
// PropStore's own "stored null, distinguishable from absence" contract,
// still nil here but ScalarAbsent == false). Both render identically as
// null in the projected VALUE, so this flag exists purely for row-identity
// purposes -- RETURN DISTINCT's dedup key and WITH grouping's group key
// (pipeline.go's outValKey/appendSymbolKey) -- because PostgreSQL itself
// treats them as different jsonb values for exactly those purposes: an
// absent property renders as SQL NULL (jsonb's `->` on a missing key), and
// SQL NULL groups/dedups together with every other SQL NULL; a stored JSON
// null renders as the non-NULL value 'null'::jsonb, which groups/dedups
// only with other 'null'::jsonb values, never with SQL NULL. Collapsing the
// two into one dedup bucket (this package's original behavior, before this
// field existed) under-counts DISTINCT/grouped rows relative to pg whenever
// a query's result mixes both across different rows.
type OutVal struct {
	Kind         OutKind
	Node         snapshot.NodeID
	Edge         EdgeRef
	Path         *PathVal
	Scalar       any
	ScalarAbsent bool
}

// PathVal is a materialized path value: an alternating node/edge sequence,
// Nodes[i] connected to Nodes[i+1] by Edges[i]. Declared here because
// ResultSet/OutVal need the type; this file's assembleChainPathVal
// constructs them for named paths in mixed fixed/variable-length chains, while
// expand.go's expansion functions construct them for standalone
// variable-length and shortest-path patterns. An empty PathVal (both slices nil) is the
// eventual representation of a zero-length `*0..` path, per the design doc.
type PathVal struct {
	Nodes []snapshot.NodeID
	Edges []EdgeRef
}

// ResultSet is Execute's fully materialized output: Keys are the RETURN
// column names in projection order, and each Rows[i] has exactly
// len(Keys) entries, aligned with Keys.
type ResultSet struct {
	Keys []string
	Rows [][]OutVal
}

// --- work accounting ---------------------------------------------------

// workMeter accumulates Budgets.MaxWork's generic work-unit counter and
// Budgets.MaxRows' exact final-row count over the course of one Execute
// call. See Budgets' doc comment for the exact accounting/checking policy.
//
// limitTarget/limitTargetSet thread runQuery's own LIMIT-early-termination
// target (pipeline.go's limitTarget(q)) down to a component executor that
// has no direct access to the Query itself -- expandShortestPathComponent
// (expand.go) receives only (env, meter, part, step), several calls below
// runQuery, so the meter (which already reaches every producer) carries
// this instead of widening that call chain's signatures. runQuery sets both
// fields exactly once, near the top of its own body, but ONLY for a
// single-Part query (see its own comment on that gate: a 2-Part query's
// final LIMIT target counts WITH-stage output rows, an entirely different
// quantity from how many rows Part[0]'s own shortestPath component needs to
// produce, so threading it through for a 2-Part query would be wrong, not
// merely unnecessary). Every other case -- a 2-Part query, and every test
// that builds a workMeter directly without going through runQuery at all --
// leaves limitTargetSet at its zero value (false), which every reader of
// these fields must treat identically to pipeline.go's own limitTarget(q)
// returning -1: "no LIMIT pushdown here, run the unlimited path". Do NOT
// repurpose limitTarget's zero value (0) as that sentinel -- 0 is a value
// the meter must still be able to represent faithfully (a literal
// `LIMIT 0`, which is set, not absent), so only limitTargetSet distinguishes
// "unset" from "set, whatever the target turns out to be". This does NOT
// mean every reader treats limitTarget == 0 the same as a positive target,
// though: traverse.Query.Limit's own contract is "0 => unbounded" (the
// opposite of a cutoff), so expand.go's shortestPathLimit further excludes
// limitTarget == 0 from ever narrowing its own cap, even when
// limitTargetSet is true -- see that function's own doc comment.
type workMeter struct {
	budget         Budgets
	work           int64
	unchecked      int64
	limitTarget    int64
	limitTargetSet bool
	finalRows      int
}

// spend adds units to the work counter, checking it against
// budget.MaxWork every 1024 units (an outstanding balance below that
// threshold is caught by the mandatory final check instead -- see check).
func (m *workMeter) spend(units int64) error {
	m.work += units
	m.unchecked += units
	if m.unchecked < 1024 {
		return nil
	}
	m.unchecked = 0
	return m.check()
}

// spendProduct charges the whole size of a row product up front, so a join
// too large to build is refused before anything is allocated for it. The
// multiplication is done in float64 first: left*right as int64 can itself
// overflow at these sizes (a 3-billion-row product squared does not fit),
// and an overflowed product could wrap to something small enough to look
// affordable.
//
// A product beyond either budget is ErrBudget, exactly as reaching the same
// total one row at a time would be, so the caller delegates to PostgreSQL
// instead of trying to hold the result.
func (m *workMeter) spendProduct(left, right int) error {
	if left == 0 || right == 0 {
		return nil
	}
	product := float64(left) * float64(right)
	if m.budget.MaxWork > 0 && product > float64(m.budget.MaxWork) {
		return ErrBudget
	}
	if m.budget.MaxRows > 0 && product > float64(m.budget.MaxRows) {
		return ErrBudget
	}
	// Within budget, so the exact total is representable and is charged
	// through the ordinary counter.
	return m.spend(int64(left) * int64(right))
}

// check compares the accumulated work total against budget.MaxWork
// unconditionally, regardless of the 1024-unit batching spend applies.
// Execute calls this once, unconditionally, right before returning a
// successful ResultSet, so a query whose total work never reaches 1024 is
// still correctly bounded.
func (m *workMeter) check() error {
	if m.budget.MaxWork > 0 && m.work > m.budget.MaxWork {
		return ErrBudget
	}
	return nil
}

// addFinalRow accounts for one row admitted into the query's final result
// (after WHERE filtering, before projection): it enforces budget.MaxRows
// exactly (no batching -- a row cap is meant to stop enumeration the moment
// it is reached) and spends one generic work unit for it.
func (m *workMeter) addFinalRow() error {
	m.finalRows++
	if m.budget.MaxRows > 0 && m.finalRows > m.budget.MaxRows {
		return ErrBudget
	}
	return m.spend(1)
}

// --- Execute -------------------------------------------------------------

// Execute runs q (a Plan-compiled Query) against env's snapshot, fully
// materializing every result row before returning -- per Query's own
// documented execution invariant, any evaluator error anywhere during
// materialization aborts the whole call. b bounds the work Execute is
// willing to spend; see Budgets.
//
// The actual multi-part/WITH/DISTINCT/ORDER BY/SKIP/LIMIT pipeline lives in
// pipeline.go's runQuery; this function is left as the package's
// stable public entry point plus its own nil-defensiveness.
func Execute(env *Env, q *Query, b Budgets) (*ResultSet, error) {
	if env == nil || env.Snap == nil || q == nil {
		return nil, errUnsupportedStep
	}
	return runQuery(env, q, &workMeter{budget: b})
}

// --- pattern matching: components and joins -------------------------------

// matchPart matches every pattern in part against env's snapshot, joining
// disjoint pattern components by cartesian product and connected ones by
// shared node-symbol identity (see groupComponents), and returns every
// fully-bound row -- WHERE is not applied here; Execute does that once over
// the complete row set.
//
// A part with no node patterns at all -- part.Nodes is empty -- is a
// legitimate, Plan-accepted shape: a whole query with no MATCH clause at all
// (a bare `RETURN <expr>`), or, in a 2-Part (one-WITH-boundary) query,
// Part[0] itself being nothing but a leading `WITH <expr> AS x` with no
// MATCH before it (planPart returns Part{Nodes: map[string]*NodeConstraint{}}
// -- empty, not nil -- for an empty ReadingClauses list; see its own doc).
// Either way, the query has exactly one solution at this point: the empty
// binding, satisfied trivially with no constraints to check -- mirroring
// pg's own "a WITH/RETURN with nothing to match still runs its expressions
// exactly once, not zero times" semantics, and exactly the same treatment
// runCarriedPart (pipeline.go) already gives Part[1] under the identical
// condition. Falling through to groupComponents below would instead find
// zero components (groupComponents iterates part.Nodes' keys, of which
// there are none) and this function would return zero rows -- silently
// discarding the query's one and only solution instead of serving it, e.g.
// dropping every row of `WITH 365 AS max_days MATCH (n:User) WHERE
// n.x < max_days RETURN n` before Part[1] (the MATCH) ever runs.
func matchPart(env *Env, part *Part, meter *workMeter) ([]*Row, error) {
	if len(part.Nodes) == 0 {
		return []*Row{NewRow()}, nil
	}

	var merged []*Row
	first := true
	for _, comp := range groupComponents(part) {
		rows, err := runComponent(env, meter, part, comp.syms, comp.stepIdxs)
		if err != nil {
			return nil, err
		}
		if first {
			merged, first = rows, false
			continue
		}
		merged, err = cartesianJoin(meter, merged, rows)
		if err != nil {
			return nil, err
		}
	}
	return merged, nil
}

// component is one connected piece of a Part's pattern graph: every node
// symbol union-find-linked by a chain of Steps (or, for an isolated node
// pattern, a single symbol with no Steps at all), plus the indices into
// Part.Chains that belong to it.
type component struct {
	syms     []string
	stepIdxs []int
}

// groupComponents partitions part's node symbols (and the Steps touching
// them) into connected components via union-find over Part.Chains' FromSym/
// ToSym edges. Two symbols end up in the same component exactly when a
// chain of Steps connects them, directly implementing "shared variables
// join by binding equality" (same component => joined during the walk) vs.
// "cartesian product" (disjoint components => matchPart's plain merge).
// Both the returned component order and each component's syms are sorted,
// so the whole pipeline (anchor choice included) is deterministic.
func groupComponents(part *Part) []component {
	parent := make(map[string]string, len(part.Nodes))
	var find func(string) string
	find = func(s string) string {
		if parent[s] != s {
			parent[s] = find(parent[s])
		}
		return parent[s]
	}
	union := func(a, b string) {
		ra, rb := find(a), find(b)
		if ra != rb {
			parent[ra] = rb
		}
	}

	syms := make([]string, 0, len(part.Nodes))
	for s := range part.Nodes {
		parent[s] = s
		syms = append(syms, s)
	}
	sort.Strings(syms)

	for i := range part.Chains {
		st := &part.Chains[i]
		union(st.FromSym, st.ToSym)
	}

	byRoot := map[string]*component{}
	var order []string
	for _, s := range syms {
		root := find(s)
		c, ok := byRoot[root]
		if !ok {
			c = &component{}
			byRoot[root] = c
			order = append(order, root)
		}
		c.syms = append(c.syms, s)
	}
	for i := range part.Chains {
		root := find(part.Chains[i].FromSym)
		byRoot[root].stepIdxs = append(byRoot[root].stepIdxs, i)
	}

	out := make([]component, 0, len(order))
	for _, root := range order {
		out = append(out, *byRoot[root])
	}
	return out
}

// cartesianJoin merges every row of left with every row of right into a
// fresh row carrying both sides' bindings. Callers only ever combine rows
// from two different components (see groupComponents), which by
// construction never bind the same symbol, so this is a plain merge with no
// equality check to perform.
//
// The product is charged to the meter BEFORE any of it is built, and the
// output slice is grown by append rather than reserved up front. Both
// matter: a reserved len(left)*len(right) is one allocation that no budget
// has approved yet, and on a multi-component pattern over a real graph it
// is enormous -- two disjoint `(a:User), (b:User)` components over 300k
// users reserve 300k x 300k x 8 bytes, 720 GB. That is under the size at
// which Go panics on a slice length, so it arrives as `fatal error:
// runtime: out of memory`, which no recover() can catch: the API process
// dies where declining would have handed the query to PostgreSQL.
func cartesianJoin(meter *workMeter, left, right []*Row) ([]*Row, error) {
	if err := meter.spendProduct(len(left), len(right)); err != nil {
		return nil, err
	}
	var out []*Row
	for _, l := range left {
		for _, r := range right {
			nr := cloneRow(l)
			mergeRowInto(nr, r)
			if err := meter.spend(1); err != nil {
				return nil, err
			}
			out = append(out, nr)
		}
	}
	return out, nil
}

// cloneRow returns a shallow copy of r's bindings on a fresh Row. Reaches
// into Row's unexported maps directly (same package) rather than adding a
// new exported Row method purely for this internal need.
func cloneRow(r *Row) *Row {
	nr := NewRow()
	mergeRowInto(nr, r)
	return nr
}

// mergeRowInto copies every binding from src into dst, usedEdges (see Row's
// own doc) included: two cartesian-joined or WITH-carried rows' used-edge
// sets union together, so a closing Step evaluated after the merge still
// sees every edge either side already consumed.
func mergeRowInto(dst, src *Row) {
	for k, v := range src.nodes {
		dst.SetNode(k, v)
	}
	for k, v := range src.edges {
		dst.SetEdge(k, v)
	}
	for k, v := range src.scalars {
		dst.SetScalar(k, v)
	}
	for k, v := range src.paths {
		dst.SetPathVar(k, v)
	}
	for _, fwd := range src.usedEdges {
		dst.markEdgeUsed(fwd)
	}
	for _, identity := range src.trailEdges {
		dst.markTrailEdge(identity)
	}
}

// --- one component: anchor scan + BFS expansion + closing-edge checks ----

// runComponent matches one connected pattern component: it scans the
// component's most-anchored symbol (chooseAnchor), then walks the
// component's Steps breadth-first from that symbol, expanding a fresh
// binding across each first-visited ("tree") Step and deferring any Step
// whose both endpoints are already bound by the time it is reached (a
// "closing" Step -- a cycle in the pattern's own symbol graph, e.g.
// `(a)-->(b), (b)-->(a)` reusing "a"/"b", or a self-loop pattern
// `(a)-->(a)`) to a verification pass once the whole tree has been walked.
//
// Dispatch, in order:
//
//  1. Every Step in the component must agree on PathSym (uniformPathSym) --
//     a mix of a named-path Step with an unnamed one (or two differently
//     named ones) can only arise from a comma-joined pattern part sharing a
//     symbol with a named `MATCH p = ...` part, a shape no required corpus
//     query needs; declined outright.
//  2. A component containing a shortestPath/allShortestPaths Step
//     (Shortest != ShortestNone) must consist of exactly that one Step --
//     see plan.go's shortestStepsAreIsolated, which also rejects this
//     shape at plan time -- and dispatches to
//     expandShortestPathComponent, which resolves both endpoints as
//     complete, pre-adjacency node sets and has no way to honor a further
//     chain hanging off either one.
//  3. A component consisting of exactly one variable-length Step (Range !=
//     nil) dispatches to expandVarLengthComponent.
//  4. A component with two or more Steps, at least one variable-length, no
//     shortestPath: the mixed fixed/var-length chain shape (e.g.
//     `(c:Computer)-[:HasSession]->(u:User)-[:MemberOf*1..]->(g:Group)`).
//     Dispatches to expandChainComponent, which requires the component to
//     be a strict left-to-right chain (isStrictLinearChain) -- declining
//     anything else (a cycle, a "star" from one bound symbol) rather than
//     guess, since a variable-length Step can only ever be expanded forward
//     from its own FromSym (Direction is always Outbound for Range != nil).
//  5. Otherwise: every Step is fixed-length. A named path (PathSym != "")
//     also requires isStrictLinearChain and dispatches to
//     expandChainComponent (so RETURN p's PathVal assembly logic lives in
//     one place); an unnamed pure-fixed component falls through to this
//     function's own general BFS/closing-edge walk below, exactly as
//     before named-path chain support was added -- zero behavior change
//     for every pre-existing shape.
func runComponent(env *Env, meter *workMeter, part *Part, syms []string, stepIdxs []int) ([]*Row, error) {
	pathSym, pathUniform := uniformPathSym(part, stepIdxs)
	if !pathUniform {
		return nil, errUnsupportedStep
	}

	if hasSpecialStep(part, stepIdxs) {
		if hasShortestStep(part, stepIdxs) {
			if len(stepIdxs) != 1 {
				return nil, errUnsupportedStep
			}
			return expandShortestPathComponent(env, meter, part, &part.Chains[stepIdxs[0]])
		}
		if len(stepIdxs) == 1 {
			return expandVarLengthComponent(env, meter, part, &part.Chains[stepIdxs[0]])
		}
		if !isStrictLinearChain(part, stepIdxs) {
			return nil, errUnsupportedStep
		}
		return expandChainComponent(env, meter, part, stepIdxs, pathSym)
	}

	if pathSym != "" {
		if !isStrictLinearChain(part, stepIdxs) {
			return nil, errUnsupportedStep
		}
		return expandChainComponent(env, meter, part, stepIdxs, pathSym)
	}

	anchor := chooseAnchor(env, part.Nodes, syms)
	rows, err := scanAnchor(env, meter, anchor, part.Nodes[anchor])
	if err != nil {
		return nil, err
	}
	return runComponentTreeFrom(env, meter, part, stepIdxs, anchor, rows)
}

// runComponentTreeFrom is runComponent's own tree-walk/closing-edge
// expansion (see its doc comment above) factored out so it can run over an
// anchorRows chunk a caller already collected for anchor -- via scanAnchor
// (runComponent's own use, below) or, for a future chunked LIMIT driver, via
// scanAnchorVisit stopped early with errStopScan -- instead of always
// starting a scan of its own. anchor must be the same symbol anchorRows is
// keyed on (chooseAnchor's pick, when the caller is runComponent itself);
// stepIdxs is the component's Steps, unfiltered -- runComponentTreeFrom
// rediscovers which of them are "tree" vs. "closing" itself via the same
// BFS runComponent always ran.
func runComponentTreeFrom(env *Env, meter *workMeter, part *Part, stepIdxs []int, anchor string, anchorRows []*Row) ([]*Row, error) {
	rows := anchorRows
	if len(stepIdxs) == 0 {
		return rows, nil
	}

	adjBySym := map[string][]int{}
	for _, idx := range stepIdxs {
		st := &part.Chains[idx]
		adjBySym[st.FromSym] = append(adjBySym[st.FromSym], idx)
		adjBySym[st.ToSym] = append(adjBySym[st.ToSym], idx)
	}

	visited := map[string]bool{anchor: true}
	queue := []string{anchor}
	processed := make(map[int]bool, len(stepIdxs))
	var closing []int
	var err error

	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]

		for _, idx := range adjBySym[cur] {
			if processed[idx] {
				continue
			}
			processed[idx] = true
			st := &part.Chains[idx]

			other, boundIsFrom := st.ToSym, true
			if cur == st.ToSym && cur != st.FromSym {
				other, boundIsFrom = st.FromSym, false
			}

			if visited[other] {
				closing = append(closing, idx)
				continue
			}
			visited[other] = true

			boundSym, unboundSym := st.FromSym, st.ToSym
			if !boundIsFrom {
				boundSym, unboundSym = st.ToSym, st.FromSym
			}

			rows, err = expandStep(env, meter, rows, st, boundSym, unboundSym, boundIsFrom, part.Nodes[unboundSym], "")
			if err != nil {
				return nil, err
			}
			queue = append(queue, other)
		}
	}

	for _, idx := range closing {
		rows, err = verifyClosingStep(env, meter, rows, &part.Chains[idx])
		if err != nil {
			return nil, err
		}
	}

	return rows, nil
}

// runComponentFrom replays runComponent's own dispatch (see its doc comment
// above -- every numbered branch there except #2) over anchorRows, a chunk
// of comp's own anchor rows a caller already collected (via scanAnchor or,
// for a future chunked LIMIT driver, scanAnchorVisit), instead of running a
// scanAnchor of its own: the single entry point that driver needs per
// chunk, covering all three non-shortestPath component executors
// (fixed-length/BFS, chain, var-length) behind one call.
//
// Branch #2 (a shortestPath/allShortestPaths component) is deliberately
// NOT reproduced here: expandShortestPathComponent resolves both pattern
// endpoints as complete node sets up front (see its own doc) rather than
// growing rows from one symbol's anchor scan, so it has no "given anchor
// rows" tail to dispatch to at all -- callers must route that shape through
// runComponent/expandShortestPathComponent directly, exactly as matchPart
// already does.
func runComponentFrom(env *Env, meter *workMeter, part *Part, comp component, anchorRows []*Row) ([]*Row, error) {
	stepIdxs := comp.stepIdxs
	pathSym, pathUniform := uniformPathSym(part, stepIdxs)
	if !pathUniform {
		return nil, errUnsupportedStep
	}

	if hasSpecialStep(part, stepIdxs) {
		if hasShortestStep(part, stepIdxs) {
			return nil, errUnsupportedStep
		}
		if len(stepIdxs) == 1 {
			return expandVarLengthComponentFrom(env, meter, part, &part.Chains[stepIdxs[0]], anchorRows)
		}
		if !isStrictLinearChain(part, stepIdxs) {
			return nil, errUnsupportedStep
		}
		return expandChainComponentFrom(env, meter, part, stepIdxs, pathSym, anchorRows)
	}

	if pathSym != "" {
		if !isStrictLinearChain(part, stepIdxs) {
			return nil, errUnsupportedStep
		}
		return expandChainComponentFrom(env, meter, part, stepIdxs, pathSym, anchorRows)
	}

	anchor := chooseAnchor(env, part.Nodes, comp.syms)
	return runComponentTreeFrom(env, meter, part, stepIdxs, anchor, anchorRows)
}

// hasSpecialStep reports whether any of part.Chains[stepIdxs] is a
// variable-length or shortestPath/allShortestPaths Step -- the condition on
// which runComponent dispatches out to expand.go instead of walking the
// component itself.
func hasSpecialStep(part *Part, stepIdxs []int) bool {
	for _, idx := range stepIdxs {
		st := &part.Chains[idx]
		if st.Range != nil || st.Shortest != ShortestNone {
			return true
		}
	}
	return false
}

// hasShortestStep reports whether any of part.Chains[stepIdxs] is a
// shortestPath/allShortestPaths Step.
func hasShortestStep(part *Part, stepIdxs []int) bool {
	for _, idx := range stepIdxs {
		if part.Chains[idx].Shortest != ShortestNone {
			return true
		}
	}
	return false
}

// uniformPathSym reports the single PathSym value every Step in stepIdxs
// agrees on (possibly "", meaning none of them are part of a named path),
// and false if they disagree (a mix of "" and a name, or two different
// names) -- a shape that can only arise from a comma-joined pattern part
// sharing a symbol with a named `MATCH p = ...` part, which addPatternPart
// never produces on its own (PathSym is set uniformly across every Step of
// one named PatternPart). An empty stepIdxs (an isolated node component)
// trivially agrees on "".
func uniformPathSym(part *Part, stepIdxs []int) (string, bool) {
	if len(stepIdxs) == 0 {
		return "", true
	}
	sym := part.Chains[stepIdxs[0]].PathSym
	for _, idx := range stepIdxs[1:] {
		if part.Chains[idx].PathSym != sym {
			return "", false
		}
	}
	return sym, true
}

// isStrictLinearChain reports whether stepIdxs (already in ascending
// Part.Chains order -- ie. pattern order, see groupComponents' doc) forms a
// SIMPLE, LEFT-TO-RIGHT chain with no branching and no repeated symbol: the
// component's own seed symbol is part.Chains[stepIdxs[0]].FromSym, and every
// step's own ToSym must be a symbol never seen before (ruling out both a
// same-step self-loop and a cycle back to an earlier symbol), while every
// step after the first must start exactly where the previous one ended
// (part.Chains[stepIdxs[i-1]].ToSym == part.Chains[stepIdxs[i]].FromSym).
//
// This is deliberately NOT a general "is this pattern graph a simple path"
// test up to reordering/reorientation: buildStep normalizes an inbound
// arrow (`<-`) by swapping FromSym/ToSym so the recorded Step direction
// always matches the *traversal* direction, which means a chain written
// with a backward arrow (`(a)<-[:X]-(b)-->(c)`) does NOT satisfy
// step[i].ToSym == step[i+1].FromSym even though it is, structurally, still
// a linear a-b-c pattern -- both of that shape's Steps end up with FromSym
// "b". expandChainComponent's own anchoring (always scan
// stepIdxs[0].FromSym, then walk strictly forward) has no way to compose a
// variable-length Step's forward-only expansion with such a shape anyway
// (see expandChainComponent's own doc), so this check declines it outright
// rather than attempt a general chain-reordering algorithm no required
// corpus shape (every migrated BloodHound pattern chain is written with
// only forward arrows) needs.
func isStrictLinearChain(part *Part, stepIdxs []int) bool {
	if len(stepIdxs) == 0 {
		return true
	}
	seen := map[string]bool{part.Chains[stepIdxs[0]].FromSym: true}
	for i, idx := range stepIdxs {
		step := &part.Chains[idx]
		if step.FromSym == step.ToSym {
			return false
		}
		if i > 0 && part.Chains[stepIdxs[i-1]].ToSym != step.FromSym {
			return false
		}
		if seen[step.ToSym] {
			return false
		}
		seen[step.ToSym] = true
	}
	return len(seen) == len(stepIdxs)+1
}

// pathStepArcKey returns the internal-only Row binding key
// expandChainComponent/expandStep/expandVarLengthTrailsForSeed use to
// recover step index idx's own specific contribution to a named path once
// every step of the chain has bound its own piece (see
// assembleChainPathVal): a single EdgeRef for a fixed step (used only when
// that step's own relationship is anonymous -- an anonymous relationship
// binds no EdgeSym at all, so there would otherwise be no way to recover
// exactly which of possibly several parallel edges produced this particular
// row), or a whole per-step trail *PathVal for a variable-length step (a
// var-length step's OWN PathSym, when it has one, names the path for the
// WHOLE chain, not this one step's own segment, so a separate key is needed
// regardless of whether the step is named). "$" can never collide with a
// real Cypher identifier -- see partBuilder.symbolFor's identical reasoning
// at plan time.
func pathStepArcKey(stepIdx int) string {
	return "$patharc" + itoa(stepIdx)
}

// expandChainComponent executes a component of one or more Steps -- none
// shortestPath -- that isStrictLinearChain has already confirmed forms a
// simple left-to-right chain: runComponent's dispatch target for
// both a mixed fixed/var-length multi-step chain (e.g.
// `(c:Computer)-[:HasSession]->(u:User)-[:MemberOf*1..]->(g:Group)`) and,
// when pathSym != "", a chain that needs its whole traversal-order PathVal
// assembled for a `RETURN p` projection (see assembleChainPathVal) --
// including a PURE fixed-length named-path chain, so path assembly lives in
// exactly one place rather than being duplicated into runComponent's
// general BFS/closing-edge walk below.
//
// Anchoring: this deliberately takes the simplest correct approach, and does
// NOT run runComponent's general
// BFS-from-cost-optimal-anchor algorithm. A variable-length Step can only
// ever be expanded forward from its own FromSym (buildStep: Direction is
// always Outbound for Range != nil; expandVarLengthTrailsForSeed/adjacency
// always walk boundIsFrom == true), so composing it with an
// arbitrary-direction BFS would be unneeded complexity no required shape
// needs. Instead, this function always scans stepIdxs[0]'s own FromSym --
// the chain's own leftmost symbol -- as the sole anchor (via scanAnchor, so
// the existing id/objectid/kind/full-scan cost-tiered heuristic still
// applies to *how* that one symbol is scanned, just not to *which* symbol is
// chosen), then walks every step of the chain, in order, growing every row
// by exactly one step at a time: a fixed step via expandStep, a
// variable-length step via expandVarLengthTrailsForSeed (expand.go, factored
// out of expandVarLengthComponent so both dispatch paths share the identical
// trail-DFS semantics). Trail-edge uniqueness within one trail stays scoped
// to each variable-length step's own call (a fresh DFS per step, per row) --
// distinct variable-length steps of the same chain may legally reuse the
// same physical edge in their own trails, mirroring dawgs' own per-expansion
// recursive CTE -- while a FIXED step's edge and a trail's edges mutually
// exclude each other across the whole chain, in both orders (see expand.go's
// package doc's CROSS-STEP bullet and Row.trailEdges' doc).
func expandChainComponent(env *Env, meter *workMeter, part *Part, stepIdxs []int, pathSym string) ([]*Row, error) {
	startSym := part.Chains[stepIdxs[0]].FromSym
	rows, err := scanAnchor(env, meter, startSym, part.Nodes[startSym])
	if err != nil {
		return nil, err
	}
	return expandChainComponentFrom(env, meter, part, stepIdxs, pathSym, rows)
}

// expandChainComponentFrom is expandChainComponent's own step-by-step
// expansion (see its doc comment above), factored out so it can grow an
// anchorRows chunk a caller already collected for the chain's own leftmost
// symbol (part.Chains[stepIdxs[0]].FromSym) instead of always starting a
// scanAnchor of its own. Each step still runs through the same fixed
// (expandStep) or variable-length (expandVarLengthTrailsForSeed) expansion
// in chain order, and pathSym's PathVal assembly (assembleChainPathVal)
// still runs last, over the fully-grown rows -- unchanged by this split.
func expandChainComponentFrom(env *Env, meter *workMeter, part *Part, stepIdxs []int, pathSym string, anchorRows []*Row) ([]*Row, error) {
	rows := anchorRows
	var err error

	for _, idx := range stepIdxs {
		step := &part.Chains[idx]
		toNC := part.Nodes[step.ToSym]
		arcKey := ""
		if pathSym != "" {
			arcKey = pathStepArcKey(idx)
		}

		if step.Range == nil {
			rows, err = expandStep(env, meter, rows, step, step.FromSym, step.ToSym, true, toNC, arcKey)
			if err != nil {
				return nil, err
			}
			continue
		}

		if step.EdgeSym != "" {
			return nil, errUnsupportedStep
		}

		next := make([]*Row, 0, len(rows))
		for _, r := range rows {
			grown, err := expandVarLengthTrailsForSeed(env, meter, step, toNC, r, arcKey)
			if err != nil {
				return nil, err
			}
			next = append(next, grown...)
		}
		rows = next
	}

	if pathSym != "" {
		for _, r := range rows {
			pv, err := assembleChainPathVal(r, part, stepIdxs)
			if err != nil {
				return nil, err
			}
			r.SetPathVar(pathSym, pv)
		}
	}

	return rows, nil
}

// assembleChainPathVal builds one fully matched row's whole named-path
// PathVal from stepIdxs (already confirmed by isStrictLinearChain to be a
// simple left-to-right chain, in pattern order): the chain's own leftmost
// node (part.Chains[stepIdxs[0]].FromSym) seeds Nodes[0], and each
// subsequent step's own segment -- a fixed step's single edge (its EdgeSym
// binding if the relationship is named, else the internal pathStepArcKey
// binding expandStep left for exactly this purpose), or a variable-length
// step's whole per-row trail (bound under pathStepArcKey by
// expandVarLengthTrailsForSeed) -- appends only its own NEW node(s) (the
// node it starts from is already the path's current last node) plus its own
// edge(s), in order. A zero-length `*0..` segment's trail carries no nodes
// at all (PathVal's own doc: both slices nil for the zero-length case), so
// it appends nothing -- "the path skips the step", exactly like the two
// endpoints it merged were the same node all along.
//
// The loop below always walks stepIdxs in TRAVERSAL order (each step's own
// FromSym/ToSym, which buildStep normalizes to match graph traversal
// direction, not necessarily the order the pattern was WRITTEN in -- see
// Step.Reversed's own doc). For a plain forward chain those two orders
// coincide and the loop's output is already correct. A single-step chain
// whose one step is Reversed (`MATCH p = (t)<-[:R]-(s) RETURN p`, a
// one-step chain that trivially satisfies isStrictLinearChain -- see its
// own doc) is the one shape where they diverge: the loop below would
// otherwise emit [s, t] (traversal order) where Cypher's (and pg's) own
// "node sequence follows the pattern as written" semantics require [t, s].
// This is corrected below by reusing reversePathVal (expand.go), exactly
// the same fix expandVarLengthTrailsForSeed/expandShortestPathComponent
// already apply for a standalone Reversed step.
//
// A Reversed step at any OTHER position -- i.e. a multi-step chain
// (len(stepIdxs) > 1) carrying a Reversed step anywhere in it -- can never
// actually reach here: isStrictLinearChain's own continuity test
// (stepIdxs[i-1].ToSym == stepIdxs[i].FromSym) requires a Reversed step's
// FromSym (always that step's own freshly-introduced pattern variable, per
// Step.Reversed's doc) to equal the previous step's ToSym, which cannot
// happen for a genuinely continuing chain -- so no required corpus shape,
// nor any shape isStrictLinearChain actually admits, needs multi-step
// splicing to account for per-step reversal at all. The guard below makes
// that structural fact an explicit, checked precondition instead of an
// emergent property of isStrictLinearChain's own logic (which could
// silently stop holding under a future change to it): without this guard, a
// var-length Reversed step's own per-step trail -- already flipped into
// pattern-written order by expandVarLengthTrailsForSeed's own
// step.Reversed handling before this function ever reads it back out via
// pathStepArcKey -- would be spliced in assuming TRAVERSAL order (this
// function's segPV.Nodes[1:]/segPV.Edges append below), silently corrupting
// the assembled path rather than failing loudly.
func assembleChainPathVal(r *Row, part *Part, stepIdxs []int) (*PathVal, error) {
	if len(stepIdxs) > 1 {
		for _, idx := range stepIdxs {
			if part.Chains[idx].Reversed {
				return nil, errUnsupportedStep
			}
		}
	}

	startSym := part.Chains[stepIdxs[0]].FromSym
	startID, ok := r.Node(startSym)
	if !ok {
		return nil, fmt.Errorf("interpret: assembleChainPathVal: symbol %q not bound", startSym)
	}
	pv := &PathVal{Nodes: []snapshot.NodeID{startID}}

	for _, idx := range stepIdxs {
		step := &part.Chains[idx]

		if step.Range == nil {
			toID, ok := r.Node(step.ToSym)
			if !ok {
				return nil, fmt.Errorf("interpret: assembleChainPathVal: symbol %q not bound", step.ToSym)
			}
			edgeKey := step.EdgeSym
			if edgeKey == "" {
				edgeKey = pathStepArcKey(idx)
			}
			edge, ok := r.Edge(edgeKey)
			if !ok {
				return nil, fmt.Errorf("interpret: assembleChainPathVal: step %d edge %q not bound", idx, edgeKey)
			}
			pv.Nodes = append(pv.Nodes, toID)
			pv.Edges = append(pv.Edges, edge)
			continue
		}

		seg, ok := r.PathVar(pathStepArcKey(idx))
		if !ok {
			return nil, fmt.Errorf("interpret: assembleChainPathVal: step %d trail not bound", idx)
		}
		segPV, ok := seg.(*PathVal)
		if !ok || segPV == nil {
			return nil, fmt.Errorf("interpret: assembleChainPathVal: step %d trail has unexpected type %T", idx, seg)
		}
		if len(segPV.Nodes) == 0 {
			continue
		}
		pv.Nodes = append(pv.Nodes, segPV.Nodes[1:]...)
		pv.Edges = append(pv.Edges, segPV.Edges...)
	}

	if len(stepIdxs) == 1 && part.Chains[stepIdxs[0]].Reversed {
		// The one-step-chain, Reversed==true case this function's own doc
		// comment above flags: pv was just built in traversal order (this
		// step's FromSym first); flip it to pattern-written order, exactly
		// like expandVarLengthTrailsForSeed/expandShortestPathComponent
		// already do for a standalone Reversed step.
		reversePathVal(pv)
	}

	return pv, nil
}

// --- anchor selection ------------------------------------------------------

// anchorTier ranks the four anchoring strategies, best (lowest) first: id
// anchor, objectid anchor, smallest kind bitmap, full scan. Correctness
// never depends on this order -- see the package doc -- only cost does.
type anchorTier int

const (
	tierID anchorTier = iota
	tierObjectID
	tierKind
	tierScan
)

// anchorRank is one symbol's anchoring cost estimate: tier first, then
// (within tierKind only) the smallest candidate-set size, smaller better.
type anchorRank struct {
	tier anchorTier
	size int
}

// better reports whether r is a strictly cheaper anchor than o.
func (r anchorRank) better(o anchorRank) bool {
	if r.tier != o.tier {
		return r.tier < o.tier
	}
	return r.size < o.size
}

// rankOf computes sym's anchorRank from its NodeConstraint, per the fixed
// heuristic: an id() anchor (at most one distinct value survives Plan, see
// NodeConstraint's doc) beats an objectid anchor beats the smallest single
// kind bitmap among every AND-ed kind label beats an unconstrained full
// scan.
func rankOf(env *Env, nc *NodeConstraint) anchorRank {
	if nc != nil && len(nc.IDs) > 0 {
		return anchorRank{tier: tierID}
	}
	if nc != nil && nc.ObjectIDAnchor != nil {
		return anchorRank{tier: tierObjectID}
	}
	if nc != nil && len(nc.Kinds) > 0 {
		return anchorRank{tier: tierKind, size: smallestKindBitmap(env, nc.Kinds).Count()}
	}
	return anchorRank{tier: tierScan, size: env.Snap.NodeCount()}
}

// chooseAnchor picks the best (chooseAnchor.better) ranked symbol among
// syms, which the caller supplies already sorted (see groupComponents) so
// that a tie breaks toward the lexicographically first symbol name
// deterministically.
func chooseAnchor(env *Env, nodes map[string]*NodeConstraint, syms []string) string {
	best := syms[0]
	bestRank := rankOf(env, nodes[best])
	for _, s := range syms[1:] {
		if r := rankOf(env, nodes[s]); r.better(bestRank) {
			best, bestRank = s, r
		}
	}
	return best
}

// smallestKindBitmap returns the smallest-cardinality kind bitmap among
// kinds (a node symbol's Kinds list is AND-ed together -- see
// NodeConstraint's doc -- so scanning whichever named kind has the fewest
// members, then verifying the full AND-list per candidate, is the standard
// smallest-set-first intersection strategy).
func smallestKindBitmap(env *Env, kinds []snapshot.KindID) *snapshot.Bitset {
	best := env.Snap.NodesOfKind(kinds[0])
	for _, k := range kinds[1:] {
		if bm := env.Snap.NodesOfKind(k); bm.Count() < best.Count() {
			best = bm
		}
	}
	return best
}

// errStopScan tells scanAnchorVisit to stop producing anchor rows. It is
// a control-flow sentinel, not a failure: scanAnchorVisit returns it
// unchanged so callers can distinguish a deliberate stop from an error.
var errStopScan = errors.New("interpret: stop anchor scan")

// scanAnchorVisit streams sym's initial row set through visit, one row per
// node id nodeSatisfiesConstraint(nc) admits, drawn from whichever candidate
// source rankOf picked (a single id() lookup, a single objectid lookup, the
// smallest AND-ed kind bitmap, or every node in the snapshot). Every
// candidate visited -- regardless of source -- spends one work unit and is
// independently checked against the *complete* nc (not just whatever
// narrowed the candidate source), since e.g. an id() anchor does not by
// itself guarantee the same symbol's kind labels also hold. Per Budgets'
// documented "+1 per row produced anywhere -- component rows,
// cartesian-joined rows, and final rows alike", a candidate that survives
// nc also spends a second, separate work unit for the row it produces --
// exactly the two-tier charge expandStep/verifyClosingStep already apply
// (inspect the candidate, then charge again for each one that becomes a
// row) -- so the anchor scan that seeds a component is not a silent
// exception to that accounting.
//
// visit returning errStopScan stops the scan cleanly -- e.g. a chunked
// caller that only wants the next N anchor rows -- and scanAnchorVisit
// returns that sentinel unchanged rather than wrapping or swallowing it, so
// the caller can tell a deliberate stop apart from a genuine evaluator
// error (any other non-nil return from visit aborts the scan the same way
// and is likewise returned unchanged).
func scanAnchorVisit(env *Env, meter *workMeter, sym string, nc *NodeConstraint, visit func(*Row) error) error {
	overlay := env.Snap.Overlay()
	admit := func(id snapshot.NodeID) error {
		// A tombstoned base node's dense id still resolves through Dense
		// (snapshot.View.Alive's own doc) and still occupies a slot in
		// [0, NodeCount()) (snapshot.View.NodeCount's doc), so every
		// candidate source below -- not just the unconstrained full-scan
		// default -- can hand admit a dead id once Overlay() is true: an
		// id() anchor (case below) resolves through Dense with no liveness
		// check of its own, and the unconstrained default scans every dense
		// id in range regardless of whether it is still alive. The
		// ObjectIDAnchor and kind-bitmap candidate sources are already
		// alive-correct by construction (NodesByObjectID/NodesOfKind's own
		// overlay doc), so this is a no-op for them, not a second filter.
		if overlay && !env.Snap.Alive(id) {
			return nil
		}
		if err := meter.spend(1); err != nil {
			return err
		}
		if !nodeSatisfiesConstraint(env, nc, id) {
			return nil
		}
		r := NewRow()
		r.SetNode(sym, id)
		if err := meter.spend(1); err != nil {
			return err
		}
		return visit(r)
	}

	switch {
	case nc != nil && len(nc.IDs) > 0:
		if id, ok := env.Snap.Dense(nc.IDs[0]); ok {
			if err := admit(id); err != nil {
				return err
			}
		}

	case nc != nil && nc.ObjectIDAnchor != nil:
		// PostgreSQL enforces no uniqueness constraint on objectid, so more
		// than one node can carry the same value; visit every one of them
		// (NodesByObjectID), not just an arbitrary witness -- see its doc.
		if ids, ok := env.Snap.NodesByObjectID(*nc.ObjectIDAnchor); ok {
			for _, id := range ids {
				if err := admit(id); err != nil {
					return err
				}
			}
		}

	case nc != nil && len(nc.Kinds) > 0:
		var iterErr error
		smallestKindBitmap(env, nc.Kinds).Iterate(func(id snapshot.NodeID) bool {
			if err := admit(id); err != nil {
				iterErr = err
				return false
			}
			return true
		})
		if iterErr != nil {
			return iterErr
		}

	default:
		n := env.Snap.NodeCount()
		for i := 0; i < n; i++ {
			if err := admit(snapshot.NodeID(i)); err != nil {
				return err
			}
		}
	}

	return nil
}

// scanAnchor produces sym's initial row set (see scanAnchorVisit's doc for
// the exact candidate-source/work-accounting contract). A thin
// append-collecting wrapper over scanAnchorVisit: its own visit callback
// never returns errStopScan, so scanAnchorVisit's return value here is
// always either nil or a genuine evaluator error, never the sentinel.
func scanAnchor(env *Env, meter *workMeter, sym string, nc *NodeConstraint) ([]*Row, error) {
	var rows []*Row
	if err := scanAnchorVisit(env, meter, sym, nc, func(r *Row) error {
		rows = append(rows, r)
		return nil
	}); err != nil {
		return nil, err
	}
	return rows, nil
}

// nodeSatisfiesConstraint reports whether id carries every one of nc's kind
// labels (AND-ed) and, redundantly with Part.Where (see NodeConstraint's own
// doc comment), matches nc's id()/objectid anchor if either is set. A nil nc
// (a node symbol this executor did not look up a constraint for -- never
// actually produced by Plan, but harmless) admits everything.
func nodeSatisfiesConstraint(env *Env, nc *NodeConstraint, id snapshot.NodeID) bool {
	if nc == nil {
		return true
	}
	if len(nc.Kinds) > 0 {
		have := env.Snap.KindIDsOf(id)
		for _, k := range nc.Kinds {
			if !containsKindID(have, k) {
				return false
			}
		}
	}
	if len(nc.IDs) > 0 {
		dbID := env.Snap.GraphID(id)
		found := false
		for _, want := range nc.IDs {
			if want == dbID {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	if nc.ObjectIDAnchor != nil {
		ids, ok := env.Snap.NodesByObjectID(*nc.ObjectIDAnchor)
		if !ok || !containsNodeID(ids, id) {
			return false
		}
	}
	return true
}

// containsNodeID reports whether id appears anywhere in ids, used to test
// node-id membership against a possibly-multi-valued objectid match set
// (see NodesByObjectID).
func containsNodeID(ids []snapshot.NodeID, id snapshot.NodeID) bool {
	for _, h := range ids {
		if h == id {
			return true
		}
	}
	return false
}

// --- Step expansion ---------------------------------------------------------

// adjCandidate is one adjacency hit: the node at the other end, the edge's
// kind, and its identity -- fwd (a forward-CSR index; Snap.OutTargets[fwd]/
// Snap.OutKinds[fwd]/Snap.OutEdgeIDs[fwd] all describe it, per EdgeRef's
// doc) when adjacency walked env.Snap.Out/In (!Overlay()), or edgeID (the
// edge's own database id, as yielded by OutEdges/InEdges -- a delta edge has
// no forward-CSR slot to name at all) when it walked OutEdges/InEdges
// (Overlay()). Exactly one of fwd/edgeID is meaningful for a given
// adjCandidate, decided the same way EdgeRef's own two fields are -- see its
// doc. edgeRefFor/containsFwd (expand.go) are the two places that read
// whichever one applies.
type adjCandidate struct {
	other  snapshot.NodeID
	kind   snapshot.KindID
	fwd    uint64
	edgeID uint64
}

// edgeRefFor converts c into the EdgeRef a Row binds it under, resolving
// which of EdgeRef's two fields to populate the same way adjCandidate's own
// doc decides which of fwd/edgeID is meaningful: c was produced by
// adjacency() against snap, so snap.Overlay() is exactly the discriminant
// that decided which field adjacency itself populated.
func edgeRefFor(snap *snapshot.View, c adjCandidate) EdgeRef {
	if snap.Overlay() {
		return EdgeRef{EdgeID: c.edgeID}
	}
	return EdgeRef{Fwd: c.fwd}
}

// candidateIdentity returns c's own overlay-aware edge identity value, for
// callers (Row.markEdgeUsed/edgeUsed) that need a single uint64 to record or
// test "is this the same physical edge another Step already consumed" --
// see adjCandidate's own doc for why fwd and edgeID are never mixed within
// one query's Row bookkeeping (a single execution runs against one
// snap.Overlay()-ness throughout).
func candidateIdentity(snap *snapshot.View, c adjCandidate) uint64 {
	if snap.Overlay() {
		return c.edgeID
	}
	return c.fwd
}

// edgeRefIdentity returns ref's overlay-aware edge identity, the EdgeRef
// counterpart of candidateIdentity: the same discriminant (snap.Overlay())
// that decided which of EdgeRef's two fields edgeRefFor populated decides
// which one names the physical edge here. Used to record a finished trail's
// edges into Row.trailEdges (see that field's doc).
func edgeRefIdentity(snap *snapshot.View, ref EdgeRef) uint64 {
	if snap.Overlay() {
		return ref.EdgeID
	}
	return ref.Fwd
}

// adjacency enumerates every edge incident to bound that step's shape
// admits, spending one work unit per adjacency slot inspected (whether or
// not it survives any filter -- edge-kind filtering happens in the caller,
// see expandStep/verifyClosingStep):
//
//   - Direction == Outbound: Out(bound) if boundIsFrom (bound is the
//     pattern's structural "from" side, so its forward CSR gives the "to"
//     candidates directly), else In(bound) (bound is the "to" side, so its
//     reverse CSR gives the "from" candidates that would traverse forward
//     into it).
//   - Direction == Both, step.FromSym != step.ToSym (two distinct pattern
//     symbols joined by an undirected step -- the common case, and the only
//     shape expandStep's tree-edge calls ever produce): both Out(bound) and
//     In(bound), each candidate guarded by other != bound -- the "start.id
//     != end.id" rule pinned from dawgs' traversal_directionless.go
//     leftNodeConstraint/terminalNodeConstraint, applied only when the two
//     endpoint *identifiers* differ, mirroring
//     buildPairwiseDirectionlessTraversalPatternRoot's own "Only apply
//     endpoint inequality when the bound nodes are different" comment: a
//     non-self-loop edge satisfies exactly one of pg's `id = start_id`/`id =
//     end_id` disjuncts from bound's perspective, so Out and In never
//     double-count the same physical edge here, and a genuine self-loop
//     (which would satisfy both) is excluded from both instead. The guard
//     is keyed on the step's *symbols*, not a runtime id comparison: pg
//     decides this branch statically from the identifiers at translate
//     time, never from a runtime value, so two distinct symbols that happen
//     to alias to the same node at runtime (e.g. bound earlier via an
//     unrelated self-loop elsewhere in the pattern) must still be excluded
//     here exactly as they would be in pg's generated SQL -- see
//     TestExecUndirectedDifferentSymbolClosingStepExcludesRuntimeSelfLoop.
//   - Direction == Both, step.FromSym == step.ToSym (a literal same-symbol
//     pattern, e.g. `(n)-[:E]-(n)`): the inequality guard above does not
//     apply -- excluding other == bound here would make every such pattern
//     deterministically match nothing, silently diverging from pg, which
//     does match self-loops for this shape. This mirrors dawgs'
//     buildSelfReferentialDirectionlessTraversalRoot, the branch reached for
//     exactly this shape, which applies no equivalent inequality guard
//     (its comment: "push the right-node join condition into WHERE so that
//     start_id and end_id both reference the same node"). Only Out(bound)
//     is walked, not also In(bound): a self-loop's start and end are the
//     same node, so it already appears in Out(bound) once; walking
//     In(bound) too would find the identical edge a second time via the
//     reverse index and double-count it. Multiplicity was pinned by
//     generating dawgs@v0.8.0's actual SQL for `match (u)-[]-(u) return u`
//     (via translate.Translate/Translated): the self-referential root joins
//     the edge table to a single node table once, with the *same*
//     OR-condition `(n0.id = e.end_id or n0.id = e.start_id)` appearing in
//     both the JOIN's ON and the WHERE clause -- a single INNER JOIN, not a
//     union of a forward and a reverse traversal, so it emits exactly ONE
//     row per matching self-loop edge, never two. Walking only Out(bound)
//     here reproduces that multiplicity exactly.
//
// buildStep only ever normalizes a compiled Step's Direction to Outbound or
// Both (an inbound arrow is swapped into an outbound one at plan time), so
// this is exhaustive over what a Step can actually carry.
func adjacency(env *Env, meter *workMeter, step *Step, bound snapshot.NodeID, boundIsFrom bool) ([]adjCandidate, error) {
	var out []adjCandidate
	sameSymbol := step.FromSym == step.ToSym

	visitOut := func() error {
		if !env.Snap.Overlay() {
			targets, kinds, _ := env.Snap.Out(bound)
			lo := env.Snap.Base().OutOffsets[bound]
			for i, other := range targets {
				if err := meter.spend(1); err != nil {
					return err
				}
				if step.Direction == graph.DirectionBoth && !sameSymbol && other == bound {
					continue
				}
				out = append(out, adjCandidate{other: other, kind: kinds[i], fwd: lo + uint64(i)})
			}
			return nil
		}
		var spendErr error
		env.Snap.OutEdges(bound, func(other snapshot.NodeID, kind snapshot.KindID, edgeID uint64) bool {
			if err := meter.spend(1); err != nil {
				spendErr = err
				return false
			}
			if step.Direction == graph.DirectionBoth && !sameSymbol && other == bound {
				return true
			}
			out = append(out, adjCandidate{other: other, kind: kind, edgeID: edgeID})
			return true
		})
		return spendErr
	}
	visitIn := func() error {
		if !env.Snap.Overlay() {
			sources, kinds := env.Snap.In(bound)
			lo := env.Snap.Base().InOffsets[bound]
			for i, other := range sources {
				if err := meter.spend(1); err != nil {
					return err
				}
				if step.Direction == graph.DirectionBoth && !sameSymbol && other == bound {
					continue
				}
				out = append(out, adjCandidate{other: other, kind: kinds[i], fwd: uint64(env.Snap.Base().InEdgeIdx[lo+uint64(i)])})
			}
			return nil
		}
		var spendErr error
		env.Snap.InEdges(bound, func(other snapshot.NodeID, kind snapshot.KindID, edgeID uint64) bool {
			if err := meter.spend(1); err != nil {
				spendErr = err
				return false
			}
			if step.Direction == graph.DirectionBoth && !sameSymbol && other == bound {
				return true
			}
			out = append(out, adjCandidate{other: other, kind: kind, edgeID: edgeID})
			return true
		})
		return spendErr
	}

	if step.Direction == graph.DirectionBoth {
		if err := visitOut(); err != nil {
			return nil, err
		}
		if sameSymbol {
			// Same-symbol undirected step: only a self-loop on bound can
			// ever match (the caller reduces every candidate to other ==
			// bound anyway), and Out(bound) already lists each self-loop
			// exactly once -- see the doc comment above for the pg-matching
			// multiplicity this preserves.
			return out, nil
		}
		if err := visitIn(); err != nil {
			return nil, err
		}
		return out, nil
	}

	if boundIsFrom {
		if err := visitOut(); err != nil {
			return nil, err
		}
	} else {
		if err := visitIn(); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// edgeKindOK reports whether have is admitted by want (Step.EdgeKinds'
// documented "empty = any" contract, and Cypher's OR-of-types semantics for
// a relationship pattern's `[:A|B]` disjunction when want is non-empty).
func edgeKindOK(want []snapshot.KindID, have snapshot.KindID) bool {
	if len(want) == 0 {
		return true
	}
	for _, k := range want {
		if k == have {
			return true
		}
	}
	return false
}

// expandStep grows rows across a "tree" Step (see runComponent's doc): for
// every row, it enumerates boundSym's adjacency, keeps candidates whose edge
// kind step admits and whose far endpoint unboundNC admits (a required,
// structural check -- see NodeConstraint's doc comment on why pattern-
// position kind labels have no WHERE-clause counterpart to fall back on),
// and emits one new row per surviving candidate binding unboundSym (and, if
// named, step.EdgeSym). pathArcKey, when non-empty, additionally binds the
// surviving candidate's own specific edge under that key too (see
// pathStepArcKey's doc) -- expandChainComponent's own mechanism for
// recovering an anonymous fixed step's exact edge instance once the whole
// chain has been walked; "" (every call site outside expandChainComponent)
// skips this entirely, matching a caller with no path to assemble.
func expandStep(env *Env, meter *workMeter, rows []*Row, step *Step, boundSym, unboundSym string, boundIsFrom bool, unboundNC *NodeConstraint, pathArcKey string) ([]*Row, error) {
	var out []*Row
	for _, r := range rows {
		boundID, _ := r.Node(boundSym)
		cands, err := adjacency(env, meter, step, boundID, boundIsFrom)
		if err != nil {
			return nil, err
		}
		for _, c := range cands {
			if !edgeKindOK(step.EdgeKinds, c.kind) {
				continue
			}
			if !nodeSatisfiesConstraint(env, unboundNC, c.other) {
				continue
			}
			// Cypher's relationship-uniqueness rule, the same one
			// verifyClosingStep applies: no two Steps of one pattern may
			// resolve to the identical relationship. This step marks its
			// edge used just below, but used to consume one already taken
			// by an earlier step of the same pattern without objecting --
			// so a two-step chain over a single edge matched it twice.
			// dawgs emits `e1.id != e0.id` for every step past the first,
			// so `MATCH (a)-[:E]->(b)<-[:E]-(c)` over one edge returns no
			// rows from PostgreSQL and returned one here, and the
			// co-membership shape
			// `(u1:User)-[:MemberOf]->(g)<-[:MemberOf]-(u2:User)` paired
			// every user with themselves through a single membership edge.
			if r.edgeUsed(candidateIdentity(env.Snap, c)) {
				continue
			}
			// The other half of the same rule for a mixed fixed/var-length
			// chain: a fixed step may not resolve to an edge a preceding
			// variable-length step's trail already consumed either -- dawgs
			// emits `e1.id != all (path)` for every expansion preceding a
			// fixed step (see Row.trailEdges' doc for the full asymmetric
			// contract, incl. why the trail DFS itself does NOT check this
			// set).
			if r.trailEdgeUsed(candidateIdentity(env.Snap, c)) {
				continue
			}
			nr := cloneRow(r)
			nr.SetNode(unboundSym, c.other)
			if step.EdgeSym != "" {
				nr.SetEdge(step.EdgeSym, edgeRefFor(env.Snap, c))
			}
			if pathArcKey != "" {
				nr.SetEdge(pathArcKey, edgeRefFor(env.Snap, c))
			}
			// Recorded regardless of EdgeSym/pathArcKey -- an anonymous
			// relationship pattern (no variable name at all) still consumes
			// a real, specific edge, and verifyClosingStep needs to know
			// that just as much as it would for a named one. See Row's
			// usedEdges doc.
			nr.markEdgeUsed(candidateIdentity(env.Snap, c))
			if err := meter.spend(1); err != nil {
				return nil, err
			}
			out = append(out, nr)
		}
	}
	return out, nil
}

// verifyClosingStep checks a "closing" Step (see runComponent's doc) whose
// two endpoints are already bound in every row: for each row it looks for a
// matching edge between the two bound ids, fanning out one row per matching
// edge (parallel qualifying edges, like a fresh tree-edge expansion, each
// produce a distinct row) and binding step.EdgeSym (if named) to whichever
// edge matched. A row with no matching edge at all is dropped.
//
// A candidate edge already recorded in the row's own usedEdges (r.edgeUsed)
// is skipped -- Cypher's relationship-uniqueness rule: no two Steps of the
// same pattern may resolve to the identical relationship. Without this
// check, a self-loop node (its one outgoing edge simultaneously satisfying
// both "the tree Step that reached it" and "the closing Step verifying the
// cycle") would silently double-count that single edge as two distinct
// hops -- exactly the shape the dawgs conformance corpus's own "self_cycles"
// dataset catches (`MATCH (a)-[]->(b)-[]->(a)` over a node with a single
// self-loop edge must not itself count as a valid two-hop round trip, since
// there is no second, distinct edge to close the cycle with). This is the
// one place in this package such a coincidence can arise undetected: a
// strict linear chain (expandChainComponent) never revisits an
// already-bound symbol at all (isStrictLinearChain's own "no repeated
// ToSym" check), so it can never produce a closing Step in the first place,
// and every *other* reuse of the same relationship variable name is already
// rejected outright at plan time (buildStep's declareEdgeSymbol) -- neither
// of those two guards, though, has anything to say about two *anonymous* (or
// differently named) relationship patterns that just happen, for this one
// row, to resolve to the same physical edge.
func verifyClosingStep(env *Env, meter *workMeter, rows []*Row, step *Step) ([]*Row, error) {
	var out []*Row
	for _, r := range rows {
		fromID, _ := r.Node(step.FromSym)
		toID, _ := r.Node(step.ToSym)

		cands, err := adjacency(env, meter, step, fromID, true)
		if err != nil {
			return nil, err
		}
		for _, c := range cands {
			if c.other != toID {
				continue
			}
			if !edgeKindOK(step.EdgeKinds, c.kind) {
				continue
			}
			if r.edgeUsed(candidateIdentity(env.Snap, c)) {
				continue
			}
			// Same trail-edge exclusion expandStep applies (see Row.trailEdges'
			// doc): unreachable today -- a component containing a
			// variable-length Step is always a strict linear chain, which
			// never produces a closing Step -- but checked anyway so the two
			// fixed-step expanders enforce one rule, not two.
			if r.trailEdgeUsed(candidateIdentity(env.Snap, c)) {
				continue
			}
			nr := cloneRow(r)
			if step.EdgeSym != "" {
				nr.SetEdge(step.EdgeSym, edgeRefFor(env.Snap, c))
			}
			nr.markEdgeUsed(candidateIdentity(env.Snap, c))
			if err := meter.spend(1); err != nil {
				return nil, err
			}
			out = append(out, nr)
		}
	}
	return out, nil
}

// --- RETURN projection -----------------------------------------------------

// projectionKeys returns proj's RETURN column names in projection order --
// the caller-observable names (ProjectionOutput.OutputName, pg's own
// naming), not the Cypher-side aliases planning resolves against.
func projectionKeys(proj Projection) []string {
	keys := make([]string, len(proj.Items))
	for i, item := range proj.Items {
		keys[i] = item.OutputName
	}
	return keys
}

// projectRow evaluates every RETURN item in proj against r, in order.
func projectRow(env *Env, proj Projection, r *Row) ([]OutVal, error) {
	out := make([]OutVal, len(proj.Items))
	for i, item := range proj.Items {
		v, err := projectItem(env, r, item)
		if err != nil {
			return nil, err
		}
		out[i] = v
	}
	return out, nil
}

// projectItem evaluates one RETURN item. A bare node, edge, or path variable
// projects as OutNode/OutEdge/OutPath directly from the row's own binding
// (EvalValue would otherwise materialize a node's full property map, reject
// an edge variable outright, or (having no path-value model at all) report
// ErrUnsupported for a path variable -- see evalVariableValue's doc
// comment); everything else -- a property lookup, a function call,
// arithmetic, or a literal -- goes through EvalValue and projects as
// OutScalar.
func projectItem(env *Env, r *Row, item ProjectionOutput) (OutVal, error) {
	if v, isVar := unwrapParens(item.Expr).(*cypher.Variable); isVar && v != nil {
		if nodeID, ok := r.Node(v.Symbol); ok {
			return OutVal{Kind: OutNode, Node: nodeID}, nil
		}
		if edgeRef, ok := r.Edge(v.Symbol); ok {
			return OutVal{Kind: OutEdge, Edge: edgeRef}, nil
		}
		if pv, ok := r.PathVar(v.Symbol); ok {
			// Every path value this package ever binds (see expand.go) is a
			// *PathVal; Row.PathVar's `any` shape exists only because it
			// predates that task, per its own doc comment.
			return OutVal{Kind: OutPath, Path: pv.(*PathVal)}, nil
		}
	}

	val, ok, err := EvalValue(env, r, item.Expr)
	if err != nil {
		return OutVal{}, err
	}
	if !ok {
		// Absent, not a present JSON null -- see OutVal.ScalarAbsent's doc.
		return OutVal{Kind: OutScalar, Scalar: nil, ScalarAbsent: true}, nil
	}
	return OutVal{Kind: OutScalar, Scalar: val}, nil
}
