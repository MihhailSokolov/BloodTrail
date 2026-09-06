// SPDX-License-Identifier: Apache-2.0

// Package interpret: this file implements the executor: turning one Plan-
// compiled Query into a fully materialized ResultSet by scanning/expanding
// each Part's pattern directly over the snapshot's CSR arrays and property
// bags, joining shared variables by binding identity, filtering by WHERE,
// and projecting the RETURN clause.
//
// This file provides the single-Part machinery every Query shape this
// package serves is built from: pattern matching within one Part
// (matchPart/runComponent, fixed-length Steps here, variable-length/
// shortestPath Steps dispatched out to expand.go), WHERE filtering, and
// RETURN projection. pipeline.go's runQuery (Task 9) is what actually drives
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
// item, reading whatever expand.go's Task 8 functions bound via
// Row.SetPathVar (always a *PathVal in this package, see expand.go).
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

// errUnsupportedStep marks a query shape outside this task's scope: a
// var-length or shortestPath/allShortestPaths Step (Task 8), or a WITH
// clause / ORDER BY / SKIP / LIMIT / RETURN DISTINCT (Task 9). It is
// unexported deliberately -- a caller only needs to know Execute declined
// and must delegate the whole query to PostgreSQL, not which future task
// will lift the restriction.
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
type OutVal struct {
	Kind   OutKind
	Node   snapshot.NodeID
	Edge   EdgeRef
	Path   *PathVal
	Scalar any
}

// PathVal is a materialized path value: an alternating node/edge sequence,
// Nodes[i] connected to Nodes[i+1] by Edges[i]. Declared here because
// ResultSet/OutVal need the type; nothing in this file ever constructs one
// (see the package doc's Task 8 seam) -- an empty PathVal (both slices nil)
// is the eventual representation of a zero-length `*0..` path, per the
// design doc, but that shape is likewise Task 8's to produce.
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
type workMeter struct {
	budget    Budgets
	work      int64
	unchecked int64
	finalRows int
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
// pipeline.go's runQuery (Task 9); this function is left as the package's
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
func matchPart(env *Env, part *Part, meter *workMeter) ([]*Row, error) {
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
func cartesianJoin(meter *workMeter, left, right []*Row) ([]*Row, error) {
	out := make([]*Row, 0, len(left)*len(right))
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

// mergeRowInto copies every binding from src into dst.
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
//     see Task 8b's plan.go shortestStepsAreIsolated, which now also
//     rejects this shape at plan time -- and dispatches to
//     expandShortestPathComponent, which resolves both endpoints as
//     complete, pre-adjacency node sets and has no way to honor a further
//     chain hanging off either one.
//  3. A component consisting of exactly one variable-length Step (Range !=
//     nil) dispatches to expandVarLengthComponent, unchanged since Task 8.
//  4. A component with two or more Steps, at least one variable-length, no
//     shortestPath: Task 8b's mixed fixed/var-length chain shape (e.g.
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
//     before Task 8b -- zero behavior change for every existing shape.
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

// hasSpecialStep reports whether any of part.Chains[stepIdxs] is a
// variable-length or shortestPath/allShortestPaths Step -- runComponent's
// Task 8 dispatch condition.
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
// simple left-to-right chain: runComponent's Task 8b dispatch target for
// both a mixed fixed/var-length multi-step chain (e.g.
// `(c:Computer)-[:HasSession]->(u:User)-[:MemberOf*1..]->(g:Group)`) and,
// when pathSym != "", a chain that needs its whole traversal-order PathVal
// assembled for a `RETURN p` projection (see assembleChainPathVal) --
// including a PURE fixed-length named-path chain, so path assembly lives in
// exactly one place rather than being duplicated into runComponent's
// general BFS/closing-edge walk below.
//
// Anchoring (documented per the brief's explicit "simplest correct approach
// is fine" allowance): this does NOT run runComponent's general
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
// trail-DFS semantics). Trail-edge uniqueness stays scoped to each
// variable-length step's own call (a fresh DFS per step, per row) --
// distinct variable-length steps of the same chain may legally reuse the
// same physical edge in their own trails, mirroring dawgs' own per-expansion
// recursive CTE (see expand.go's package doc).
func expandChainComponent(env *Env, meter *workMeter, part *Part, stepIdxs []int, pathSym string) ([]*Row, error) {
	startSym := part.Chains[stepIdxs[0]].FromSym
	rows, err := scanAnchor(env, meter, startSym, part.Nodes[startSym])
	if err != nil {
		return nil, err
	}

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
func assembleChainPathVal(r *Row, part *Part, stepIdxs []int) (*PathVal, error) {
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

// scanAnchor produces sym's initial row set: one row per node id
// nodeSatisfiesConstraint(nc) admits, drawn from whichever candidate source
// rankOf picked (a single id() lookup, a single objectid lookup, the
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
func scanAnchor(env *Env, meter *workMeter, sym string, nc *NodeConstraint) ([]*Row, error) {
	var rows []*Row
	visit := func(id snapshot.NodeID) error {
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
		rows = append(rows, r)
		return nil
	}

	switch {
	case nc != nil && len(nc.IDs) > 0:
		if id, ok := env.Snap.Dense(nc.IDs[0]); ok {
			if err := visit(id); err != nil {
				return nil, err
			}
		}

	case nc != nil && nc.ObjectIDAnchor != nil:
		if id, ok := env.Snap.Props.NodeByObjectID(*nc.ObjectIDAnchor); ok {
			if err := visit(id); err != nil {
				return nil, err
			}
		}

	case nc != nil && len(nc.Kinds) > 0:
		var iterErr error
		smallestKindBitmap(env, nc.Kinds).Iterate(func(id snapshot.NodeID) bool {
			if err := visit(id); err != nil {
				iterErr = err
				return false
			}
			return true
		})
		if iterErr != nil {
			return nil, iterErr
		}

	default:
		n := env.Snap.NodeCount()
		for i := 0; i < n; i++ {
			if err := visit(snapshot.NodeID(i)); err != nil {
				return nil, err
			}
		}
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
		lo, hi := env.Snap.KindOffsets[id], env.Snap.KindOffsets[id+1]
		have := env.Snap.NodeKinds[lo:hi]
		for _, k := range nc.Kinds {
			if !containsKindID(have, k) {
				return false
			}
		}
	}
	if len(nc.IDs) > 0 {
		dbID := env.Snap.GraphIDs[id]
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
		gotID, ok := env.Snap.Props.NodeByObjectID(*nc.ObjectIDAnchor)
		if !ok || gotID != id {
			return false
		}
	}
	return true
}

// --- Step expansion ---------------------------------------------------------

// adjCandidate is one adjacency-slot hit: the node at the other end, the
// edge's kind, and its forward-CSR index (Snap.OutTargets[fwd]/
// Snap.OutKinds[fwd]/Snap.OutEdgeIDs[fwd] all describe it, per EdgeRef's
// doc).
type adjCandidate struct {
	other snapshot.NodeID
	kind  snapshot.KindID
	fwd   uint64
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
//     deterministically match nothing, which is exactly Finding 1 of the
//     2026-09-05 milestone-4 review. This mirrors dawgs'
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
		targets, kinds := env.Snap.Out(bound)
		lo := env.Snap.OutOffsets[bound]
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
	visitIn := func() error {
		sources, kinds := env.Snap.In(bound)
		lo := env.Snap.InOffsets[bound]
		for i, other := range sources {
			if err := meter.spend(1); err != nil {
				return err
			}
			if step.Direction == graph.DirectionBoth && !sameSymbol && other == bound {
				continue
			}
			out = append(out, adjCandidate{other: other, kind: kinds[i], fwd: uint64(env.Snap.InEdgeIdx[lo+uint64(i)])})
		}
		return nil
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
			nr := cloneRow(r)
			nr.SetNode(unboundSym, c.other)
			if step.EdgeSym != "" {
				nr.SetEdge(step.EdgeSym, EdgeRef{Fwd: c.fwd})
			}
			if pathArcKey != "" {
				nr.SetEdge(pathArcKey, EdgeRef{Fwd: c.fwd})
			}
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
// produce a distinct row -- this executor implements no relationship-
// uniqueness tracking, matching plan.go's own documented scope) and binding
// step.EdgeSym (if named) to whichever edge matched. A row with no matching
// edge at all is dropped.
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
			nr := cloneRow(r)
			if step.EdgeSym != "" {
				nr.SetEdge(step.EdgeSym, EdgeRef{Fwd: c.fwd})
			}
			if err := meter.spend(1); err != nil {
				return nil, err
			}
			out = append(out, nr)
		}
	}
	return out, nil
}

// --- RETURN projection -----------------------------------------------------

// projectionKeys returns proj's RETURN column names in projection order.
func projectionKeys(proj Projection) []string {
	keys := make([]string, len(proj.Items))
	for i, item := range proj.Items {
		keys[i] = item.Alias
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
		return OutVal{Kind: OutScalar, Scalar: nil}, nil
	}
	return OutVal{Kind: OutScalar, Scalar: val}, nil
}
