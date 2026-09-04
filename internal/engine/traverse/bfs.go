// SPDX-License-Identifier: Apache-2.0
package traverse

import "github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"

// bfsFrom runs a full kind-filtered BFS from seed over the chosen direction
// (forward = Out-CSR, !forward = In-CSR), capped at maxDepth, writing
// distances into sc. It resets sc first, so sc holds exactly this run's
// distances on return: dist[seed] == 0, and every reached node's distance
// is its hop count from seed. Returns the deepest level reached (0 if
// nothing beyond seed was reached).
func bfsFrom(s *snapshot.Snapshot, seed snapshot.NodeID, forward bool, kinds *snapshot.KindMask, maxDepth int, sc *scratch) int {
	sc.reset()
	sc.set(seed, 0)

	frontier := []snapshot.NodeID{seed}
	deepest := 0

	for d := 0; d < maxDepth && len(frontier) > 0; d++ {
		var next []snapshot.NodeID
		for _, u := range frontier {
			var targets []snapshot.NodeID
			var edgeKinds []snapshot.KindID
			if forward {
				targets, edgeKinds = s.Out(u)
			} else {
				targets, edgeKinds = s.In(u)
			}
			for i, w := range targets {
				if !kinds.Has(edgeKinds[i]) {
					continue
				}
				if _, seen := sc.get(w); seen {
					continue
				}
				sc.set(w, int8(d+1))
				next = append(next, w)
			}
		}
		if len(next) > 0 {
			deepest = d + 1
		}
		frontier = next
	}

	return deepest
}

// pathState is one partial (or complete) path on enumerate's explicit DFS
// stack: nodes[len(nodes)-1] is the walk's current frontier node.
type pathState struct {
	nodes []snapshot.NodeID
	kinds []snapshot.KindID
}

// enumerate collects every path from `from` whose every hop (u -> w, kind
// allowed) satisfies distTo.get(w) == distTo.get(u)-1, ending at distance 0.
// distTo must hold distances TO the destination (i.e. produced by a reverse
// bfsFrom over the In-CSR when enumerating forward paths). Appends to out,
// stopping when len(out) == cap (cap<=0: unbounded) or budget.add fails.
//
// It walks forward over Out-adjacency with an explicit stack in place of
// recursion; the strictly-decreasing distance requirement makes the
// explored state space acyclic even when the underlying graph has cycles,
// so no separate visited-set is needed. Parallel edges admitted under
// different allowed kinds are pushed as distinct stack entries and so
// produce distinct output paths.
func enumerate(s *snapshot.Snapshot, from snapshot.NodeID, distTo *scratch, kinds *snapshot.KindMask, cap int, budget *memBudget, out []Path) ([]Path, error) {
	stack := []pathState{{nodes: []snapshot.NodeID{from}}}

	for len(stack) > 0 {
		cur := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		u := cur.nodes[len(cur.nodes)-1]
		du, ok := distTo.get(u)
		if !ok {
			continue
		}

		if du == 0 {
			if len(cur.nodes) < 2 {
				// from == destination: no zero-length path is produced.
				continue
			}
			if err := budget.add(uint64(len(cur.nodes))*12 + 48); err != nil {
				return out, err
			}
			out = append(out, Path{
				Nodes: append([]snapshot.NodeID(nil), cur.nodes...),
				Kinds: append([]snapshot.KindID(nil), cur.kinds...),
			})
			if cap > 0 && len(out) == cap {
				return out, nil
			}
			continue
		}

		targets, edgeKinds := s.Out(u)
		for i, w := range targets {
			k := edgeKinds[i]
			if !kinds.Has(k) {
				continue
			}
			dw, ok := distTo.get(w)
			if !ok || dw != du-1 {
				continue
			}
			nextNodes := make([]snapshot.NodeID, len(cur.nodes)+1)
			copy(nextNodes, cur.nodes)
			nextNodes[len(cur.nodes)] = w

			nextKinds := make([]snapshot.KindID, len(cur.kinds)+1)
			copy(nextKinds, cur.kinds)
			nextKinds[len(cur.kinds)] = k

			stack = append(stack, pathState{nodes: nextNodes, kinds: nextKinds})
		}
	}

	return out, nil
}

// pathMode selects how many shortest paths pairPaths returns. A later task
// exposes an equivalent exported Mode type; this stays internal until a
// caller outside the package needs it.
type pathMode int

const (
	// modeAll returns every shortest r->t path, capped at capPerPair.
	modeAll pathMode = iota
	// modeOne returns a single shortest r->t path.
	modeOne
)

// pairShortest finds D = length of the shortest r->t path (kind-filtered),
// or -1 if none exists within maxDepth. r == t always yields -1: like
// enumerate, this package never produces zero-length paths, even when r
// sits on a cycle back to itself.
//
// Phase 1 is a bidirectional meet-in-the-middle search that alternately
// expands whichever frontier is currently smaller — forward from r over
// Out-CSR, backward from t over In-CSR — one level at a time. It keeps its
// own two scratches for this: scF for forward distances, scTmp for
// backward distances, so it never touches scT. Whenever a level's
// expansion settles a node the other side had already marked, the two
// sides' distances sum to a candidate for D. After finishing a level, the
// search keeps going only while the levels explored so far
// (levelsF+levelsB) are still less than min(D, maxDepth): once that sum
// reaches D, every node on a path shorter than D has necessarily already
// been settled by both sides (its position along any such path splits into
// a forward part and a backward part that together sum to less than D, so
// each part individually fits within the levels already expanded), so no
// further expansion can improve on D. The search also stops early if
// either frontier empties out (no path exists within maxDepth).
//
// Phase 2 runs only once D is known: it reruns a full, D-capped bfsFrom
// from r (forward, into scF) and from t (backward, into scT), discarding
// phase 1's partial exploration. This is what makes the caller's
// enumeration simple and duplicate-free (see pairEnumerate): with complete
// distance data on both sides, "lies on a shortest path" reduces to a
// local two-buffer check at each hop, rather than having to reconstruct
// which of phase 1's partially-explored nodes actually mattered.
func pairShortest(s *snapshot.Snapshot, r, t snapshot.NodeID, kinds *snapshot.KindMask, maxDepth int, scF, scT, scTmp *scratch) int {
	if r == t {
		return -1
	}

	scF.reset()
	scTmp.reset()
	scF.set(r, 0)
	scTmp.set(t, 0)

	frontF := []snapshot.NodeID{r}
	frontB := []snapshot.NodeID{t}
	levelsF, levelsB := 0, 0
	D := -1

	for len(frontF) > 0 && len(frontB) > 0 {
		limit := maxDepth
		if D >= 0 && D < limit {
			limit = D
		}
		if levelsF+levelsB >= limit {
			break
		}

		if len(frontF) <= len(frontB) {
			var next []snapshot.NodeID
			for _, u := range frontF {
				targets, edgeKinds := s.Out(u)
				for i, w := range targets {
					if !kinds.Has(edgeKinds[i]) {
						continue
					}
					if _, seen := scF.get(w); seen {
						continue
					}
					nd := int8(levelsF + 1)
					scF.set(w, nd)
					next = append(next, w)
					if db, seen := scTmp.get(w); seen {
						if cand := int(nd) + int(db); D < 0 || cand < D {
							D = cand
						}
					}
				}
			}
			levelsF++
			frontF = next
		} else {
			var next []snapshot.NodeID
			for _, u := range frontB {
				targets, edgeKinds := s.In(u)
				for i, w := range targets {
					if !kinds.Has(edgeKinds[i]) {
						continue
					}
					if _, seen := scTmp.get(w); seen {
						continue
					}
					nd := int8(levelsB + 1)
					scTmp.set(w, nd)
					next = append(next, w)
					if df, seen := scF.get(w); seen {
						if cand := int(df) + int(nd); D < 0 || cand < D {
							D = cand
						}
					}
				}
			}
			levelsB++
			frontB = next
		}
	}

	if D < 0 {
		return -1
	}

	bfsFrom(s, r, true, kinds, D, scF)
	bfsFrom(s, t, false, kinds, D, scT)

	return D
}

// pairPaths returns all (mode == modeAll, capped at capPerPair; capPerPair
// <= 0 is unbounded) or one (mode == modeOne) shortest r->t paths. It runs
// pairShortest to find D and populate scF/scT, then hands off to
// pairEnumerate; if no path exists within maxDepth, out is returned
// unchanged and no error is produced (absence of a path is not a failure).
func pairPaths(s *snapshot.Snapshot, r, t snapshot.NodeID, kinds *snapshot.KindMask, maxDepth, capPerPair int, mode pathMode, budget *memBudget, scF, scT, scTmp *scratch, out []Path) ([]Path, error) {
	D := pairShortest(s, r, t, kinds, maxDepth, scF, scT, scTmp)
	if D < 0 {
		return out, nil
	}

	cap := capPerPair
	if mode == modeOne {
		cap = 1
	}

	return pairEnumerate(s, r, t, D, kinds, cap, budget, scF, scT, out)
}

// pairEnumerate collects every shortest r->t path of length D, using the
// scF/scT distance buffers pairShortest's phase 2 populated: scF holds
// forward distances from r, scT holds backward distances from t, both
// capped at D. A hop u -> w (kind allowed) lies on some shortest path iff
// scF.get(w) == scF.get(u)+1 and scT.get(w) == D-scF.get(w): the first
// clause keeps the walk advancing at r's pace, the second confirms w still
// has exactly enough room left to reach t within the D-hop budget. Both
// clauses are needed together — unlike enumerate's single distTo buffer,
// scF and scT come from two independent BFS runs, so a node can satisfy
// one without the other (e.g. be reachable from r in scF.get(w) hops along
// a walk that overshoots t, or be close to t but only via a longer detour
// from r). Because phase 2 reran full BFS from both ends rather than
// reusing phase 1's partial data, every node satisfying both clauses truly
// sits on some shortest r->t path, so this DFS enumerates each such path
// exactly once (per distinct edge-kind choice, as with enumerate) and
// completes as soon as it reaches w == t rather than continuing past it.
func pairEnumerate(s *snapshot.Snapshot, r, t snapshot.NodeID, D int, kinds *snapshot.KindMask, cap int, budget *memBudget, scF, scT *scratch, out []Path) ([]Path, error) {
	stack := []pathState{{nodes: []snapshot.NodeID{r}}}

	for len(stack) > 0 {
		cur := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		u := cur.nodes[len(cur.nodes)-1]
		if u == t {
			if err := budget.add(uint64(len(cur.nodes))*12 + 48); err != nil {
				return out, err
			}
			out = append(out, Path{
				Nodes: append([]snapshot.NodeID(nil), cur.nodes...),
				Kinds: append([]snapshot.KindID(nil), cur.kinds...),
			})
			if cap > 0 && len(out) == cap {
				return out, nil
			}
			continue
		}

		du, ok := scF.get(u)
		if !ok {
			continue
		}

		targets, edgeKinds := s.Out(u)
		for i, w := range targets {
			k := edgeKinds[i]
			if !kinds.Has(k) {
				continue
			}
			dw, ok := scF.get(w)
			if !ok || dw != du+1 {
				continue
			}
			dtw, ok := scT.get(w)
			if !ok || int(dtw) != D-int(dw) {
				continue
			}

			nextNodes := make([]snapshot.NodeID, len(cur.nodes)+1)
			copy(nextNodes, cur.nodes)
			nextNodes[len(cur.nodes)] = w

			nextKinds := make([]snapshot.KindID, len(cur.kinds)+1)
			copy(nextKinds, cur.kinds)
			nextKinds[len(cur.kinds)] = k

			stack = append(stack, pathState{nodes: nextNodes, kinds: nextKinds})
		}
	}

	return out, nil
}
