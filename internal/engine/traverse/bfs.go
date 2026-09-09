// SPDX-License-Identifier: Apache-2.0
package traverse

import "github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"

// bfsFrom runs a full kind-filtered BFS from seed over the chosen direction
// (forward = Out-CSR, !forward = In-CSR), capped at maxDepth, writing
// distances into sc. It resets sc first, so sc holds exactly this run's
// distances on return: dist[seed] == 0, and every reached node's distance
// is its hop count from seed. Returns the deepest level reached (0 if
// nothing beyond seed was reached).
//
// kinds nil means every kind is allowed (Query.Kinds' own doc) -- every
// kinds.Has(k) consumption site in this file (there are five, across
// bfsFrom, enumerate, pairShortest, and pairEnumerate) guards it with
// kinds != nil first, rather than requiring a fully-populated SetAll mask
// sized to some ceiling: a ceiling sized off the base snapshot alone cannot
// represent a kind a delta segment introduces after the base was built (see
// AllShortestPaths' own doc for the bug this used to cause).
func bfsFrom(s *snapshot.View, seed snapshot.NodeID, forward bool, kinds *snapshot.KindMask, maxDepth int, sc *scratch) int {
	sc.reset()
	sc.set(seed, 0)

	frontier := []snapshot.NodeID{seed}
	deepest := 0

	overlay := s.Overlay()
	for d := 0; d < maxDepth && len(frontier) > 0; d++ {
		var next []snapshot.NodeID
		for _, u := range frontier {
			visit := func(w snapshot.NodeID, k snapshot.KindID) {
				if kinds != nil && !kinds.Has(k) {
					return
				}
				if _, seen := sc.get(w); seen {
					return
				}
				sc.set(w, int8(d+1))
				next = append(next, w)
			}
			if !overlay {
				var targets []snapshot.NodeID
				var edgeKinds []snapshot.KindID
				if forward {
					targets, edgeKinds, _ = s.Out(u)
				} else {
					targets, edgeKinds = s.In(u)
				}
				for i, w := range targets {
					visit(w, edgeKinds[i])
				}
			} else if forward {
				s.OutEdges(u, func(w snapshot.NodeID, k snapshot.KindID, _ uint64) bool {
					visit(w, k)
					return true
				})
			} else {
				s.InEdges(u, func(w snapshot.NodeID, k snapshot.KindID, _ uint64) bool {
					visit(w, k)
					return true
				})
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

// enumerate collects every path whose every hop satisfies the strictly-
// decreasing distance requirement against distBuf, ending at distance 0.
// Direction is controlled by forward:
//
//   - forward == true: `from` is the path's root. distBuf must hold
//     distances TO the destination (produced by a reverse bfsFrom over the
//     In-CSR). The walk follows Out-adjacency from `from`, requiring
//     distBuf.get(w) == distBuf.get(u)-1 at each hop u -> w, so nodes are
//     visited in root-to-destination order and appended to the output
//     as-is.
//   - forward == false: `from` is the path's destination. distBuf must hold
//     distances FROM the root (produced by a forward bfsFrom over the
//     Out-CSR). The walk follows In-adjacency from `from` — i.e. it walks
//     the graph's real edges backward, from destination toward root — so
//     nodes are visited in destination-to-root order; each candidate stack
//     entry is reversed before being appended to the output so the emitted
//     Path is always root-to-destination.
//
// Appends to out, stopping when len(out) == cap (cap<=0: unbounded) or
// budget.add fails.
//
// It walks the chosen adjacency with an explicit stack in place of
// recursion; the strictly-decreasing distance requirement makes the
// explored state space acyclic even when the underlying graph has cycles,
// so no separate visited-set is needed. Parallel edges admitted under
// different allowed kinds are pushed as distinct stack entries and so
// produce distinct output paths.
func enumerate(s *snapshot.View, from snapshot.NodeID, distBuf *scratch, kinds *snapshot.KindMask, cap int, budget *memBudget, out []Path, forward bool) ([]Path, error) {
	stack := []pathState{{nodes: []snapshot.NodeID{from}}}

	overlay := s.Overlay()
	for len(stack) > 0 {
		cur := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		u := cur.nodes[len(cur.nodes)-1]
		du, ok := distBuf.get(u)
		if !ok {
			continue
		}

		if du == 0 {
			if len(cur.nodes) < 2 {
				// from == the other endpoint: no zero-length path is produced.
				continue
			}
			nodes := append([]snapshot.NodeID(nil), cur.nodes...)
			pathKinds := append([]snapshot.KindID(nil), cur.kinds...)
			if !forward {
				reverseNodes(nodes)
				reverseKinds(pathKinds)
			}
			if err := budget.add(uint64(len(nodes))*12 + 48); err != nil {
				return out, err
			}
			out = append(out, Path{Nodes: nodes, Kinds: pathKinds})
			if cap > 0 && len(out) == cap {
				return out, nil
			}
			continue
		}

		visit := func(w snapshot.NodeID, k snapshot.KindID) {
			if kinds != nil && !kinds.Has(k) {
				return
			}
			dw, ok := distBuf.get(w)
			if !ok || dw != du-1 {
				return
			}
			nextNodes := make([]snapshot.NodeID, len(cur.nodes)+1)
			copy(nextNodes, cur.nodes)
			nextNodes[len(cur.nodes)] = w

			nextKinds := make([]snapshot.KindID, len(cur.kinds)+1)
			copy(nextKinds, cur.kinds)
			nextKinds[len(cur.kinds)] = k

			stack = append(stack, pathState{nodes: nextNodes, kinds: nextKinds})
		}
		if !overlay {
			var targets []snapshot.NodeID
			var edgeKinds []snapshot.KindID
			if forward {
				targets, edgeKinds, _ = s.Out(u)
			} else {
				targets, edgeKinds = s.In(u)
			}
			for i, w := range targets {
				visit(w, edgeKinds[i])
			}
		} else if forward {
			s.OutEdges(u, func(w snapshot.NodeID, k snapshot.KindID, _ uint64) bool {
				visit(w, k)
				return true
			})
		} else {
			s.InEdges(u, func(w snapshot.NodeID, k snapshot.KindID, _ uint64) bool {
				visit(w, k)
				return true
			})
		}
	}

	return out, nil
}

// reverseNodes reverses ns in place.
func reverseNodes(ns []snapshot.NodeID) {
	for i, j := 0, len(ns)-1; i < j; i, j = i+1, j-1 {
		ns[i], ns[j] = ns[j], ns[i]
	}
}

// reverseKinds reverses ks in place.
func reverseKinds(ks []snapshot.KindID) {
	for i, j := 0, len(ks)-1; i < j; i, j = i+1, j-1 {
		ks[i], ks[j] = ks[j], ks[i]
	}
}

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
func pairShortest(s *snapshot.View, r, t snapshot.NodeID, kinds *snapshot.KindMask, maxDepth int, scF, scT, scTmp *scratch) int {
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

	overlay := s.Overlay()

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
			visit := func(w snapshot.NodeID, k snapshot.KindID) {
				if kinds != nil && !kinds.Has(k) {
					return
				}
				if _, seen := scF.get(w); seen {
					return
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
			for _, u := range frontF {
				if !overlay {
					targets, edgeKinds, _ := s.Out(u)
					for i, w := range targets {
						visit(w, edgeKinds[i])
					}
				} else {
					s.OutEdges(u, func(w snapshot.NodeID, k snapshot.KindID, _ uint64) bool {
						visit(w, k)
						return true
					})
				}
			}
			levelsF++
			frontF = next
		} else {
			var next []snapshot.NodeID
			visit := func(w snapshot.NodeID, k snapshot.KindID) {
				if kinds != nil && !kinds.Has(k) {
					return
				}
				if _, seen := scTmp.get(w); seen {
					return
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
			for _, u := range frontB {
				if !overlay {
					targets, edgeKinds := s.In(u)
					for i, w := range targets {
						visit(w, edgeKinds[i])
					}
				} else {
					s.InEdges(u, func(w snapshot.NodeID, k snapshot.KindID, _ uint64) bool {
						visit(w, k)
						return true
					})
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

// pairPaths returns r->t shortest paths, appending to out and stopping once
// len(out) reaches cap (cap<=0: unbounded). cap is an absolute target
// against out's cumulative length, not a per-call delta — see pathCap,
// which callers use to compute it (accounting for both Query.Limit and
// ModeOne's "one path per pair"). It runs pairShortest to find D and
// populate scF/scT, then hands off to pairEnumerate; if no path exists
// within maxDepth, out is returned unchanged and no error is produced
// (absence of a path is not a failure).
func pairPaths(s *snapshot.View, r, t snapshot.NodeID, kinds *snapshot.KindMask, maxDepth, cap int, budget *memBudget, scF, scT, scTmp *scratch, out []Path) ([]Path, error) {
	D := pairShortest(s, r, t, kinds, maxDepth, scF, scT, scTmp)
	if D < 0 {
		return out, nil
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
func pairEnumerate(s *snapshot.View, r, t snapshot.NodeID, D int, kinds *snapshot.KindMask, cap int, budget *memBudget, scF, scT *scratch, out []Path) ([]Path, error) {
	stack := []pathState{{nodes: []snapshot.NodeID{r}}}

	overlay := s.Overlay()
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

		visit := func(w snapshot.NodeID, k snapshot.KindID) {
			if kinds != nil && !kinds.Has(k) {
				return
			}
			dw, ok := scF.get(w)
			if !ok || dw != du+1 {
				return
			}
			dtw, ok := scT.get(w)
			if !ok || int(dtw) != D-int(dw) {
				return
			}

			nextNodes := make([]snapshot.NodeID, len(cur.nodes)+1)
			copy(nextNodes, cur.nodes)
			nextNodes[len(cur.nodes)] = w

			nextKinds := make([]snapshot.KindID, len(cur.kinds)+1)
			copy(nextKinds, cur.kinds)
			nextKinds[len(cur.kinds)] = k

			stack = append(stack, pathState{nodes: nextNodes, kinds: nextKinds})
		}
		if !overlay {
			targets, edgeKinds, _ := s.Out(u)
			for i, w := range targets {
				visit(w, edgeKinds[i])
			}
		} else {
			s.OutEdges(u, func(w snapshot.NodeID, k snapshot.KindID, _ uint64) bool {
				visit(w, k)
				return true
			})
		}
	}

	return out, nil
}
