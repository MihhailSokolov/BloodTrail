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
