// SPDX-License-Identifier: Apache-2.0
//
// csrbench: in-memory CSR graph micro-benchmark at AD-like scale.
// Single-threaded, Go 1.17-compatible. Synthetic graph with hub nodes
// (mimics Domain Users / large groups) and per-edge kind byte.
package main

import (
	"flag"
	"fmt"
	"math"
	"os"
	"runtime"
	"sort"
	"time"
)

type rng struct{ s uint64 }

func (r *rng) next() uint64 { // xorshift64*
	r.s ^= r.s >> 12
	r.s ^= r.s << 25
	r.s ^= r.s >> 27
	return r.s * 2685821657736338717
}
func (r *rng) f64() float64 { return float64(r.next()>>11) / (1 << 53) }

type CSR struct {
	off  []uint64 // len N+1
	adj  []uint32 // len M
	kind []uint8  // len M
}

func buildCSR(n int, src, dst []uint32, kind []uint8) *CSR {
	m := len(src)
	off := make([]uint64, n+1)
	for i := 0; i < m; i++ {
		off[src[i]+1]++
	}
	for i := 0; i < n; i++ {
		off[i+1] += off[i]
	}
	adj := make([]uint32, m)
	kd := make([]uint8, m)
	pos := make([]uint64, n)
	copy(pos, off[:n])
	for i := 0; i < m; i++ {
		p := pos[src[i]]
		adj[p] = dst[i]
		kd[p] = kind[i]
		pos[src[i]]++
	}
	return &CSR{off, adj, kd}
}

type bitset []uint64

func newBitset(n int) bitset        { return make(bitset, (n+63)/64) }
func (b bitset) test(i uint32) bool { return b[i>>6]&(1<<(i&63)) != 0 }
func (b bitset) set(i uint32)       { b[i>>6] |= 1 << (i & 63) }
func (b bitset) clear() {
	for i := range b {
		b[i] = 0
	}
}

// bfs: frontier BFS with optional kind mask (bit k set => kind k allowed). maxDepth<0 = unbounded.
// returns per-level frontier sizes, total visited, edges scanned.
func bfs(g *CSR, start uint32, kindMask uint64, maxDepth int, visited bitset, dist []uint8) (levels []int, visitedN int, scanned uint64) {
	frontier := []uint32{start}
	visited.set(start)
	if dist != nil {
		dist[start] = 0
	}
	visitedN = 1
	depth := 0
	for len(frontier) > 0 && (maxDepth < 0 || depth < maxDepth) {
		levels = append(levels, len(frontier))
		next := frontier[:0:0]
		for _, u := range frontier {
			s, e := g.off[u], g.off[u+1]
			scanned += e - s
			for i := s; i < e; i++ {
				if kindMask != 0 && kindMask&(1<<g.kind[i]) == 0 {
					continue
				}
				v := g.adj[i]
				if !visited.test(v) {
					visited.set(v)
					if dist != nil {
						dist[v] = uint8(depth + 1)
					}
					next = append(next, v)
					visitedN++
				}
			}
		}
		frontier = next
		depth++
	}
	if len(frontier) > 0 {
		levels = append(levels, len(frontier))
	}
	return
}

// bidirectional BFS shortest path (unweighted), returns path length (hops) or -1, and edges scanned.
func biBFS(fwd, rev *CSR, s, t uint32, kindMask uint64, maxDepth int) (int, uint64) {
	if s == t {
		return 0, 0
	}
	n := len(fwd.off) - 1
	seenF, seenB := make([]int32, n), make([]int32, n) // -1 = unseen else depth; allocate lazily in real impl
	for i := range seenF {
		seenF[i], seenB[i] = -1, -1
	}
	seenF[s], seenB[t] = 0, 0
	ff, fb := []uint32{s}, []uint32{t}
	df, db := 0, 0
	var scanned uint64
	for len(ff) > 0 && len(fb) > 0 {
		if maxDepth >= 0 && df+db >= maxDepth {
			return -1, scanned
		}
		// expand the smaller side
		if len(ff) <= len(fb) {
			next := ff[:0:0]
			for _, u := range ff {
				a, b := fwd.off[u], fwd.off[u+1]
				scanned += b - a
				for i := a; i < b; i++ {
					if kindMask != 0 && kindMask&(1<<fwd.kind[i]) == 0 {
						continue
					}
					v := fwd.adj[i]
					if seenB[v] >= 0 {
						return df + 1 + int(seenB[v]), scanned
					}
					if seenF[v] < 0 {
						seenF[v] = int32(df + 1)
						next = append(next, v)
					}
				}
			}
			ff = next
			df++
		} else {
			next := fb[:0:0]
			for _, u := range fb {
				a, b := rev.off[u], rev.off[u+1]
				scanned += b - a
				for i := a; i < b; i++ {
					if kindMask != 0 && kindMask&(1<<rev.kind[i]) == 0 {
						continue
					}
					v := rev.adj[i]
					if seenF[v] >= 0 {
						return db + 1 + int(seenF[v]), scanned
					}
					if seenB[v] < 0 {
						seenB[v] = int32(db + 1)
						next = append(next, v)
					}
				}
			}
			fb = next
			db++
		}
	}
	return -1, scanned
}

func memMB() (heap, sys float64) {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return float64(ms.HeapAlloc) / 1e6, float64(ms.Sys) / 1e6
}

func main() {
	n := flag.Int("n", 5_000_000, "nodes")
	m := flag.Int("m", 50_000_000, "edges")
	hubFrac := flag.Float64("hub", 0.5, "fraction of edges whose target is drawn from a skewed (hub) distribution")
	alpha := flag.Float64("alpha", 3.0, "skew exponent for hub targets (higher = more concentrated)")
	seed := flag.Uint64("seed", 42, "seed")
	flag.Parse()

	// Validate before any of it reaches the generator, where a bad value is
	// a panic rather than a message: -n 0 divides by zero on the first
	// `r.next() % uint64(N)`, and an -alpha of 0 makes math.Pow(x, 0) == 1
	// for every x, so every hub target becomes exactly N -- one past the
	// last node -- and the reverse CSR build indexes off[N+1] out of range.
	// A negative alpha is worse still, producing targets near 2^32.
	switch {
	case *n <= 0:
		fmt.Fprintln(os.Stderr, "csrbench: -n must be at least 1")
		os.Exit(2)
	case *m < 0:
		fmt.Fprintln(os.Stderr, "csrbench: -m cannot be negative")
		os.Exit(2)
	case *alpha <= 0:
		fmt.Fprintln(os.Stderr, "csrbench: -alpha must be greater than 0 (hub targets are drawn as pow(u, alpha), so alpha <= 0 lands outside the node range)")
		os.Exit(2)
	case *hubFrac < 0 || *hubFrac > 1:
		fmt.Fprintln(os.Stderr, "csrbench: -hub must be a fraction between 0 and 1")
		os.Exit(2)
	}

	N, M := *n, *m
	r := &rng{*seed}

	fmt.Printf("graph: N=%d M=%d (avg out-degree %.1f), GOARCH=%s, cores=%d\n", N, M, float64(M)/float64(N), runtime.GOARCH, runtime.NumCPU())

	// ---- generate edges ----
	t0 := time.Now()
	src := make([]uint32, M)
	dst := make([]uint32, M)
	kind := make([]uint8, M)
	nf := float64(N)
	for i := 0; i < M; i++ {
		src[i] = uint32(r.next() % uint64(N))
		if r.f64() < *hubFrac {
			// skewed toward low ids: hubs are ids near 0 (e.g. "Domain Users", big groups)
			dst[i] = uint32(math.Pow(r.f64(), *alpha) * nf)
		} else {
			dst[i] = uint32(r.next() % uint64(N))
		}
		// 30 edge kinds, skewed (kind 0 = MemberOf-like, very common)
		k := r.f64()
		switch {
		case k < 0.45:
			kind[i] = 0
		case k < 0.60:
			kind[i] = 1
		case k < 0.70:
			kind[i] = 2
		default:
			kind[i] = uint8(3 + r.next()%27)
		}
	}
	fmt.Printf("generate edges: %v\n", time.Since(t0))

	t0 = time.Now()
	fwd := buildCSR(N, src, dst, kind)
	rev := buildCSR(N, dst, src, kind)
	src, dst, kind = nil, nil, nil
	runtime.GC()
	h, s := memMB()
	fmt.Printf("build fwd+rev CSR: %v   heap=%.0f MB sys=%.0f MB\n", time.Since(t0), h, s)

	// degree stats
	maxIn, maxOut := uint64(0), uint64(0)
	var hubID uint32
	for i := 0; i < N; i++ {
		if d := rev.off[i+1] - rev.off[i]; d > maxIn {
			maxIn, hubID = d, uint32(i)
		}
		if d := fwd.off[i+1] - fwd.off[i]; d > maxOut {
			maxOut = d
		}
	}
	fmt.Printf("max in-degree=%d (node %d), max out-degree=%d\n", maxIn, hubID, maxOut)

	visited := newBitset(N)
	dist := make([]uint8, N)

	// 1. Full forward BFS from a random ordinary node
	start := uint32(N / 2)
	visited.clear()
	t0 = time.Now()
	lv, vn, sc := bfs(fwd, start, 0, -1, visited, nil)
	el := time.Since(t0)
	fmt.Printf("\n[1] full forward BFS from node %d: %v, visited=%d (%.1f%%), edges scanned=%d (%.0f M edges/s), levels=%v\n",
		start, el, vn, 100*float64(vn)/nf, sc, float64(sc)/el.Seconds()/1e6, lv)

	// 2. Reverse BFS from "Domain Admins"-like target => shortest-path distance from EVERY node to it
	target := hubID
	visited.clear()
	t0 = time.Now()
	lv, vn, sc = bfs(rev, target, 0, -1, visited, dist)
	el = time.Since(t0)
	fmt.Printf("[2] reverse BFS from hub target %d (all-sources shortest-path distances): %v, can-reach=%d (%.1f%%), edges scanned=%d, levels=%v\n",
		target, el, vn, 100*float64(vn)/nf, sc, lv)

	// 3. Kind-filtered reverse BFS (only 4 of 30 kinds allowed) - "edge filter" like BloodHound's pathfinding filter
	mask := uint64(1<<0 | 1<<2 | 1<<5 | 1<<7)
	visited.clear()
	t0 = time.Now()
	lv, vn, sc = bfs(rev, target, mask, -1, visited, nil)
	el = time.Since(t0)
	fmt.Printf("[3] reverse BFS from %d, 4/30 kinds allowed: %v, can-reach=%d, edges scanned=%d, levels=%v\n", target, el, vn, sc, lv)

	// 4. Bounded-depth (3 hops) forward BFS from ordinary node — the "couple of steps" case
	visited.clear()
	t0 = time.Now()
	lv, vn, sc = bfs(fwd, start, 0, 3, visited, nil)
	el = time.Since(t0)
	fmt.Printf("[4] 3-hop forward BFS from %d: %v, visited=%d, edges scanned=%d, levels=%v\n", start, el, vn, sc, lv)

	// 5. Bidirectional shortest path between random pairs
	var total time.Duration
	var totalScanned uint64
	found := 0
	pairs := 20
	for i := 0; i < pairs; i++ {
		a := uint32(r.next() % uint64(N))
		b := uint32(r.next() % uint64(N))
		t0 = time.Now()
		d, scn := biBFS(fwd, rev, a, b, 0, -1)
		total += time.Since(t0)
		totalScanned += scn
		if d >= 0 {
			found++
		}
	}
	fmt.Printf("[5] bidirectional BFS shortest path, %d random pairs: avg %v/pair (incl. O(N) seen-array alloc), found=%d, avg edges scanned=%d\n",
		pairs, total/time.Duration(pairs), found, totalScanned/uint64(pairs))

	// 6. Many-source BFS: 1000 random sources, 4-hop bounded, i.e. batch "what can each principal reach in 4 hops"
	t0 = time.Now()
	var sumVisited int
	for i := 0; i < 1000; i++ {
		a := uint32(r.next() % uint64(N))
		visited.clear() // O(N/64) words - 78k words for 5M nodes; fine
		_, vn, _ = bfs(fwd, a, 0, 4, visited, nil)
		sumVisited += vn
	}
	el = time.Since(t0)
	fmt.Printf("[6] 1000 x 4-hop BFS from random sources: %v total (%v each), avg reach=%d\n", el, el/1000, sumVisited/1000)

	// distance histogram from [2]
	hist := make(map[int]int)
	for i := 0; i < N; i++ {
		if visited.test(uint32(i)) { // note: visited was reused; recompute distances from dist[] instead
		}
	}
	_ = hist
	ds := make([]int, 0)
	cnt := map[int]int{}
	for i := 0; i < N; i++ {
		if dist[i] > 0 || uint32(i) == target {
			cnt[int(dist[i])]++
		}
	}
	for k := range cnt {
		ds = append(ds, k)
	}
	sort.Ints(ds)
	fmt.Printf("\nshortest-path distance histogram to target %d (from [2]):", target)
	for _, k := range ds {
		fmt.Printf(" d%d=%d", k, cnt[k])
	}
	fmt.Println()
	h, s = memMB()
	fmt.Printf("final heap=%.0f MB sys=%.0f MB\n", h, s)
	os.Exit(0)
}
