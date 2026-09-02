# CSR benchmark: AD-shaped synthetic graph

Source: [`main.go`](main.go). Zero dependencies. This is a throwaway measurement tool, not part of the driver.

## Purpose

Check, rather than assert, that a compressed-sparse-row (CSR) representation of an
Active Directory attack graph fits in memory on ordinary hardware and that the
traversals BloodHound needs (full BFS, reverse BFS from a hub target, kind-filtered
BFS, bounded-depth BFS, bidirectional shortest path, many-source bounded BFS) run in
well under a second.

## Method

- Synthetic graph. Sources uniform; 50% of edge targets drawn from a skewed
  distribution (`u^3 * N`) so a few nodes have in-degree in the tens or hundreds of
  thousands, mimicking Domain Users / Authenticated Users / large groups.
- 30 edge kinds, skewed (kind 0 is 45% of edges, like MemberOf).
- Forward and reverse CSR: `uint64` offsets, `uint32` adjacency, `uint8` kind per edge.
- Single-threaded. Visited set is a bitset; BFS is frontier-based.
- Machine: Apple M3 Pro, 12 cores, 18 GB. Toolchain: Go 1.17.1 `darwin/amd64`
  running under Rosetta. Native arm64 or a modern x86 server core will be faster;
  treat these numbers as conservative.
- Seed 42 for both runs.

Reproduce:

```bash
go build -o csrbench . && ./csrbench -n 5000000 -m 50000000
```

Flags: `-n` nodes, `-m` edges, `-hub` fraction of hub-targeted edges (0.5),
`-alpha` skew exponent (3.0), `-seed` (42).

## Results

| Operation | 1M nodes / 10M edges | 5M nodes / 50M edges |
|---|---:|---:|
| Build forward + reverse CSR from edge list | 0.26 s | 2.5 s |
| Heap after build | 116 MB | 580 MB |
| Full forward BFS from an ordinary node | 118 ms | 857 ms |
| Reverse BFS from hub target (distance from every node) | 116 ms | 822 ms |
| Same, 4 of 30 kinds allowed | 178 ms | 1.12 s |
| 3-hop bounded BFS from an ordinary node | 12 µs | 23 µs |
| Bidirectional shortest path, random pair (avg of 20) | 0.74 ms | 4.1 ms |
| 1,000 × 4-hop BFS from random sources | 136 ms | 185 ms |
| Edge throughput during full BFS | 85 M edges/s | 58 M edges/s |

The bidirectional figure is dominated by allocating two N-entry `int32` "seen" arrays
per query in the prototype; a production version keeps them per worker and clears
only touched entries.

## Raw output, 5M / 50M

```
graph: N=5000000 M=50000000 (avg out-degree 10.0), GOARCH=amd64, cores=12
generate edges: 1.732856708s
build fwd+rev CSR: 2.495293625s   heap=580 MB sys=1168 MB
max in-degree=146008 (node 0), max out-degree=29

[1] full forward BFS from node 2500000: 857.37025ms, visited=4997379 (99.9%), edges scanned=49973707 (58 M edges/s), levels=[1 7 68 661 6498 62674 538928 2609775 1747001 31628 138]
[2] reverse BFS from hub target 0 (all-sources shortest-path distances): 822.379875ms, can-reach=4999769 (100.0%), edges scanned=49997877, levels=[1 143878 1204385 3318277 333042 185 1]
[3] reverse BFS from 0, 4/30 kinds allowed: 1.123353083s, can-reach=4983342, edges scanned=49837255, levels=[1 82889 443256 1773422 2340430 335360 7835 149]
[4] 3-hop forward BFS from 2500000: 23.459µs, visited=737, edges scanned=741, levels=[1 7 68 661]
[5] bidirectional BFS shortest path, 20 random pairs: avg 4.140237ms/pair (incl. O(N) seen-array alloc), found=20, avg edges scanned=4220
[6] 1000 x 4-hop BFS from random sources: 185.170791ms total (185.17µs each), avg reach=11015

shortest-path distance histogram to target 0 (from [2]): d0=1 d1=143878 d2=1204385 d3=3318277 d4=333042 d5=185 d6=1
final heap=929 MB sys=1299 MB
```

## Raw output, 1M / 10M

```
graph: N=1000000 M=10000000 (avg out-degree 10.0), GOARCH=amd64, cores=12
generate edges: 346.969958ms
build fwd+rev CSR: 260.483917ms   heap=116 MB sys=238 MB
max in-degree=50047 (node 0), max out-degree=30

[1] full forward BFS from node 500000: 117.503625ms, visited=999495 (99.9%), edges scanned=9994915 (85 M edges/s), levels=[1 4 31 306 3006 28363 217528 620049 129361 842 4]
[2] reverse BFS from hub target 0 (all-sources shortest-path distances): 115.671875ms, can-reach=999951 (100.0%), edges scanned=9999588, levels=[1 48790 364411 571550 15187 12]
[3] reverse BFS from 0, 4/30 kinds allowed: 178.082542ms, can-reach=996603, edges scanned=9966702, levels=[1 28185 143954 466028 336048 21914 465 8]
[4] 3-hop forward BFS from 500000: 12.25µs, visited=342, edges scanned=341, levels=[1 4 31 306]
[5] bidirectional BFS shortest path, 20 random pairs: avg 740.824µs/pair (incl. O(N) seen-array alloc), found=20, avg edges scanned=1981
[6] 1000 x 4-hop BFS from random sources: 135.561875ms total (135.561µs each), avg reach=10565

shortest-path distance histogram to target 0 (from [2]): d0=1 d1=48790 d2=364411 d3=571550 d4=15187 d5=12
final heap=99 MB sys=264 MB
```

## Caveat

The synthetic graph is a stand-in. Real AD graphs have more structure (OU containment
trees, per-domain clustering, ACL edges concentrated on a few objects), which keeps
BFS frontiers smaller for longer. The first milestone replaces these numbers with
measurements on a real topology export.
