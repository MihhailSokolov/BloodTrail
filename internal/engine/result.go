// SPDX-License-Identifier: Apache-2.0

package engine

import "github.com/specterops/dawgs/graph"

// pathResult implements graph.Result over a graph.PathSet computed by the
// in-memory engine: one row per path, projecting a single "p" column whose
// value is the graph.Path itself. It is the shape ops.FetchByQuery (the real
// BloodHound cypher-endpoint consumer) expects back from
// graph.Transaction.Query -- see graph/result.go's Result interface and
// ops/ops.go:190's Next/Values/Mapper/Close usage.
//
// The zero value is not useful; construct with newPathResult.
type pathResult struct {
	paths graph.PathSet

	// idx is the current row. It starts one before the first path (-1);
	// Next() advances it before checking bounds, mirroring the
	// database/sql Rows convention FetchByQuery's `for queryResult.Next()
	// { ... }` loop assumes -- Values() is only ever called after a Next()
	// that returned true.
	idx int
}

// newPathResult wraps paths as a graph.Result: one row per path, Keys()
// []string{"p"}, Values() []any{path}, and a Mapper() that recognizes only
// *graph.Path targets, declining every other target type so
// ops.FetchByQuery's relationship/node/path type-switch (in that order; see
// ops/ops.go:190) reaches the path branch for these rows instead of
// misclassifying them.
func newPathResult(paths graph.PathSet) graph.Result {
	return &pathResult{paths: paths, idx: -1}
}

// Next advances to the next path, reporting whether one exists.
func (r *pathResult) Next() bool {
	r.idx++
	return r.idx < len(r.paths)
}

// Keys names the single projected column, matching a Cypher query that
// returns a lone path variable (e.g. "RETURN p").
func (r *pathResult) Keys() []string {
	return []string{"p"}
}

// Values returns the current row's single value: the graph.Path most
// recently advanced to by Next(). Calling Values() before any Next() call,
// or after Next() has returned false, returns nil.
func (r *pathResult) Values() []any {
	if r.idx < 0 || r.idx >= len(r.paths) {
		return nil
	}
	return []any{r.paths[r.idx]}
}

// Mapper returns a graph.ValueMapper built from mapPathValue.
func (r *pathResult) Mapper() graph.ValueMapper {
	return graph.NewValueMapper(mapPathValue)
}

// Scan is graph.Result's deprecated convenience method, implemented via
// graph.ScanNextResult for interface completeness; the intended consumer
// (ops.FetchByQuery) drives Values()/Mapper() directly rather than calling
// Scan.
func (r *pathResult) Scan(targets ...any) error {
	return graph.ScanNextResult(r, targets...)
}

// Error always returns nil: a pathResult is built from an already-computed
// PathSet, so nothing can fail during iteration.
func (r *pathResult) Error() error {
	return nil
}

// Close is a no-op: a pathResult holds no external resource (cursor,
// connection, file) to release.
func (r *pathResult) Close() {}

// mapPathValue is pathResult's ValueMapper function. It maps a graph.Path
// rawValue into a *graph.Path target and returns false for every other
// combination -- notably *graph.Relationship and *graph.Node targets, the
// two ops.FetchByQuery's Values() loop tries before *graph.Path (see
// ops/ops.go:190's if/else chain: relationship first, node second, path
// third) -- so a pathResult row is never misclassified as a relationship or
// node value and always reaches FetchByQuery's path branch.
func mapPathValue(rawValue, target any) bool {
	path, isPath := rawValue.(graph.Path)
	if !isPath {
		return false
	}

	pathTarget, isPathTarget := target.(*graph.Path)
	if !isPathTarget {
		return false
	}

	*pathTarget = path
	return true
}
