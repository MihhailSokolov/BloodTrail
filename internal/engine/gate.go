// SPDX-License-Identifier: Apache-2.0

// Package engine: this file implements the translate-gate -- the last line
// of defense between the in-memory Cypher interpreter (interpret.Plan/
// Execute) and any query the real PostgreSQL-backed dawgs driver would
// refuse to serve.
//
// BloodTrail's in-memory interpreter is a *reimplementation* of a
// meaningful Cypher subset, not a wrapper around dawgs' own translator, so
// nothing already guarantees the two ever agree on which queries are
// servable. A query the interpreter mistakenly accepts but the real
// pg-backed driver would reject -- an unsupported function, a type mismatch
// pgsql's own type-checker would catch, a syntax shape its translator has no
// case for -- must never be served successfully from memory: that would
// silently produce a result PostgreSQL itself would never have returned,
// which is worse than simply delegating and reproducing pg's own rejection.
// translateGateOK closes that gap by asking dawgs' own translator the same
// question a real pg-backed serve would eventually ask, without any
// database round trip.
package engine

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/pgsql/translate"
	"github.com/specterops/dawgs/graph"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

// snapshotKindMapper implements pgsql.KindMapper (cypher/models/pgsql/
// model.go:13-18, dawgs@v0.8.0) over a single snapshot's own KindTable,
// letting translate.Translate resolve every kind name a Cypher query
// references (a node label, a relationship type) to the same database kind
// ids the real pg-backed driver's own KindMapper would produce -- without a
// database round trip, since the snapshot already carries the complete
// global kind table (see KindTable's own doc comment in snapshot/kinds.go).
type snapshotKindMapper struct {
	kinds *snapshot.KindTable
}

// MapKinds resolves every name in kinds via kinds.ID, all-or-nothing: if any
// single name is unresolvable, the whole call fails with a wrapped error
// naming every missing kind (never a user-visible message -- it only ever
// drives translateGateOK's bool result, discarded before any query result
// reaches a caller), rather than returning a partial or empty slice.
//
// The empty-slice case matters specifically for the *exclusive* kind
// matcher dawgs' own translator builds from a node label
// (translate/kind.go:12-43's newPGKindIDMatcher, dawgs@v0.8.0): matching an
// empty id slice with postgres' `@>` "contains" operator is trivially true
// of every row, i.e. it would silently match every node in the database
// instead of failing translation -- exactly the "matches everything" trap
// that function's own comment (kind.go:20-27) warns against. Returning
// (nil, non-nil error) instead of (empty, nil) for a wholly-unmappable kind
// list guarantees translateKindMatcher (translate/kind.go:45-56) sees the
// error and returns its own wrapped failure before newPGKindIDMatcher is
// ever reached, so an unknown kind can never degenerate into an unbounded
// match.
func (m snapshotKindMapper) MapKinds(_ context.Context, kinds graph.Kinds) ([]int16, error) {
	ids := make([]int16, 0, len(kinds))
	var missing []string
	for _, kind := range kinds {
		if id, ok := m.kinds.ID(kind.String()); ok {
			ids = append(ids, int16(id))
		} else {
			missing = append(missing, kind.String())
		}
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("bloodtrail: unknown kinds: %s", strings.Join(missing, ", "))
	}
	return ids, nil
}

// AssertKinds always fails. dawgs' translator only calls a KindMapper's
// AssertKinds for CREATE clauses (translate/create.go:278 for node creates,
// :450 for edge creates, dawgs@v0.8.0) -- never for anything this gate is
// asked to check, since BloodTrail's in-memory interpreter only ever plans
// read queries and rejects CREATE/MERGE/DELETE/SET outright before a query
// reaches translateGateOK at all. This method exists solely so
// snapshotKindMapper satisfies pgsql.KindMapper; reaching it at all would
// mean a write clause reached the gate, a bug elsewhere this milestone's
// read-only serving path is not supposed to allow -- so it fails loudly
// rather than fabricating an assertion result.
func (m snapshotKindMapper) AssertKinds(context.Context, graph.Kinds) ([]int16, error) {
	return nil, errors.New("bloodtrail: kind assertion during read serving")
}

// translateGateOK reports whether q would also translate successfully
// through dawgs' own PostgreSQL translator (translate.Translate, cypher/
// models/pgsql/translate/translator.go:848, dawgs@v0.8.0), discarding the
// translate.Result itself -- the gate only needs a yes/no answer, never the
// generated SQL. parameters is always nil: BloodTrail never serves
// parameterized queries from memory, so there is nothing to bind.
//
// Any error from translate.Translate -- including one produced by this
// gate's own snapshotKindMapper for a kind name the snapshot has never seen
// -- is treated as a translation failure (false). A panic inside
// translate.Translate is recovered and also treated as false: the dawgs
// translator is not guaranteed panic-free over every malformed or unusual
// input tree, and a panic during a gate check must never propagate out and
// take down the query-serving path the same way an ordinary translation
// error would not.
func translateGateOK(ctx context.Context, q *cypher.RegularQuery, snap *snapshot.Snapshot) (ok bool) {
	defer func() {
		if recover() != nil {
			ok = false
		}
	}()

	mapper := snapshotKindMapper{kinds: snap.Kinds}
	_, err := translate.Translate(ctx, q, mapper, nil, int32(snap.GraphID))
	return err == nil
}
