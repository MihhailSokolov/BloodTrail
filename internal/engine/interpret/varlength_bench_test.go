// SPDX-License-Identifier: Apache-2.0

package interpret

import (
	"fmt"
	"testing"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
	"github.com/specterops/dawgs/cypher/frontend"
)

// BenchmarkVarLengthWideSeedSide reproduces the benchmark graph's "All
// Global Administrators" shape: 84k AZBase nodes, one AZTenant, and exactly
// ONE AZGlobalAdmin edge in the whole graph. The var-length step seeds from
// every AZBase node, so what this measures is the per-seed cost of a seed
// that cannot produce anything -- which is where that query's time goes,
// and which a profile showed was dominated by Row construction rather than
// by the trail walk anyone would suspect.
func BenchmarkVarLengthWideSeedSide(b *testing.B) {
	const (
		kiAZBase   snapshot.KindID = 1
		kiAZTenant snapshot.KindID = 2
		keAdmin    snapshot.KindID = 3
	)
	const wide = 84000

	bld := snapshot.NewBuilder(1)
	bld.SetKinds(map[snapshot.KindID]string{
		kiAZBase: "AZBase", kiAZTenant: "AZTenant", keAdmin: "AZGlobalAdmin",
	})
	for i := 0; i < wide; i++ {
		props := []byte(fmt.Sprintf(`{"objectid":"az-%d"}`, i))
		if err := bld.AddNode(uint64(i+1), []snapshot.KindID{kiAZBase}, props); err != nil {
			b.Fatal(err)
		}
	}
	tenantID := uint64(wide + 1)
	if err := bld.AddNode(tenantID, []snapshot.KindID{kiAZTenant}, []byte(`{"objectid":"tenant"}`)); err != nil {
		b.Fatal(err)
	}
	// The single admissible edge, from the LAST wide node.
	bld.AddEdge(1, uint64(wide), tenantID, keAdmin)
	snap, err := bld.Build()
	if err != nil {
		b.Fatal(err)
	}
	view := snapshot.NewView(snap)

	// Planned here rather than through the test helpers: planNoFail takes a
	// *testing.T, and a zero-value one would panic instead of failing.
	parsed, err := frontend.ParseCypher(frontend.NewContext(),
		`MATCH p = (:AZBase)-[:AZGlobalAdmin*1..]->(:AZTenant) RETURN p LIMIT 1000`)
	if err != nil {
		b.Fatalf("parse: %v", err)
	}
	rq, ok := Plan(parsed, view)
	if !ok || rq == nil {
		b.Fatal("planner declined the benchmark query")
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rs, err := Execute(&Env{Snap: view}, rq, Budgets{MaxRows: 100000, MaxWork: 1 << 28, MaxLiveRows: 2000000})
		if err != nil {
			b.Fatal(err)
		}
		if len(rs.Rows) != 1 {
			b.Fatalf("got %d rows, want 1", len(rs.Rows))
		}
	}
}
