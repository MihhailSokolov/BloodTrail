// SPDX-License-Identifier: Apache-2.0

package interpret

import (
	"fmt"
	"testing"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

// TestParenthesisedLikeAnchorNarrowsOnALiteralRun pins both halves of the
// string anchor for a parenthesised property, `(n.name) STARTS WITH 'a_b'`,
// whose needle dawgs hands to LIKE unescaped. The answer must be LIKE's --
// aXb2 matches, since _ is any one character -- and the candidates still
// come from the string index, narrowed on the pattern's leading literal run
// "a": the budget is far too small to have walked the User bitmap.
func TestParenthesisedLikeAnchorNarrowsOnALiteralRun(t *testing.T) {
	const kiUser snapshot.KindID = 1
	var nodes []execNodeSpec
	for i := 0; i < 4000; i++ {
		nodes = append(nodes, execNodeSpec{uint64(i + 1), []snapshot.KindID{kiUser},
			map[string]any{"name": fmt.Sprintf("user%04d", i)}})
	}
	nodes = append(nodes,
		execNodeSpec{5001, []snapshot.KindID{kiUser}, map[string]any{"name": "a_b1"}},
		execNodeSpec{5002, []snapshot.KindID{kiUser}, map[string]any{"name": "aXb2"}},
		execNodeSpec{5003, []snapshot.KindID{kiUser}, map[string]any{"name": "ab3"}},
	)
	snap := buildExecSnapshot(t, map[snapshot.KindID]string{kiUser: "User"}, nodes, nil)
	tight := Budgets{MaxRows: 100, MaxWork: 200, MaxLiveRows: 1000}

	rs := mustExec(t, snap, `MATCH (n:User) WHERE (n.name) STARTS WITH 'a_b' RETURN n`, tight)
	if len(rs.Rows) != 2 {
		t.Fatalf("got %d rows, want 2 (a_b1 and aXb2)", len(rs.Rows))
	}
}
