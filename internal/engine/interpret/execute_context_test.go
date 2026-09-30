// SPDX-License-Identifier: Apache-2.0

package interpret

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

// executeContextFixture: 3000 users, enough that scanning them spends past
// the work meter's 1024-unit check interval several times over.
func executeContextFixture(t *testing.T) *snapshot.View {
	t.Helper()
	const kUser snapshot.KindID = 1
	nodes := make([]execNodeSpec, 0, 3000)
	for i := 0; i < 3000; i++ {
		nodes = append(nodes, execNodeSpec{uint64(1 + i), []snapshot.KindID{kUser}, map[string]any{"v": float64(i)}})
	}
	return buildExecSnapshot(t, map[snapshot.KindID]string{kUser: "User"}, nodes, nil)
}

// cancelledAfterContext reports itself live for its first `live` Err calls
// and cancelled from then on: a request cancelled while its query runs, at
// a point no timing can race.
type cancelledAfterContext struct {
	context.Context
	live  int
	calls int
}

func (c *cancelledAfterContext) Err() error {
	c.calls++
	if c.calls > c.live {
		return context.Canceled
	}
	return nil
}

// TestExecuteHonoursRequestContext: a served read runs under the request's
// context. Execute took none, so a request already cancelled -- or one
// cancelled while its query ran, for as long as the budgets allowed -- was
// computed in full and returned with a nil error, where PostgreSQL answers
// the same context with its error. A done context now fails the query on
// entry, and a cancellation mid-query at the next work-budget check.
func TestExecuteHonoursRequestContext(t *testing.T) {
	snap := executeContextFixture(t)
	q := planQuery(t, snap, `MATCH (u:User) WHERE u.v >= 0 RETURN u`)

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	expired, cancelExpired := context.WithDeadline(context.Background(), time.Now().Add(-time.Minute))
	defer cancelExpired()

	for name, tc := range map[string]struct {
		ctx  context.Context
		want error
	}{
		"cancelled before the query":  {cancelled, context.Canceled},
		"deadline passed":             {expired, context.DeadlineExceeded},
		"cancelled while it runs":     {&cancelledAfterContext{Context: context.Background(), live: 1}, context.Canceled},
		"live context (served whole)": {context.Background(), nil},
		"no context (served whole)":   {nil, nil},
	} {
		t.Run(name, func(t *testing.T) {
			rs, err := Execute(&Env{Snap: snap, Ctx: tc.ctx}, q, Budgets{MaxRows: 100_000, MaxWork: 1 << 28})
			if tc.want == nil {
				if err != nil {
					t.Fatalf("Execute: %v", err)
				}
				if len(rs.Rows) != 3000 {
					t.Fatalf("got %d rows, want 3000", len(rs.Rows))
				}
				return
			}
			if !errors.Is(err, tc.want) || rs != nil {
				t.Fatalf("Execute = (%v rows, %v), want (nil, %v)", executeContextRowCount(rs), err, tc.want)
			}
		})
	}
}

func executeContextRowCount(rs *ResultSet) any {
	if rs == nil {
		return nil
	}
	return len(rs.Rows)
}
