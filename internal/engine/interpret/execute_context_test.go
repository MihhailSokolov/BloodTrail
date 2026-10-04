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

// TestWorkMeterChecksTheContextOnItsOwnCadence pins the PERIODIC half of the
// context check, which TestExecuteHonoursRequestContext above cannot see.
//
// That test asserts only the error a cancelled request comes back with, and
// check() is called twice for different reasons: every 1024 work units from
// spend, and once unconditionally before Execute returns a successful
// ResultSet. The second call alone produces the same context.Canceled for the
// same query, so the test passes unchanged even if spend never consults the
// context at all -- it would merely mean a cancelled request is computed in
// full and then answered with the cancellation, which is the behaviour the
// context plumbing exists to prevent.
//
// What distinguishes the two is WHERE the work counter stops. runQuery is
// driven directly here, with a meter this test owns, so the counter can be
// read after the error: a query cancelled from its first check must stop at
// the first 1024-unit boundary, not at the full cost of materializing every
// row.
func TestWorkMeterChecksTheContextOnItsOwnCadence(t *testing.T) {
	snap := executeContextFixture(t)
	env := &Env{Snap: snap}
	q := planQuery(t, snap, `MATCH (u:User) WHERE u.v >= 0 RETURN u`)
	budget := Budgets{MaxRows: 100_000, MaxWork: 1 << 28}

	// What the whole query costs when nothing interrupts it -- the number the
	// cancelled run below must NOT reach.
	whole := &workMeter{budget: budget}
	rs, err := runQuery(env, q, whole)
	if err != nil {
		t.Fatalf("runQuery on a live context: %v", err)
	}
	if len(rs.Rows) != 3000 {
		t.Fatalf("runQuery served %d rows, want 3000", len(rs.Rows))
	}
	if whole.work <= 2*1024 {
		t.Fatalf("the whole query costs %d work units, which is not enough above the 1024-unit check interval for this test to tell the two checks apart", whole.work)
	}

	// Cancelled from the very first Err call, so the first periodic check is
	// the one that fails -- no timing involved.
	ctx := &cancelledAfterContext{Context: context.Background(), live: 0}
	cancelled := &workMeter{budget: budget, ctx: ctx}
	if rs, err := runQuery(env, q, cancelled); !errors.Is(err, context.Canceled) || rs != nil {
		t.Fatalf("runQuery = (%v rows, %v), want (nil, context.Canceled)", executeContextRowCount(rs), err)
	}
	if ctx.calls != 1 {
		t.Errorf("the context was consulted %d time(s), want exactly 1: the first periodic check must be the one that fails", ctx.calls)
	}
	// spend charges in small increments, so the boundary is crossed at or
	// just past 1024; anything at the next boundary or beyond means the
	// periodic check did not fire when it was due.
	if cancelled.work < 1024 || cancelled.work >= 2*1024 {
		t.Errorf("the cancelled query spent %d work units, want the first 1024-unit boundary (the whole query costs %d): spend is not checking the context on its own cadence",
			cancelled.work, whole.work)
	}
}
