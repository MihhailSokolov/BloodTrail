// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"context"
	"strings"
	"testing"

	"golang.org/x/sync/errgroup"
)

// TestGoRecoveredReportsAPanicAsTheGroupsError pins loadNodes' panic
// contract on the wrapper itself: a panicking member of the group becomes
// the error Wait returns, carrying what panicked, the panic value and a
// stack, while the other members still run to completion and the process
// survives.
func TestGoRecoveredReportsAPanicAsTheGroupsError(t *testing.T) {
	g, _ := errgroup.WithContext(context.Background())

	ran := make(chan struct{})
	goRecovered(g, "staging nodes", func() error { panic("staging blew up") })
	goRecovered(g, "parsing node properties", func() error {
		close(ran)
		return nil
	})

	err := g.Wait()
	if err == nil {
		t.Fatalf("Wait returned no error for a panicking group member")
	}
	for _, want := range []string{"staging nodes", "staging blew up", "goRecovered"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("Wait error %q does not mention %q", err, want)
		}
	}
	select {
	case <-ran:
	default:
		t.Fatalf("the group's other member never ran")
	}
}

// TestParseLoadedPropsKeepsParseOutcomes pins that routing a parse worker's
// job through parseLoadedProps left ParseProps' own outcome alone: a sound
// property bag parses, an empty one is no properties, and a malformed one
// comes back as that node's error (which the consumer turns into the load's
// error) rather than being swallowed by the recover.
func TestParseLoadedPropsKeepsParseOutcomes(t *testing.T) {
	if res := parseLoadedProps([]byte(`{"name":"ok"}`)); res.err != nil {
		t.Fatalf("parseLoadedProps on a sound property bag: %v", res.err)
	}
	if res := parseLoadedProps(nil); res.err != nil {
		t.Fatalf("parseLoadedProps on no properties at all: %v", res.err)
	}
	if res := parseLoadedProps([]byte(`{"name":`)); res.err == nil {
		t.Fatalf("parseLoadedProps reported no error for malformed JSON")
	}
}
