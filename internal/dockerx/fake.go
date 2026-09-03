// SPDX-License-Identifier: Apache-2.0

package dockerx

import (
	"context"
	"fmt"
	"io"
	"strings"
)

// FakeRunner scripts command outputs for tests. Keys are the full command line
// joined by single spaces, e.g. "docker compose -f x up -d".
type FakeRunner struct {
	Calls   []string
	Outputs map[string][]byte
	Errors  map[string]error
}

func (s *FakeRunner) Run(_ context.Context, _ io.Reader, name string, args ...string) ([]byte, error) {
	line := strings.Join(append([]string{name}, args...), " ")
	s.Calls = append(s.Calls, line)
	if err, ok := s.Errors[line]; ok {
		return nil, err
	}
	if out, ok := s.Outputs[line]; ok {
		return out, nil
	}
	return nil, fmt.Errorf("fake runner: unscripted command %q", line)
}

// Called reports whether any recorded call starts with prefix.
func (s *FakeRunner) Called(prefix string) bool {
	for _, c := range s.Calls {
		if strings.HasPrefix(c, prefix) {
			return true
		}
	}
	return false
}
