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
	Calls    []string
	Outputs  map[string][]byte
	Errors   map[string]error
	Prefixes map[string][]byte // fallback when no exact Outputs/Errors key matches
	// Sequences answers successive calls whose command line starts with the
	// key with successive entries, so a test can script a value that changes
	// as the code under test progresses (a row count before and after a
	// migration, say). It is consulted first; once a key's entries run out,
	// Errors, Outputs and Prefixes answer as usual.
	Sequences map[string][][]byte
}

func (s *FakeRunner) Run(_ context.Context, _ io.Reader, name string, args ...string) ([]byte, error) {
	line := strings.Join(append([]string{name}, args...), " ")
	s.Calls = append(s.Calls, line)
	if key := longestPrefix(line, s.Sequences); key != "" {
		out := s.Sequences[key][0]
		s.Sequences[key] = s.Sequences[key][1:]
		return out, nil
	}
	if err, ok := s.Errors[line]; ok {
		return nil, err
	}
	if out, ok := s.Outputs[line]; ok {
		return out, nil
	}
	best := ""
	for prefix := range s.Prefixes {
		if strings.HasPrefix(line, prefix) && len(prefix) > len(best) {
			best = prefix
		}
	}
	if best != "" {
		return s.Prefixes[best], nil
	}
	return nil, fmt.Errorf("fake runner: unscripted command %q", line)
}

// longestPrefix returns the longest key of seqs that line starts with and that
// still has an entry left, or "" when there is none.
func longestPrefix(line string, seqs map[string][][]byte) string {
	best := ""
	for prefix, remaining := range seqs {
		if len(remaining) > 0 && strings.HasPrefix(line, prefix) && len(prefix) > len(best) {
			best = prefix
		}
	}
	return best
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
