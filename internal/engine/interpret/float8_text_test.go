// SPDX-License-Identifier: Apache-2.0

package interpret

import (
	"errors"
	"testing"
)

// TestParseFloat8TextRejectsUnderflow pins parseFloat8Text to float8in: a
// spelling whose non-zero mantissa rounds to zero is out of range there,
// while a denormal and a zero spelled with an exponent are accepted.
func TestParseFloat8TextRejectsUnderflow(t *testing.T) {
	for _, tc := range []struct {
		text string
		want float64
		fail bool
	}{
		{text: "1e-400", fail: true},
		{text: "-1e-400", fail: true},
		{text: "2e-324", fail: true},
		{text: "0.0000001e-330", fail: true},
		{text: "1e400", fail: true},
		{text: "1e-310", want: 1e-310},
		{text: "2.5e-324", want: 5e-324},
		{text: "0e-400", want: 0},
		{text: "-0.0", want: 0},
		{text: "1.5", want: 1.5},
	} {
		got, err := parseFloat8Text(tc.text)
		if tc.fail {
			if !errors.Is(err, ErrRuntimeCast) {
				t.Errorf("parseFloat8Text(%q) = %v, %v; want ErrRuntimeCast", tc.text, got, err)
			}
			continue
		}
		if err != nil || got != tc.want {
			t.Errorf("parseFloat8Text(%q) = %v, %v; want %v", tc.text, got, err, tc.want)
		}
	}
}

// TestCastsDeclineFloat8Underflow checks every float8 text cast the
// evaluator performs -- a property against a float operand, a float
// coalesce(), and In over a numeric list -- declines an underflowing string.
func TestCastsDeclineFloat8Underflow(t *testing.T) {
	if _, err := castPropertyForOrder("1e-400", true); !errors.Is(err, ErrRuntimeCast) {
		t.Errorf("castPropertyForOrder('1e-400', float8) error = %v, want ErrRuntimeCast", err)
	}
	if _, err := castTextAs("1e-400", coalesceFloat8); !errors.Is(err, ErrRuntimeCast) {
		t.Errorf("castTextAs('1e-400', float8) error = %v, want ErrRuntimeCast", err)
	}
	if _, err := In("1e-400", true, []any{0.0, 1.5}); !errors.Is(err, ErrRuntimeCast) {
		t.Errorf("In('1e-400', [0, 1.5]) error = %v, want ErrRuntimeCast", err)
	}
}
