// SPDX-License-Identifier: Apache-2.0

package interpret

import (
	"encoding/json"
	"errors"
	"testing"
)

// TestCastPropertyForOrderBoundsAtTwoPow53 pins castPropertyForOrder's two
// different 2^53 bounds as deliberate, so a later tidy-up cannot "make them
// consistent" by widening the number branch -- which would turn a safe
// decline into a wrong answer.
//
// jsonb keeps an integer exactly and `->>` hands PostgreSQL its exact text,
// so pg's `(properties ->> 'x')::int8` separates 9007199254740992 from
// 9007199254740993. A stored NUMBER reaches this package only after
// json.Unmarshal has rounded it to a float64, where both texts collapse onto
// 2^53 (asserted below, not assumed), so the integer cast must decline from
// 2^53 up. A stored STRING is never rounded: 2^53 parses exactly and is
// served, and only 2^53+1 loses a digit and declines.
func TestCastPropertyForOrderBoundsAtTwoPow53(t *testing.T) {
	// The premise: the decode this package's stored numbers go through
	// cannot tell 2^53 from 2^53+1.
	var bag map[string]any
	if err := json.Unmarshal([]byte(`{"a":9007199254740992,"b":9007199254740993}`), &bag); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if bag["a"] != bag["b"] {
		t.Fatalf("decoded %v and %v separately; the number branch's bound rests on them collapsing", bag["a"], bag["b"])
	}

	for name, tc := range map[string]struct {
		v       any
		want    any
		wantErr error
	}{
		"stored number below 2^53":         {float64(maxExactInt - 1), float64(maxExactInt - 1), nil},
		"stored number at 2^53 declines":   {float64(maxExactInt), nil, ErrRuntimeCast},
		"stored string below 2^53":         {"9007199254740991", float64(maxExactInt - 1), nil},
		"stored string at 2^53 is served":  {"9007199254740992", float64(maxExactInt), nil},
		"stored string past 2^53 declines": {"9007199254740993", nil, ErrRuntimeCast},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := castPropertyForOrder(tc.v, false)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("castPropertyForOrder(%#v) = (%v, %v), want %v", tc.v, got, err, tc.wantErr)
				}
				return
			}
			if err != nil || got != tc.want {
				t.Fatalf("castPropertyForOrder(%#v) = (%v, %v), want (%v, nil)", tc.v, got, err, tc.want)
			}
		})
	}
}
