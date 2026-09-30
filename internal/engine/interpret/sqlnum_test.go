// SPDX-License-Identifier: Apache-2.0

package interpret

import (
	"errors"
	"testing"

	"github.com/specterops/dawgs/cypher/models/cypher"
)

// TestIntegerArithmeticFollowsPostgresWidths pins integerArithmetic to
// PostgreSQL's integer operators: int4 overflows at 2^31, and a result past
// the 2^53 float64 holds exactly declines at any width.
func TestIntegerArithmeticFollowsPostgresWidths(t *testing.T) {
	for _, tc := range []struct {
		name    string
		typ     sqlNum
		a       float64
		op      cypher.Operator
		b       float64
		want    float64
		wantErr error
	}{
		{"int4 sum", sqlNumInt4, 2147483646, cypher.OperatorAdd, 1, 2147483647, nil},
		{"int4 sum overflows", sqlNumInt4, 2147483647, cypher.OperatorAdd, 1, 0, ErrRuntimeCast},
		{"int4 difference overflows", sqlNumInt4, -2147483648, cypher.OperatorSubtract, 1, 0, ErrRuntimeCast},
		{"int4 product overflows", sqlNumInt4, 31536000, cypher.OperatorMultiply, 1000, 0, ErrRuntimeCast},
		{"int4 huge product overflows", sqlNumInt4, 9007199254740992, cypher.OperatorMultiply, 9007199254740992, 0, ErrRuntimeCast},
		{"int8 product", sqlNumInt8, 31536000, cypher.OperatorMultiply, 1000, 31536000000, nil},
		{"int8 past 2^53 declines", sqlNumInt8, 9007199254740992, cypher.OperatorAdd, 1, 0, ErrUnsupported},
		{"numeric past 2^53 declines", sqlNumNumeric, 9007199254740990, cypher.OperatorAdd, 9, 0, ErrUnsupported},
		{"numeric fraction declines", sqlNumNumeric, 1.5, cypher.OperatorAdd, 1, 0, ErrUnsupported},
		{"int4 division truncates", sqlNumInt4, -7, cypher.OperatorDivide, 2, -3, nil},
		{"int4 modulo keeps the dividend's sign", sqlNumInt4, -7, cypher.OperatorModulo, 2, -1, nil},
		{"int4 division by zero", sqlNumInt4, 1, cypher.OperatorDivide, 0, 0, ErrRuntimeCast},
		{"int4 min over minus one overflows", sqlNumInt4, -2147483648, cypher.OperatorDivide, -1, 0, ErrRuntimeCast},
		{"numeric division declines", sqlNumNumeric, 7, cypher.OperatorDivide, 2, 0, ErrUnsupported},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := integerArithmetic(tc.typ, tc.a, tc.op, tc.b)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("got %v, %v; want error %v", got, err, tc.wantErr)
				}
				return
			}
			if err != nil || got != tc.want {
				t.Fatalf("got %v, %v; want %v", got, err, tc.want)
			}
		})
	}
}

// TestIntegerLiteralTyping pins how PostgreSQL types the integer constants
// dawgs prints: int4 when the value fits, int8 otherwise.
func TestIntegerLiteralTyping(t *testing.T) {
	for _, tc := range []struct {
		value any
		want  sqlNum
	}{
		{int64(0), sqlNumInt4},
		{int64(2147483647), sqlNumInt4},
		{int64(2147483648), sqlNumInt8},
		{int64(-2147483648), sqlNumInt4},
		{uint64(3000000000), sqlNumInt8},
	} {
		if got := literalSQLNum(&cypher.Literal{Value: tc.value}); got != tc.want {
			t.Errorf("literalSQLNum(%v) = %v, want %v", tc.value, got, tc.want)
		}
	}
}

// TestEvalIntegerLiteralPastExactRangeDeclines checks a literal float64
// cannot hold exactly declines rather than rounding onto a neighbour.
func TestEvalIntegerLiteralPastExactRangeDeclines(t *testing.T) {
	for _, v := range []any{int64(9007199254740993), int64(-9007199254740993), uint64(9007199254740993)} {
		if _, _, err := evalLiteralValue(&cypher.Literal{Value: v}); !errors.Is(err, ErrUnsupported) {
			t.Errorf("evalLiteralValue(%v) error = %v, want ErrUnsupported", v, err)
		}
	}
	if got, ok, err := evalLiteralValue(&cypher.Literal{Value: int64(9007199254740992)}); err != nil || !ok || got != float64(9007199254740992) {
		t.Errorf("evalLiteralValue(2^53) = %v, %v, %v; want the exact value", got, ok, err)
	}
}
