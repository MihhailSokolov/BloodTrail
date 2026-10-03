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

// TestFloatLiteralTyping pins how PostgreSQL types a float literal as dawgs
// prints it (FormatFloat 'f'): a fraction is an exact numeric, a whole
// number an integer constant, and numeric past int8.
func TestFloatLiteralTyping(t *testing.T) {
	for _, tc := range []struct {
		value float64
		want  sqlNum
	}{
		{2.0, sqlNumInt4},
		{0.0, sqlNumInt4},
		{0.1, sqlNumNumeric},
		{-2.5, sqlNumNumeric},
		{3e9, sqlNumInt8},
		{1e20, sqlNumNumeric},
	} {
		if got := literalSQLNum(&cypher.Literal{Value: tc.value}); got != tc.want {
			t.Errorf("literalSQLNum(%v) = %v, want %v", tc.value, got, tc.want)
		}
	}
}

// TestNumericStepServed pins which arithmetic steps Plan admits by their
// PostgreSQL type.
func TestNumericStepServed(t *testing.T) {
	for _, tc := range []struct {
		op         cypher.Operator
		typ        sqlNum
		fractional bool
		want       bool
	}{
		{cypher.OperatorDivide, sqlNumFloat8, false, true},
		{cypher.OperatorDivide, sqlNumInt4, false, false},
		{cypher.OperatorDivide, sqlNumNumeric, false, false},
		{cypher.OperatorModulo, sqlNumFloat8, false, false},
		{cypher.OperatorModulo, sqlNumInt8, false, false},
		{cypher.OperatorAdd, sqlNumNumeric, true, false},
		{cypher.OperatorAdd, sqlNumNumeric, false, true},
		{cypher.OperatorMultiply, sqlNumFloat8, true, true},
		{cypher.OperatorMultiply, sqlNumInt4, false, true},
	} {
		if got := numericStepServed(tc.op, tc.typ, tc.fractional); got != tc.want {
			t.Errorf("numericStepServed(%v, %v, %v) = %v, want %v", tc.op, tc.typ, tc.fractional, got, tc.want)
		}
	}
}

// TestFloat8ArithmeticRaisesPostgresErrors pins float8Arithmetic to
// PostgreSQL's float8 operators: overflow, underflow of a product or
// quotient, and a zero divisor are errors, not Inf or 0.
func TestFloat8ArithmeticRaisesPostgresErrors(t *testing.T) {
	for _, tc := range []struct {
		a  float64
		op cypher.Operator
		b  float64
	}{
		{1e308, cypher.OperatorMultiply, 10},
		{1e308, cypher.OperatorAdd, 1e308},
		{1e-300, cypher.OperatorMultiply, 1e-300},
		{1e-300, cypher.OperatorDivide, 1e300},
		{1, cypher.OperatorDivide, 0},
	} {
		if r, err := float8Arithmetic(tc.a, tc.op, tc.b); !errors.Is(err, ErrRuntimeCast) {
			t.Errorf("float8Arithmetic(%v %v %v) = %v, %v; want ErrRuntimeCast", tc.a, tc.op, tc.b, r, err)
		}
	}
	if r, err := float8Arithmetic(0, cypher.OperatorMultiply, 1e-300); err != nil || r != 0 {
		t.Errorf("0 * 1e-300 = %v, %v; want an exact 0", r, err)
	}
	if _, err := float8Arithmetic(7, cypher.OperatorModulo, 2); !errors.Is(err, ErrUnsupported) {
		t.Errorf("float8 %% error = %v, want ErrUnsupported", err)
	}
}

// TestPropertyCastFollowsPartnerHint pins the cast dawgs gives a plain
// property from the type it infers for the property's partner: int next to
// size(), int8 next to an integer or id(), float8 next to a (signed) float
// literal, numeric next to datetime(), and none next to a WITH alias or a
// float mixed with an integer, which dawgs cannot type.
func TestPropertyCastFollowsPartnerHint(t *testing.T) {
	for _, tc := range []struct {
		partner string
		want    sqlNum
		cast    bool
	}{
		{"size(n.l)", sqlNumInt4, true},
		{"size(n.l) + size(n.l)", sqlNumInt4, true},
		{"size(n.l) * 2", sqlNumInt8, true},
		{"2", sqlNumInt8, true},
		{"id(n)", sqlNumInt8, true},
		{"-2.5", sqlNumFloat8, true},
		{"2.0", sqlNumFloat8, true},
		{"datetime().epochseconds - 86400", sqlNumNumeric, true},
		{"datetime().epochseconds * 1.5", sqlNumNumeric, true},
		{"d", sqlNumNone, false},
		{"d * 1", sqlNumInt8, true},
		{"1.5 * 2", sqlNumNone, false},
		{"size(n.l) + 2.5", sqlNumNone, false},
		{"'a'", sqlNumNone, false},
	} {
		expr := returnExprOf(t, "MATCH (n) RETURN "+tc.partner)
		_, h := operandTyping(expr)
		got, ok := hintCast(h)
		if got != tc.want || ok != tc.cast {
			t.Errorf("cast next to %s = %v, %v; want %v, %v", tc.partner, got, ok, tc.want, tc.cast)
		}
	}
}

// TestCastPropertyAsInt4Range checks the int cast dawgs puts next to size()
// declines a value outside int4, as PostgreSQL's cast raises an error.
func TestCastPropertyAsInt4Range(t *testing.T) {
	for _, v := range []any{3000000000.0, "3000000000", -2147483649.0} {
		if _, err := castPropertyAs(v, sqlNumInt4); !errors.Is(err, ErrRuntimeCast) {
			t.Errorf("castPropertyAs(%v, int4) error = %v, want ErrRuntimeCast", v, err)
		}
	}
	if got, err := castPropertyAs("2147483647", sqlNumInt4); err != nil || got != float64(2147483647) {
		t.Errorf("castPropertyAs('2147483647', int4) = %v, %v", got, err)
	}
}

// TestNegatedLiteralTyping pins the type PostgreSQL gives a minus sign over
// a number literal: its grammar folds the sign into the constant, so the type
// follows the negated value. A plus sign, and a sign over anything else,
// keeps the operand's type.
func TestNegatedLiteralTyping(t *testing.T) {
	for _, tc := range []struct {
		expr string
		want sqlNum
	}{
		{"-2147483648", sqlNumInt4},
		{"-(2147483648)", sqlNumInt4},
		{"-2147483649", sqlNumInt8},
		{"-2147483648.0", sqlNumInt4},
		{"-2.5", sqlNumNumeric},
		{"-5", sqlNumInt4},
		{"+2147483648", sqlNumInt8},
		{"-size(n.l)", sqlNumInt4},
		{"-id(n)", sqlNumInt8},
	} {
		got, _ := operandTyping(returnExprOf(t, "MATCH (n) RETURN "+tc.expr))
		if got != tc.want {
			t.Errorf("type of %s = %v, want %v", tc.expr, got, tc.want)
		}
	}
}
