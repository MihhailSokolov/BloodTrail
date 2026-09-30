// SPDX-License-Identifier: Apache-2.0

package interpret

import (
	"math"
	"strings"

	"github.com/specterops/dawgs/cypher/models/cypher"
)

// sqlnum.go models the PostgreSQL type a number has in the SQL dawgs emits,
// which is what decides how PostgreSQL computes with it. This package keeps
// every number as a float64; that is the right model only where PostgreSQL
// works in double precision too. Elsewhere it works in integers, which
// overflow into an error at their width, or in exact decimals -- and an
// arithmetic step is evaluated here with the semantics of its PostgreSQL
// type, or declines.

// sqlNum is a numeric operand's PostgreSQL type.
type sqlNum uint8

const (
	// sqlNumNone is an operand with no numeric type of its own here: a plain
	// property (its type is the cast dawgs gives it from its partner) or
	// anything that is not a number.
	sqlNumNone sqlNum = iota
	sqlNumInt4
	sqlNumInt8
	sqlNumNumeric
	sqlNumFloat8
)

const (
	minInt4 = math.MinInt32
	maxInt4 = math.MaxInt32
)

// arithSQLNum is the type PostgreSQL resolves a binary arithmetic operator
// over a and b to: float8 absorbs everything, then numeric, then int8; two
// int4 operands stay int4 (and overflow as int4).
func arithSQLNum(a, b sqlNum) sqlNum {
	switch {
	case a == sqlNumNone || b == sqlNumNone:
		return sqlNumNone
	case a == sqlNumFloat8 || b == sqlNumFloat8:
		return sqlNumFloat8
	case a == sqlNumNumeric || b == sqlNumNumeric:
		return sqlNumNumeric
	case a == sqlNumInt8 || b == sqlNumInt8:
		return sqlNumInt8
	default:
		return sqlNumInt4
	}
}

// integerSQLNum is the type PostgreSQL gives an integer constant written
// out in SQL: int4 when it fits, int8 otherwise.
func integerSQLNum(v int64) sqlNum {
	if v >= minInt4 && v <= maxInt4 {
		return sqlNumInt4
	}
	return sqlNumInt8
}

// literalSQLNum is the type of the SQL constant dawgs prints for lit. An
// integer is printed as written, so it is integerSQLNum's; a float literal
// is taken as float8.
func literalSQLNum(lit *cypher.Literal) sqlNum {
	if lit == nil || lit.Null {
		return sqlNumNone
	}
	switch v := lit.Value.(type) {
	case int64:
		return integerSQLNum(v)
	case uint64:
		if v > math.MaxInt64 {
			return sqlNumNumeric
		}
		return integerSQLNum(int64(v))
	case float64:
		return sqlNumFloat8
	}
	return sqlNumNone
}

// valueSQLNum is the type of a WITH constant's column, which dawgs projects
// as the literal itself (`select 60 as i0`): integerSQLNum's for a whole
// number. A COUNT alias is an int8 column; typing its (whole) value by its
// size instead is conservative -- int4 overflows sooner, and so declines
// sooner, than int8.
func valueSQLNum(v any) sqlNum {
	f, isNum := v.(float64)
	if !isNum {
		return sqlNumNone
	}
	if f == math.Trunc(f) && math.Abs(f) <= maxExactInt {
		return integerSQLNum(int64(f))
	}
	return sqlNumFloat8
}

// leafSQLNum is the type of an arithmetic operand that is not itself
// arithmetic, a sign or a variable: a literal, or a call whose SQL result
// type dawgs fixes -- size() is `::int`, id() the bigint id column,
// datetime()'s epoch accessors `::numeric`, a coalesce() the type of its
// literal. A plain property has none: its type is the cast dawgs gives it.
func leafSQLNum(expr cypher.Expression) sqlNum {
	switch e := unwrapParens(expr).(type) {
	case *cypher.Literal:
		return literalSQLNum(e)
	case *cypher.PropertyLookup:
		if e != nil && !propertyRead(e) {
			return sqlNumNumeric
		}
	case *cypher.FunctionInvocation:
		if e == nil {
			return sqlNumNone
		}
		switch strings.ToLower(e.Name) {
		case cypher.ListSizeFunction:
			return sqlNumInt4
		case cypher.IdentityFunction:
			return sqlNumInt8
		case cypher.CoalesceFunction:
			switch kind, ok := coalesceCastKind(e); {
			case !ok:
			case kind == coalesceInt8:
				return sqlNumInt8
			case kind == coalesceFloat8:
				return sqlNumFloat8
			}
		}
	}
	return sqlNumNone
}

// exactInteger returns f as an int64 when it is a whole number the float64
// model holds exactly (|f| <= 2^53).
func exactInteger(f float64) (int64, bool) {
	if f != math.Trunc(f) || math.Abs(f) > maxExactInt {
		return 0, false
	}
	return int64(f), true
}

// integerArithmetic computes one step over two whole numbers the way
// PostgreSQL computes it at type t -- int4, int8 or numeric -- and declines
// what it cannot reproduce: an int4 result outside int4 is PostgreSQL's
// "integer out of range" (ErrRuntimeCast), and a result the float64 model
// cannot hold exactly (past 2^53) declines too (ErrUnsupported) -- as an
// int8 PostgreSQL computes it, or raises "bigint out of range", either way
// not something to round. An operand that is not a whole number is a numeric
// fraction, computed exactly there; it declines here. Integer `/` and `%`
// truncate toward zero, as Go's do, and a zero divisor is PostgreSQL's
// "division by zero"; numeric division is exact, so it declines.
func integerArithmetic(t sqlNum, a float64, op cypher.Operator, b float64) (float64, error) {
	ai, aok := exactInteger(a)
	bi, bok := exactInteger(b)
	if !aok || !bok {
		return 0, ErrUnsupported
	}
	var r int64
	switch op {
	case cypher.OperatorAdd:
		r = ai + bi
	case cypher.OperatorSubtract:
		r = ai - bi
	case cypher.OperatorMultiply:
		// |ai|, |bi| <= 2^53, so the product fits in a float64's range; any
		// product past 2^62 is out of every range handled below, and one
		// below it multiplies exactly in int64.
		if math.Abs(a*b) > 1<<62 {
			if t == sqlNumInt4 {
				return 0, ErrRuntimeCast
			}
			return 0, ErrUnsupported
		}
		r = ai * bi
	case cypher.OperatorDivide, cypher.OperatorModulo:
		if t == sqlNumNumeric {
			return 0, ErrUnsupported
		}
		if bi == 0 {
			return 0, ErrRuntimeCast
		}
		if op == cypher.OperatorDivide {
			r = ai / bi
		} else {
			r = ai % bi
		}
	default:
		return 0, ErrUnsupported
	}
	if t == sqlNumInt4 && (r < minInt4 || r > maxInt4) {
		return 0, ErrRuntimeCast
	}
	if r > maxExactInt || r < -maxExactInt {
		return 0, ErrUnsupported
	}
	return float64(r), nil
}
