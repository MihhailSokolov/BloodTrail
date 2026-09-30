// SPDX-License-Identifier: Apache-2.0

package interpret

import (
	"math"
	"strconv"
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
// is printedFloatSQLNum's -- never float8.
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
		return printedFloatSQLNum(v)
	}
	return sqlNumNone
}

// printedFloatSQLNum is the type PostgreSQL gives a float literal as dawgs
// prints it, FormatFloat(v, 'f', -1, 64) (format/format.go): with a fraction
// it is an exact numeric constant (`0.1`); without one it is an integer
// constant (`2.0` prints as `2`, an int4), or numeric past int8.
func printedFloatSQLNum(v float64) sqlNum {
	text := strconv.FormatFloat(v, 'f', -1, 64)
	if strings.ContainsRune(text, '.') {
		return sqlNumNumeric
	}
	n, err := strconv.ParseInt(text, 10, 64)
	if err != nil {
		return sqlNumNumeric
	}
	return integerSQLNum(n)
}

// fractionalLiteral reports whether expr -- parentheses, signs and
// operator-less arithmetic aside -- is a float literal dawgs prints with a
// fraction, which PostgreSQL computes with as an exact numeric.
func fractionalLiteral(expr cypher.Expression) bool {
	for {
		expr = unwrapBareArithmetic(expr)
		u, isSign := expr.(*cypher.UnaryAddOrSubtractExpression)
		if !isSign || u == nil {
			break
		}
		expr = u.Right
	}
	lit, ok := expr.(*cypher.Literal)
	if !ok || lit == nil || lit.Null {
		return false
	}
	f, isFloat := lit.Value.(float64)
	return isFloat && f != math.Trunc(f)
}

// valueSQLNum is the type of a WITH constant's column, which dawgs projects
// as the literal itself (`select 60 as i0`, `select 5 as i0` for 5.0): a
// number's is printedFloatSQLNum's. A COUNT alias is an int8 column; typing
// its (whole) value by its size instead is conservative -- int4 overflows
// sooner, and so declines sooner, than int8.
func valueSQLNum(v any) sqlNum {
	f, isNum := v.(float64)
	if !isNum {
		return sqlNumNone
	}
	return printedFloatSQLNum(f)
}

// numericStepServed reports whether the evaluator reproduces an arithmetic
// step PostgreSQL computes at type t, before any row is read: `%` never
// (float8 has no `%` at all -- `double precision % integer` is an error --
// and numeric's is exact decimal), `/` only in float8 (integer division
// truncates, numeric's is exact decimal), and nothing numeric over a
// fractional literal (`0.1 + 0.2 = 0.3` holds exactly there).
func numericStepServed(op cypher.Operator, t sqlNum, fractionalOperand bool) bool {
	switch {
	case op == cypher.OperatorModulo:
		return false
	case op == cypher.OperatorDivide && t != sqlNumFloat8:
		return false
	case t == sqlNumNumeric && fractionalOperand:
		return false
	}
	return true
}

// float8Arithmetic computes one step as PostgreSQL's float8 operators do.
// They raise an error where IEEE arithmetic quietly overflows to an
// infinity ("value out of range: overflow"), where a product or quotient of
// non-zero operands underflows to zero ("value out of range: underflow"),
// and on a zero divisor; and float8 has no `%`.
func float8Arithmetic(a float64, op cypher.Operator, b float64) (float64, error) {
	var r float64
	switch op {
	case cypher.OperatorAdd:
		r = a + b
	case cypher.OperatorSubtract:
		r = a - b
	case cypher.OperatorMultiply:
		r = a * b
		if r == 0 && a != 0 && b != 0 {
			return 0, ErrRuntimeCast
		}
	case cypher.OperatorDivide:
		if b == 0 {
			return 0, ErrRuntimeCast
		}
		r = a / b
		if r == 0 && a != 0 {
			return 0, ErrRuntimeCast
		}
	default:
		return 0, ErrUnsupported
	}
	if math.IsInf(r, 0) {
		return 0, ErrRuntimeCast
	}
	return r, nil
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

// planOperandSQLNum is an arithmetic operand's PostgreSQL type as far as Plan
// can know it. A WITH alias holds an integer or a numeric whose width only
// its value tells (valueSQLNum), but never a float8, which is all Plan asks
// of it -- it counts as int8 here.
func planOperandSQLNum(expr cypher.Expression) sqlNum {
	switch e := unwrapParens(expr).(type) {
	case *cypher.ArithmeticExpression:
		if e != nil {
			t, _ := foldArithSQLNum(e, nil)
			return t
		}
	case *cypher.UnaryAddOrSubtractExpression:
		if e != nil {
			return planOperandSQLNum(e.Right)
		}
	case *cypher.Variable:
		return sqlNumInt8
	}
	return leafSQLNum(expr)
}

// foldArithSQLNum folds the PostgreSQL types of ae's steps the way
// evalArithmeticTyped computes them -- a plain property operand of a numeric
// step takes the cast its partner gives it, a concatenation has no numeric
// type -- calling step, when non-nil, with each numeric step's operator, type
// and whether one of its operands is a fractional literal. It stops,
// reporting false, at a step step refuses; otherwise it returns ae's type.
func foldArithSQLNum(ae *cypher.ArithmeticExpression, step func(op cypher.Operator, t sqlNum, fractionalOperand bool) bool) (sqlNum, bool) {
	cur := planOperandSQLNum(ae.Left)
	curKind := classifyAddOperand(ae.Left)
	leftFloat := staticallyFloatOperand(ae.Left)
	for i, p := range ae.Partials {
		if p == nil {
			return sqlNumNone, false
		}
		r := planOperandSQLNum(p.Right)
		rKind := classifyAddOperand(p.Right)
		rightFloat := staticallyFloatOperand(p.Right)
		numeric := p.Operator != cypher.OperatorAdd ||
			(curKind != addStaticText && rKind != addStaticText && (curKind != addPropertyLookup || rKind != addPropertyLookup))
		if numeric {
			if i == 0 && isPlainPropertyLookup(ae.Left) {
				cur = castSQLNum(rightFloat)
			}
			if isPlainPropertyLookup(p.Right) {
				r = castSQLNum(leftFloat)
			}
			cur = arithSQLNum(cur, r)
			if step != nil && !step(p.Operator, cur, (i == 0 && fractionalLiteral(ae.Left)) || fractionalLiteral(p.Right)) {
				return sqlNumNone, false
			}
		} else {
			cur = sqlNumNone
		}
		curKind = nextAddKind(p.Operator, curKind, rKind)
		leftFloat = leftFloat || rightFloat
	}
	return cur, true
}
