// SPDX-License-Identifier: Apache-2.0

package interpret

import (
	"math"
	"strconv"
	"strings"

	"github.com/specterops/dawgs/cypher/models/cypher"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
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

// negatedLiteralSQLNum is the type PostgreSQL gives a minus sign over a
// number literal, which dawgs prints as `- 2147483648` and PostgreSQL's
// grammar folds into one constant (doNegate) before typing it: the type
// follows the negated value, so `- 2147483648` is the int4 -2147483648 and
// `- 2147483649` an int8, not a sign over the int8 the literal alone would
// be. false when u is not a minus sign over a number literal (parentheses
// aside, which fold too). A sign over a sign is not folded here: the inner
// one is, and the outer negates its int4 -- declining at the int4 minimum,
// where PostgreSQL's whole-chain fold would give the int8 2147483648.
func negatedLiteralSQLNum(u *cypher.UnaryAddOrSubtractExpression) (sqlNum, bool) {
	if u == nil || u.Operator != cypher.OperatorSubtract {
		return sqlNumNone, false
	}
	lit, ok := unwrapBareArithmetic(u.Right).(*cypher.Literal)
	if !ok || lit == nil || lit.Null {
		return sqlNumNone, false
	}
	switch v := lit.Value.(type) {
	case int64:
		return integerSQLNum(-v), true
	case uint64:
		if v > 1<<63 {
			return sqlNumNumeric, true
		}
		return integerSQLNum(-int64(v)), true
	case float64:
		return printedFloatSQLNum(-v), true
	}
	return sqlNumNone, false
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
//
// The `%` arm is defensive and unreachable from Plan today: numericStepServed
// refuses `%` at every type, because float8 has none and numeric's is exact
// decimal. It is kept rather than dropped because at int4/int8 -- the only
// types that reach it -- PostgreSQL's `%` is exactly Go's, so if that gate
// is ever narrowed to admit integer `%`, the arm is already right;
// TestIntegerArithmeticFollowsPostgresWidths pins its semantics so it
// cannot drift while unreachable.
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
	// OperatorModulo is the defensive arm; see this function's doc comment.
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

// dawgsHint is the type dawgs' translator infers for an expression
// (translate.InferExpressionType), which is what it casts a plain property
// operand to -- and not always PostgreSQL's own type for the expression: an
// integer literal is hinted int8 although PostgreSQL types the printed
// constant int4.
type dawgsHint uint8

const (
	// hintNone is an expression with no type of its own -- a plain
	// property, a WITH alias (a column reference), a coalesce() of
	// properties alone. A property partnered with one stays `->>` text.
	hintNone dawgsHint = iota
	// hintInt is "int", PostgreSQL's int4: size()'s
	// `jsonb_array_length(...)::int`.
	hintInt
	// hintInt8 is an integer literal, id() or an integer coalesce().
	hintInt8
	// hintFloat8 is a float literal or a float coalesce().
	hintFloat8
	// hintNumeric is datetime().epochseconds / .epochmillis.
	hintNumeric
	// hintOther is not a number (text, a boolean, a list), or a combination
	// dawgs refuses to type (float8 with an integer fails the translation).
	hintOther
)

// combineHints is dawgs' inferred type for an arithmetic step over hints a
// and b (DataType.OperatorResultType, CoerceToSupertype): an operand with no
// type takes the other's, numeric absorbs every number, int with int8 is
// int8, and float8 with an integer is a translation error.
func combineHints(a, b dawgsHint) dawgsHint {
	switch {
	case a == hintOther || b == hintOther:
		return hintOther
	case a == hintNone:
		return b
	case b == hintNone || a == b:
		return a
	case a == hintNumeric || b == hintNumeric:
		return hintNumeric
	case a != hintFloat8 && b != hintFloat8:
		return hintInt8
	default:
		return hintOther
	}
}

// hintCast is the cast dawgs gives a plain property whose partner has hint
// h, as the PostgreSQL type it produces -- `(p ->> 'v')::int` next to
// size(), `::int8` next to an integer, `::float8` next to a float,
// `::numeric` next to datetime(). It is false where dawgs leaves the
// property uncast text (hintNone: `n.v * d` is `text * integer`) or cannot
// type the step at all (hintOther); PostgreSQL fails either way.
func hintCast(h dawgsHint) (sqlNum, bool) {
	switch h {
	case hintInt:
		return sqlNumInt4, true
	case hintInt8:
		return sqlNumInt8, true
	case hintFloat8:
		return sqlNumFloat8, true
	case hintNumeric:
		return sqlNumNumeric, true
	}
	return sqlNumNone, false
}

// leafHint is dawgs' hint for an operand that is not arithmetic or a sign.
func leafHint(expr cypher.Expression) dawgsHint {
	switch e := unwrapParens(expr).(type) {
	case *cypher.Literal:
		if e == nil || e.Null {
			return hintOther
		}
		switch e.Value.(type) {
		case int64, uint64:
			return hintInt8
		case float64:
			return hintFloat8
		}
	case *cypher.PropertyLookup:
		if e != nil && !propertyRead(e) {
			return hintNumeric
		}
		return hintNone
	case *cypher.Variable:
		return hintNone
	case *cypher.FunctionInvocation:
		if e == nil {
			return hintOther
		}
		switch strings.ToLower(e.Name) {
		case cypher.ListSizeFunction:
			return hintInt
		case cypher.IdentityFunction:
			return hintInt8
		case cypher.CoalesceFunction:
			if _, untyped := untypedCoalesce(e); untyped {
				return hintNone
			}
			switch kind, ok := coalesceCastKind(e); {
			case ok && kind == coalesceInt8:
				return hintInt8
			case ok && kind == coalesceFloat8:
				return hintFloat8
			}
		}
	}
	return hintOther
}

// operandTyping is an arithmetic operand's PostgreSQL type and dawgs hint as
// far as they are known before execution. A WITH alias holds an integer or
// a numeric whose width only its value tells (valueSQLNum), but never a
// float8, which is all Plan asks of it -- it counts as int8 here; at run time
// evalOperandTyped types it by its value.
func operandTyping(expr cypher.Expression) (sqlNum, dawgsHint) {
	switch e := unwrapParens(expr).(type) {
	case *cypher.ArithmeticExpression:
		if e != nil {
			if t, h, ok := foldArithTyping(e, nil); ok {
				return t, h
			}
			return sqlNumNone, hintOther
		}
	case *cypher.UnaryAddOrSubtractExpression:
		if e != nil {
			t, h := operandTyping(e.Right)
			if folded, isLiteral := negatedLiteralSQLNum(e); isLiteral {
				t = folded
			}
			return t, h
		}
	case *cypher.Variable:
		return sqlNumInt8, hintNone
	}
	return leafSQLNum(expr), leafHint(expr)
}

// foldArithTyping folds ae's PostgreSQL types and dawgs hints step by step
// the way evalArithmeticTyped computes them -- a plain property operand of a
// numeric step takes the cast its partner's hint gives it, a concatenation
// has no numeric type -- calling step, when non-nil, with each numeric
// step's operator, type and whether one of its operands is a fractional
// literal. It reports false at a property dawgs leaves uncast (PostgreSQL
// fails the step) and at a step step refuses; otherwise it returns ae's type
// and hint.
func foldArithTyping(ae *cypher.ArithmeticExpression, step func(op cypher.Operator, t sqlNum, fractionalOperand bool) bool) (sqlNum, dawgsHint, bool) {
	cur, curHint := operandTyping(ae.Left)
	curKind := classifyAddOperand(ae.Left)
	for i, p := range ae.Partials {
		if p == nil {
			return sqlNumNone, hintOther, false
		}
		r, rHint := operandTyping(p.Right)
		rKind := classifyAddOperand(p.Right)
		numeric := p.Operator != cypher.OperatorAdd ||
			(curKind != addStaticText && rKind != addStaticText && (curKind != addPropertyLookup || rKind != addPropertyLookup))
		if numeric {
			if i == 0 && isPlainPropertyLookup(ae.Left) {
				cast, ok := hintCast(rHint)
				if !ok {
					return sqlNumNone, hintOther, false
				}
				cur, curHint = cast, rHint
			}
			if isPlainPropertyLookup(p.Right) {
				cast, ok := hintCast(curHint)
				if !ok {
					return sqlNumNone, hintOther, false
				}
				r, rHint = cast, curHint
			}
			cur, curHint = arithSQLNum(cur, r), combineHints(curHint, rHint)
			if step != nil && !step(p.Operator, cur, (i == 0 && fractionalLiteral(ae.Left)) || fractionalLiteral(p.Right)) {
				return sqlNumNone, hintOther, false
			}
		} else {
			cur, curHint = sqlNumNone, hintOther
		}
		curKind = nextAddKind(p.Operator, curKind, rKind)
	}
	return cur, curHint, true
}

// castPropertyAs casts a property's stored value to t, the PostgreSQL type
// of the cast dawgs gives it (hintCast). float8 and int8 are
// castPropertyForOrder's; a numeric cast takes int8's rules, which accept
// only whole numbers the cast reads exactly (declining the fractions numeric
// would accept); an int4 cast takes int8's and then int4's range, outside
// which PostgreSQL raises "out of range for type integer".
func castPropertyAs(v any, t sqlNum) (any, error) {
	switch t {
	case sqlNumFloat8:
		return castPropertyForOrder(v, true)
	case sqlNumInt8, sqlNumNumeric:
		return castPropertyForOrder(v, false)
	case sqlNumInt4:
		c, err := castPropertyForOrder(v, false)
		if err != nil || c == nil {
			return c, err
		}
		if f, _ := c.(float64); f < minInt4 || f > maxInt4 {
			return nil, ErrRuntimeCast
		}
		return c, nil
	}
	return nil, ErrUnsupported
}

// groupKeysNumbersCanonical reports whether every property a RETURN groups by
// holds only numbers stored in the spelling their float64 reproduces
// (snapshot.View.NumbersCanonical): grouping merges the numbers the float64
// cannot tell apart, as DISTINCT does. A group key is read off the row
// without passing checkExpr, whose checkPropertyLookup asks the same of
// every other property read.
func groupKeysNumbersCanonical(snap *snapshot.View, group *WithClause) bool {
	for _, c := range group.Computed {
		if pl, ok := unwrapParens(c.Expr).(*cypher.PropertyLookup); ok && pl != nil && !snap.NumbersCanonical(pl.Symbol) {
			return false
		}
	}
	return true
}
