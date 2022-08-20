package expression

import "strconv"

type ExprType int

const (
	BOUND_INPUT ExprType = iota
	BOUND_BINARY
)

type Expression interface {
	ToString() string

	Equal(expr Expression) bool

	Hash() int

	Type() ExprType
}

type BinaryOpType int

const (
	EQ BinaryOpType = iota
)

type BoundBinaryOp struct {
	leftExpr  Expression
	rightExpr Expression
	op        BinaryOpType
}

func (e *BoundBinaryOp) Type() ExprType {
	return BOUND_BINARY
}

type BoundInputRef struct {
	RefIdx int
}

func (e *BoundInputRef) Type() ExprType {
	return BOUND_INPUT
}

func (e *BoundInputRef) ToString() string {
	return strconv.FormatInt(int64(e.RefIdx), 10)
}

// TODO(lokax):
func (e *BoundInputRef) Equal(expr Expression) bool {
	return false
}

func (e *BoundInputRef) Hash() int {
	return e.RefIdx
}
