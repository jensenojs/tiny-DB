package expression

import (
	"strconv"
	"tiny-db/src/common/types"
)

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

	ReturnType() types.PhysicalType
}

type BinaryOpType int

const (
	EQ BinaryOpType = iota
)

type BoundBinaryOp struct {
	LeftExpr  Expression
	RightExpr Expression
	Op        BinaryOpType
	RType     types.PhysicalType
}

func (e *BoundBinaryOp) Type() ExprType {
	return BOUND_BINARY
}

// Need more info
func (e *BoundBinaryOp) ToString() string {
	return "BoundBinaryOp"
}

// TODO():
func (e *BoundBinaryOp) Equal(expr Expression) bool {
	return false
}

func (e *BoundBinaryOp) Hash() int {
	return 0
}

func (e *BoundBinaryOp) ReturnType() types.PhysicalType {
	return e.RType
}

type BoundInputRef struct {
	RefIdx int
	RType  types.PhysicalType
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

func (e *BoundInputRef) ReturnType() types.PhysicalType {
	return e.RType
}
