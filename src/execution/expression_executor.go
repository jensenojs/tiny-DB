package execution

import (
	"tiny-db/src/common/types"
	"tiny-db/src/common/vector"
	"tiny-db/src/planner/expression"
	"tiny-db/src/storage"
)

type ExpressionExecutor struct {
	inputChunk *storage.DataChunk
}

func NewExprExec() *ExpressionExecutor {
	return &ExpressionExecutor{
		inputChunk: nil,
	}
}

func (e *ExpressionExecutor) AddChunk(c *storage.DataChunk) {
	e.inputChunk = c
}

func (e *ExpressionExecutor) Execute(expr expression.Expression, sel *vector.SelectionVector, res *vector.Vector, count int) {
	switch expr.Type() {
	case expression.BOUND_INPUT:
		e.ExecuteInputRef(expr.(*expression.BoundInputRef), sel, res, count)
	case expression.BOUND_BINARY:
		e.ExecuteBinary(expr.(*expression.BoundBinaryOp), sel, res, count)
	default:
		panic("Unsupport!")
	}
}

func (e *ExpressionExecutor) ExecuteInputRef(expr *expression.BoundInputRef, sel *vector.SelectionVector, res *vector.Vector, count int) {
	if sel != nil {
		res.SliceOther(e.inputChunk.Cols[expr.RefIdx], sel, count)
	} else {
		res.Reference(e.inputChunk.Cols[expr.RefIdx])
	}
}

func Equal[T int32 | int64](lhs []T, rhs []T, result []bool, sel *vector.SelectionVector, count int) {
	if sel == nil {
		for i := 0; i < count; i++ {
			result[i] = lhs[i] == rhs[i]
		}
	} else {
		for i := 0; i < count; i++ {
			realIdx := sel.GetIndex(i)
			result[i] = lhs[realIdx] == rhs[realIdx]
		}
	}
}

func Eq(lhs *vector.Vector, rhs *vector.Vector, res *vector.Vector, sel *vector.SelectionVector, count int) {
	if lhs.GetPhyType() != rhs.GetPhyType() || res.GetPhyType() != types.BOOL {
		panic("Type mismatched!")
	}
	// TODO(): Need Merge Validality Mask
	// rhs.Validality = lhs.Validality.Merge(rhs.Validality)
	// Extract Selection Vector here, instead of pass in
	switch lhs.GetPhyType() {
	case types.INT32:
		Equal(vector.GetColumn[int32](lhs), vector.GetColumn[int32](rhs), vector.GetColumn[bool](res), sel, count)
	default:
		panic("Unsupport type!")
	}
}

func (e *ExpressionExecutor) ExecuteBinary(expr *expression.BoundBinaryOp, sel *vector.SelectionVector, res *vector.Vector, count int) {
	lhs := vector.NewVector(expr.LeftExpr.ReturnType())
	rhs := vector.NewVector(expr.LeftExpr.ReturnType())
	e.Execute(expr.LeftExpr, sel, lhs, count)
	e.Execute(expr.RightExpr, sel, rhs, count)
	switch expr.Op {
	case expression.EQ:
		// Some problem here
		Eq(lhs, rhs, res, sel, count)
	default:
		panic("Unsupport!")
	}
}
