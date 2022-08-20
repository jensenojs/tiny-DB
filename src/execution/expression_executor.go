package execution

import (
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

func (e *ExpressionExecutor) Execute(expr expression.Expression) {
	switch expr.Type() {
	case expression.BOUND_INPUT:
		e.ExecuteInputRef(expr.(*expression.BoundInputRef))
	default:
		panic("Unsupport!")
	}
}

func (e *ExpressionExecutor) ExecuteInputRef(expr *expression.BoundInputRef, result *vector.Vector) {
	result.Reference(e.inputChunk.Cols[expr.RefIdx])
}
