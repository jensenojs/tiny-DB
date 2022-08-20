package operator

import (
	"tiny-db/src/execution/executor"
	"tiny-db/src/common/vector"
	"tiny-db/src/storage"
)

type filterState struct {
	// hard code for now, need Expression
}

type Filter struct {
	executor.Operator
	ops filterState

	child executor.Operator	
}

func NewFilter(child executor.Operator) *Filter {
	var pf = new(Filter)
	pf.Op_type = executor.PhysicalFilter
	return pf
}

func (f *Filter) InitLocalStateForExecute() error {
	return nil
}

func (f *Filter) Executor(input, output *storage.DataChunk, state any) error {

	// hard code for now.
	sels := vector.NewSelVector()
	for i := 0; i < 100; i++ {
		sels.SetIndex(i, i + 50)
	}

	for i := 0; i < input.ColumnCount(); i++ {
		from := input.Cols[i]
		from.Reference(output.Cols[i])
		output.Cols[i].Slice(sels, 50)
	}
	return nil
}