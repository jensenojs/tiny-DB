package operator

import (
	"tiny-db/src/execution/executor"
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

func (f *Filter) Executor(input *storage.DataChunk, state any) (*storage.DataChunk, error) {
	output, _ := storage.NewDataChunkWithSpecificType(input)

	// hard code for now.
	start := 0
	// count := input.ChunkNum()
	count := 10

	for i := 0; i < input.ColumnCount(); i++ {
		from := input.GetVector(i)
		to := output.GetVector(i)
		to.Dup(from, start, count)
	}
	return output, nil
}