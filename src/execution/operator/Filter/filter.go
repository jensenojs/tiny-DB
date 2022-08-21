package operator

import (
	"tiny-db/src/execution/executor"
	"tiny-db/src/common/vector"
	"tiny-db/src/storage"
)

type filterState struct {
	// TODO(Jensen): hard code for now, need Expression
}

type Filter struct {
	executor.Operator

	child executor.Operator	
}

func NewFilter(child executor.Operator) *Filter {
	var pf = new(Filter)
	pf.Op_type = executor.PhysicalFilter
	return pf
}

func (f *Filter) InitLocalStateForExecute() (any, error) {
	s := &filterState{}	
	return s, nil
}

func (f *Filter) Execute(input, output *storage.DataChunk, state any) error {

	sels := vector.NewSelVector()
	count := 0
	for i := 0; i < input.Count(); i++ {
		// need expression to decide whether this data selected.
		// if input.Cols[1].GetValue(i) >= 100 {
			sels.SetIndex(count, i)
		// }
		count++
	}

	output.Slice(input, sels, count)

	return nil
}