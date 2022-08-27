package operator

import (
	"tiny-db/src/common/vector"
	"tiny-db/src/execution/executor"
	"tiny-db/src/storage"
)

type limitState struct {
	cursor int
}

type Limit struct {
	operator

	offset int
	limit  int
	child  executor.Op
}

func NewLimit(offset, limit int, child executor.Op) *Limit {
	var pl = new(Limit)
	pl.Op_type = PhysicalLimit
	pl.offset = offset
	pl.limit = limit
	pl.child = child
	return pl
}

func (l *Limit) InitLocalStateForExecute() (any, error) {
	s := &limitState{cursor: 0}
	return s, nil
}

func (l *Limit) InitLocalStateForMaterialize() (any, error) {
	s := &limitState{cursor: 0}
	return s, nil
}

func (l *Limit) Execute(input, output *storage.DataChunk, state any) error {
	ops := state.(*limitState)
	maxCount := l.limit + l.offset
	if ops.cursor >= maxCount {
		output.Reset()
		return nil
	}

	curOffset := ops.cursor
	start := 0
	count := input.Count()

	if curOffset < l.offset {
		if input.Count()+curOffset < l.offset {
			ops.cursor += input.Count()
			output.Reset()
			return nil
		} else {
			start = l.offset - curOffset
			count = Min(input.Count()-start, l.limit)
		}
	} else {
		if curOffset+input.Count() >= l.limit {
			count = l.limit - curOffset
		}
	}

	sels := vector.NewSelVector()
	for i := 0; i < count; i++ {
		sels.SetIndex(i, start+i)
	}
	output.Slice(input, sels, count)

	return nil
}

func Min(i1, i2 int) int {
	if i1 < i2 {
		return i1
	} else {
		return i2
	}
}

func (l *Limit) IsEnd(state any) bool {
	s := state.(limitState)
	return s.cursor >= l.offset+l.limit
}
