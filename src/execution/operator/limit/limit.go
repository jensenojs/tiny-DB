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
	executor.Operator
	ops limitState

	offset int
	limit  int
	child  executor.Operator
}

func NewLimit(offset, limit int, child executor.Operator) *Limit {
	var pl = new(Limit)
	pl.Op_type = executor.PhysicalLimit
	pl.offset = offset
	pl.limit = limit
	pl.child = child
	return pl
}

func (l *Limit) InitLocalStateForExecute() error {
	l.ops.cursor = 0
	return nil
}

func (l *Limit) Execute(input *storage.DataChunk, state any) (*storage.DataChunk, error) {
	ops := state.(limitState)
	maxCount := l.limit + l.offset
	if ops.cursor >= maxCount {
		return storage.NewDataChunk(make([]*vector.Vector, 0)), nil
	}

	curOffset := ops.cursor
	output := storage.NewDataChunk(make([]*vector.Vector, 0))
	start := 0
	count := input.ChunkNum()

	if curOffset < l.offset {
		if input.ChunkNum()+curOffset < l.offset {
			ops.cursor += input.ChunkNum()
			return output, nil
		} else {
			start = l.offset - curOffset
			count = Min(input.ChunkNum() - start, l.limit)
		}
	} else {
		if curOffset + input.ChunkNum() >= l.limit {
			count = l.limit - curOffset
		} 
	}

	output, _ = storage.NewDataChunkWithSpecificType(input)
	for i := 0; i < input.ColumnCount(); i++ {
		from := input.GetVector(i)
		to := output.GetVector(i)
		to.Dup(from, start, count)
	}
	return output, nil
}

func Min(i1, i2 int) int {
	if i1 < i2 {
		return i1
	} else {
		return i2
	}
}

func (l *Limit) IsEnd() bool {
	s := l.ops
	return s.cursor >= l.offset+l.limit
}
