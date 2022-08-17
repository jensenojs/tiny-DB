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
}

func NewLimit(offset, limit int) *Limit {
	var pl = new(Limit)
	pl.Op_type = executor.PhysicalLimit
	pl.offset = offset
	pl.limit = limit
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

	cur_offset := ops.cursor

	// hard code for test now.
	chunk := storage.NewDataChunk(make([]*vector.Vector, 0))

	

	if cur_offset < l.offset {
		if input.ChunkNum()+cur_offset < l.offset {
			ops.cursor += input.ChunkNum()
			return chunk, nil
		} else {
			// leftToStart := l.offset - cur_offset
			// dataCount := Min(input.ChunkNum() - leftToStart, l.limit)
			
		}
	} else {

	}
	return nil, nil
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
