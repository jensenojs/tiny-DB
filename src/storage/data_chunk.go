package storage

import (
	"errors"
	"tiny-db/src/common/value"
	"tiny-db/src/common/vector"
)

/** DataChunk
 *
 */
type DataChunk struct {
	cols     []*vector.Vector
	row_size int
}

func NewDataChunk(cols []*vector.Vector) *DataChunk {
	row_size := 0
	if len(cols) != 0 {
		row_size = cols[0].Size()
	}
	return &DataChunk{
		cols:     cols,
		row_size: row_size,
	}
}

// Generate vecs of the same type as input.
func NewDataChunkWithSpecificType(template *DataChunk) (*DataChunk, error) {
	chunk := NewDataChunk(make([]*vector.Vector, 0))
	var vec *vector.Vector
	for i := 0; i < template.ColumnCount(); i++ {
		switch template.GetVector(i).GetType() {
		case value.INT32:
			vec = vector.NewVectorInt32(make([]int32, 0))
		case value.STRING:
			vec = vector.NewVectorString(make([]string, 0))
		default:
			return nil, errors.New("unsupported value type")
		}
		chunk.PushColumn(vec)
	}
	return chunk, nil
}

func (d *DataChunk) ColumnCount() int {
	return len(d.cols)
}

func (d *DataChunk) ChunkNum() int {
	return d.row_size
}

func (d *DataChunk) GetVector(idx int) *vector.Vector {
	return d.cols[idx]
}

func (d *DataChunk) PushColumn(input *vector.Vector) error {
	if d.ColumnCount() == 0 {
		d.cols = append(d.cols, input)
		d.row_size = input.Size()
	} else {
		if d.ChunkNum() != input.Size() {
			return errors.New("the length of vec must be equal to the cols")
		}
		d.cols = append(d.cols, input)
	}
	return nil
}

func (d *DataChunk) PushColumns(inputs []*vector.Vector) error {
	// check
	size := inputs[0].Size()
	for i := 1; i < len(inputs); i++ {
		if size != inputs[i].Size() {
			return errors.New("the number of rows between the input vectors is not equal")
		}
	}

	if d.ColumnCount() == 0 {
		d.cols = append(d.cols, inputs...)
		d.row_size = size
	} else {
		if size != d.ChunkNum() {
			return errors.New("the length of vec must be equal to the cols")
		}
		d.cols = append(d.cols, inputs...)
	}
	return nil
}

func (d *DataChunk) Clean() {
	d.cols = nil
	d.row_size = 0
}
