package storage

import (
	"tiny-db/src/common/value"
	"tiny-db/src/common/vector"
)

/** DataChunk
 *
 */
type DataChunk struct {
	Cols     []*vector.Vector
	cache    []*vector.VectorCache
	count    uint64
	capacity uint64
}

func NewDataChunk(types []value.PhysicalType) *DataChunk {
	chunk := &DataChunk{
		Cols:     make([]*vector.Vector, len(types)),
		cache:    make([]*vector.VectorCache, len(types)),
		count:    0,
		capacity: vector.COMMON_VECTOR_SIZE,
	}
	for i := 0; i < len(types); i++ {
		chunk.cache[i] = vector.NewVectorCache(types[i])
		chunk.Cols[i] = vector.NewFromCache(chunk.cache[i])
	}
	return chunk
}

func (c *DataChunk) Reset() {
	for i := 0; i < len(c.Cols); i++ {
		c.cache[i].ResetVector(c.Cols[i])
	}
}

func (c *DataChunk) Count() uint64 {
	return c.count
}

func (c *DataChunk) SetCount(count uint64) {
	c.count = count
}

func (c *DataChunk) ColumnCount() uint64 {
	return uint64(len(c.Cols))
}

/*
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
*/
