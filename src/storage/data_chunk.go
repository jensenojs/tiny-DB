package storage

import (
	"tiny-db/src/common/types"
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

func NewDataChunk(types []types.PhysicalType) *DataChunk {
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
