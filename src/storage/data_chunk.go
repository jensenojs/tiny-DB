package storage

import (
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

func (d *DataChunk) ColumnCount() int {
	return len(d.cols)
}

func (d *DataChunk) ChunkNum() int {
	return d.row_size
}

func (d *DataChunk) GetVector(idx int) *vector.Vector {
	return d.cols[idx]
}

func (d *DataChunk) Clean() {
	d.cols = nil
	d.row_size = 0
}
