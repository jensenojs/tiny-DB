package operator

import (
	"errors"
	"tiny-db/src/common/types"
	"tiny-db/src/common/vector"
	"tiny-db/src/execution/executor"
	"tiny-db/src/storage"
)

type tableScanState struct {
	scan_idx int
}

type TableScan struct {
	executor.Operator
}

func NewTableScan() *TableScan {
	var pt = new(TableScan)
	pt.Op_type = executor.PhysicalTableScan
	return pt
}

func (t *TableScan) InitLocalStateForSource() (any, error) {
	return &tableScanState{}, nil
}

func (t *TableScan) GetData(result *storage.DataChunk, state any) error {
	ops := state.(tableScanState)
	if ops.scan_idx >= 5 { // hard code here
		return nil
	}

	for i := 0; i < result.ColumnCount(); i++ {
		col := result.Cols[i]
		ptype := col.GetPhyType()
		if ptype == types.INT32 {
			rawVec := vector.GetColumn[int32](col)
			for i := 0; i < vector.COMMON_VECTOR_SIZE; i++ {
				rawVec[i] = int32(i + vector.COMMON_VECTOR_SIZE*ops.scan_idx)
			}
		} else {
			return errors.New(t.Op_type.String() + ": Unspport phyType")
		}
	}
	ops.scan_idx++
	return nil
}
