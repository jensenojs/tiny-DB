package operator

import (
	"tiny-db/src/execution/executor"
	"tiny-db/src/storage"
)

type tableScanState struct {
	scan_idx int
}

type TableScan struct {
	executor.Operator
	ops tableScanState

	scan_table *storage.Table
}

func NewTableScan(t *storage.Table) *TableScan {
	var pt = new(TableScan)
	pt.Op_type = executor.PhysicalTableScan
	pt.scan_table = t
	return pt
}

func (t *TableScan) InitLocalStateForSource() (any, error) {
	t.ops.scan_idx = 0
	return t.ops, nil
}

func (t *TableScan) GetData(result *storage.DataChunk, state any) error {
	ops := state.(tableScanState)
	vec, err := t.scan_table.GetRowsGroup(ops.scan_idx)
	if err != nil {
		return err
	}
	ops.scan_idx++
	result = storage.NewDataChunk(vec)
	println("column num is ", result.ColumnCount(), "row num is", vec[0].Size())
	return nil
}
