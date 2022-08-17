package operator

import (
	"tiny-db/src/execution/executor"
	"tiny-db/src/storage"
)

type TableScanState struct {
	scan_table *storage.Table
	scan_idx int
}

type TableScan struct {
	executor.Operator
	ops TableScanState
}

func NewTableScan() *TableScan{
	var pt = new(TableScan)
	pt.Op_type = executor.PhysicalTableScan
	return pt
}

func(t *TableScan) InitLocalStateForSource(table *storage.Table) {
	t.ops.scan_table = table
	t.ops.scan_idx = 0
}

func (t *TableScan) GetData(result *storage.DataChunk, state any) error {
	ops := state.(TableScanState)
	vec, err := ops.scan_table.GetRowsGroup(ops.scan_idx); if err != nil {
		return err
	}
	println("column num is ", len(vec), "row num is", vec[0].Size())
	ops.scan_idx++
	result = storage.NewDataChunk(vec)
	return nil	
}