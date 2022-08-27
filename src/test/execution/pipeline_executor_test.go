package test

import (
	"github.com/stretchr/testify/require"
	"testing"
	. "tiny-db/src/execution/executor"
	. "tiny-db/src/execution/operator"
)

/*
 * Test for table scan
 *
 */
func TestTableScan(t *testing.T) {
	operators := make([]Op, 0)
	ts := NewTableScan()

	operators = append(operators, ts)
	pipeline_executor, err := NewPipelineExecutor(operators, true)
	require.NoError(t, err)

	err = pipeline_executor.Execute()
	require.NoError(t, err)
}

/*
 * Test for table scan and limit.
 *
 */
func TestScanAndLimit(t *testing.T) {
	operators := make([]Op, 2)
	ts := NewTableScan()
	li := NewLimit(1000, 20, ts)

	operators[0] = ts
	operators[1] = li 
	
	pipeline_executor, err := NewPipelineExecutor(operators, true)
	require.NoError(t, err)

	// Using debug model can check the Size of result is 20 and start with 1000 (call output.Count()) and where to start (sels)
	err = pipeline_executor.Execute()
	require.NoError(t, err)
}
/*                               Limit         |<without push> <early break>
 *                                 |           |
 * (push the data to ht)        HashJoin       |
 * |<build ht>                 |      \        |<probe ht>
 * |                         Filter     Scan   |
 * |                         |                 (piepeline 2)
 * |                       Scan                (get data from source operator)
 * (pipeline 1)
 *
 */
