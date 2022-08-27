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
	// pipeline 1
	operators := make([]Op, 0)
	ts := NewTableScan()

	operators = append(operators, ts)
	pipeline_executor, err := NewPipelineExecutor(operators, false)
	require.NoError(t, err)

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
