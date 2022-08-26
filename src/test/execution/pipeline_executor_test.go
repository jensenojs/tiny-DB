package test

import (
	"testing"
	. "tiny-db/src/execution/executor"
	. "tiny-db/src/execution/operator"
)

/*
 * Test for table scan
 *
 */
func TestPipe(t *testing.T) {
	// pipeline 1
	ts := NewTableScan()
	executor := NewPipelineExecutor()

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