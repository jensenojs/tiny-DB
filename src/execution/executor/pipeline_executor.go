package executor

import (
	"tiny-db/src/storage"
)

/** PipelineExecutor
 *
 */
type PipelineExecutor struct {
	executors []op
	states    []opState
	chunk     []storage.DataChunk

	ispull bool // Need better name
}

func NewPipelineExecutor(executors []op, states []opState, chunk []storage.DataChunk) *PipelineExecutor {
	return &PipelineExecutor{
		executors: executors,
		states:    states,
		chunk:     chunk,
	}
}

func (e *PipelineExecutor) execute() {
	if e.ispull {
		e.execute_pull()
	} else {
		e.execute_push()
	}
}

func (e *PipelineExecutor) execute_pull() {

}

func (e *PipelineExecutor) execute_push() {

}
