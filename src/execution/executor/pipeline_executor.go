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
	chunk     []*storage.DataChunk

	ispull bool // Need better name
}

func NewPipelineExecutor(executors []op, states []opState, chunk []*storage.DataChunk) *PipelineExecutor {
	return &PipelineExecutor{
		executors: executors,
		states:    states,
		chunk:     chunk,
	}
}

func (e *PipelineExecutor) execute() error {
	var err error
	if e.ispull {
		err = e.executePull()
	} else {
		err = e.executePush()
	}

	if err != nil {
		return err
	}
	return nil
}

func (e *PipelineExecutor) executePull() error {
	// Set data in chunk[0] when pipeline is in source state.
	e.executors[0].GetData(e.chunk[0], e.states[0])
	if (e.chunk[0].ChunkNum() == 0) {
		return nil
	}

	// Execute
	for i := 1; i < len(e.executors); i++ {
		cp, err := e.executors[i].Execute(e.chunk[i - 1], e.states[i]); if err != nil {
			return err
		}
		e.chunk[i] = cp	

		if e.executors[i].IsPipelineBreaker() {
			return nil
		}
		if cp.ChunkNum() == 0 {
			return nil
		}
	}
	e.clean()	
	return nil
}

func (e *PipelineExecutor) executePush() error {
	// Set data in chunk[0] when pipeline is in source state.
	e.executors[0].GetData(e.chunk[0], e.states[0])
	if (e.chunk[0].ChunkNum() == 0) {
		return nil
	}

	finished := true
	// Execute
	for i := 1; i < len(e.executors) - 1; i++ {
		cp, err := e.executors[i].Execute(e.chunk[i - 1], e.states[i]); if err != nil {
			return err
		}
		e.chunk[i] = cp	

		if e.executors[i].IsPipelineBreaker() {
			return nil
		}

		if cp.ChunkNum() == 0 {
			finished = false
			return nil
		}

	}

	if finished {
		e.executors[len(e.executors) - 1].Materialize(e.chunk[len(e.chunk) - 1])
	}
	e.clean()
	return nil
}

func (e *PipelineExecutor) clean() {
	e.executors = nil
	e.chunk = nil
	e.states = nil
}