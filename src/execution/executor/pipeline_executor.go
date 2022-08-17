package executor

import (
	"tiny-db/src/storage"
)

/** PipelineExecutor
 *
 */
type PipelineExecutor struct {
	executors []op
	states    []any
	chunks    []*storage.DataChunk

	ispull bool // Need better name
}

func NewPipelineExecutor(executors []op, ispull bool) (*PipelineExecutor, error) {
	c := make([]*storage.DataChunk, len(executors))
	s := make([]any, len(executors))
	var err error

	s[0], err = executors[0].InitLocalStateForSource()
	if err != nil {
		return nil, err
	}

	for i := 1; i < len(executors)-1; i++ {
		s[i], err = executors[i].InitLocalStateForExecute()
		if err != nil {
			return nil, err
		}
	}

	if ispull {
		s[len(s)-1], err = executors[len(executors)-1].InitLocalStateForExecute()
	} else {
		s[len(s)-1], err = executors[len(executors)-1].InitLocalStateForMaterialize()
	}

	if err != nil {
		return nil, err
	}

	return &PipelineExecutor{
		executors: executors,
		chunks: c,
		states: s,
		ispull: ispull,
	}, nil
}

func (e *PipelineExecutor) Execute() error {
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
	e.executors[0].GetData(e.chunks[0], e.states[0])
	if e.chunks[0].ChunkNum() == 0 {
		return nil
	}

	// Execute
	for i := 1; i < len(e.executors); i++ {
		cp, err := e.executors[i].Execute(e.chunks[i-1], e.states[i])
		if err != nil {
			return err
		}
		e.chunks[i] = cp

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
	// Set data in chunks[0] when pipeline is in source state.
	e.executors[0].GetData(e.chunks[0], e.states[0])
	if e.chunks[0].ChunkNum() == 0 {
		return nil
	}

	needMaterialize := true
	// Execute
	for i := 1; i < len(e.executors)-1; i++ {
		cp, err := e.executors[i].Execute(e.chunks[i-1], e.states[i])
		if err != nil {
			return err
		}
		e.chunks[i] = cp

		if e.executors[i].IsPipelineBreaker() {
			return nil
		}

		if cp.ChunkNum() == 0 {
			needMaterialize = false
			return nil
		}

	}

	// Materialize
	if needMaterialize {
		e.executors[len(e.executors)-1].Materialize(e.chunks[len(e.chunks)-1])
	}

	e.clean()
	return nil
}

func (e *PipelineExecutor) clean() {
	e.executors = nil
	e.chunks = nil
	e.states = nil
}
