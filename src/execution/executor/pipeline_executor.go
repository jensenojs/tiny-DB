package executor

import (
	"tiny-db/src/common/types"
	. "tiny-db/src/storage"
)

func NewPipelineExecutor(operators []Op, ispull bool) (*PipelineExecutor, error) {
	chunks := make([]*DataChunk, len(operators))
	states := make([]any, len(operators))
	var err error

	// Hard code to make chunks types
	typs := make([]types.PhysicalType, 0)
	typs = append(typs, types.INT32)
	chunks[0] = NewDataChunk(typs)
	states[0], err = operators[0].InitLocalStateForSource()
	if err != nil {
		return nil, err
	}

	for i := 1; i < len(operators)-1; i++ {
		states[i], err = operators[i].InitLocalStateForExecute()
		if err != nil {
			return nil, err
		}
		chunks[i] = NewDataChunk(typs)
	}

	last := len(states) - 1
	if last != 0 {
		if ispull {
			states[last], err = operators[len(operators)-1].InitLocalStateForExecute()
		} else {
			states[last], err = operators[len(operators)-1].InitLocalStateForMaterialize()
		}
		chunks[last] = NewDataChunk(typs)
	}

	if err != nil {
		return nil, err
	}

	return &PipelineExecutor{
		operators: operators,
		chunks:    chunks,
		states:    states,
		ispull:    ispull,
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
		e.clean()
		return err
	}
	return nil
}

func (e *PipelineExecutor) executePull() error {
	// Set data in chunk[0] when pipeline is in source state.
	e.operators[0].GetData(e.chunks[0], e.states[0])
	if e.chunks[0].Count() == 0 {
		return nil
	}

	// Execute
	for i := 1; i < len(e.operators); i++ {
		err := e.operators[i].Execute(e.chunks[i-1], e.chunks[i], e.states[i])
		if err != nil {
			e.clean()
			return err
		}
		if e.operators[i].IsPipelineBreaker() {
			return nil
		}
		if e.chunks[i].Count() == 0 {
			return nil
		}
	}

	return nil
}

func (e *PipelineExecutor) executePush() error {
	// Set data in chunks[0] when pipeline is in source state.
	e.operators[0].GetData(e.chunks[0], e.states[0])
	if e.chunks[0].Count() == 0 {
		return nil
	}

	needMaterialize := true
	// Execute
	for i := 1; i < len(e.operators)-1; i++ {
		err := e.operators[i].Execute(e.chunks[i-1], e.chunks[i], e.states[i])
		if err != nil {
			e.clean()
			return err
		}

		if e.operators[i].IsPipelineBreaker() {
			return nil
		}

		if e.chunks[i].Count() == 0 {
			needMaterialize = false
			return nil
		}

	}

	// Materialize
	last := len(e.chunks) - 1
	if needMaterialize && last > 0 {
		e.operators[len(e.operators)-1].Materialize(e.chunks[last], e.states[last])
	}

	return nil
}

func (e *PipelineExecutor) clean() {
	e.operators = nil
	e.chunks = nil
	e.states = nil
}
