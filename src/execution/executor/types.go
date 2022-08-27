package executor

import (
	"tiny-db/src/storage"
)

/** PipelineExecutor : Need more comments here.
 *
 *  In general, one pipeline can have multiple pipeline operators.
 *  In this situation, it may not be a good idea to keep the runtime state inside the operator.
 *	Since there may be multiple threads computing for a single operator parallelly, and each thread need have their own local state.
 *  And that is why the states of operators stored in the pipeline executor.
 */
type PipelineExecutor struct {
	operators []Op
	states    []any // Need some comments here.
	chunks    []*storage.DataChunk

	ispull bool // Need better name
}

/** operator interface :
 *  used in pipeline executor
 */
type Op interface {
	GetData(output *storage.DataChunk, state any) error
	Execute(input, output *storage.DataChunk, state any) error
	Materialize(output *storage.DataChunk, state any) error

	InitLocalStateForSource() (any, error)
	InitLocalStateForExecute() (any, error)
	InitLocalStateForMaterialize() (any, error)

	IsPipelineBreaker() bool
	IsEnd(state any) bool
}
