package executor

import (
	"errors"
	"tiny-db/src/storage"
)

type PhysicalOperatorType int

const (
	PhysicalInvalid PhysicalOperatorType = iota
	PhysicalTableScan
	PhysicalFilter
	PhysicalHashJoin
	PhysicalLimit
	PhysicalCollector
)

/** PipelineExecutor : Need more comments here.
 *
 *  In general, one pipeline can have multiple pipeline executors. 
 *  In this situation, it may not be a good idea to keep the runtime state inside the operator. 
 *	Since there may be multiple threads computing for a single operator parallelly, and each thread need have their own local state.
 *  And that is why the states of executors stored in the pipeline executor.
 */
type PipelineExecutor struct {
	executors []op
	states    []any // Need some comments here.
	chunks    []*storage.DataChunk

	ispull bool // Need better name
}

/** Operator interface :
 *  used in pipeline executor
 */
type op interface {
	GetData(output *storage.DataChunk, state any) error
	Execute(input, output *storage.DataChunk, state any) error
	Materialize(input *storage.DataChunk, state any) error

	InitLocalStateForSource() (any, error)
	InitLocalStateForExecute() (any, error)
	InitLocalStateForMaterialize(mChunks []*storage.DataChunk) (any, error)

	IsPipelineBreaker() bool
	IsEnd(state any) bool
	GetOperatorType() PhysicalOperatorType
}

/** Op :
 * 	This struct is used to Inherited by a specific operator
 */
type Operator struct {
	Op_type PhysicalOperatorType
}

func (o *Operator) GetData(result *storage.DataChunk, state any) error {
	return errors.New("not implemented")
}

func (o *Operator) Execute(input, output *storage.DataChunk, state any) error {
	return errors.New("not implemented")
}

func (o *Operator) Materialize(input *storage.DataChunk, state any) error {
	return errors.New("not implemented")
}

func (o *Operator) InitLocalStateForSource() (any, error) {
	return nil, errors.New("not implemented")
}

func (o *Operator) InitLocalStateForExecute() (any, error) {
	return nil, errors.New("not implemented")
}

func (o *Operator) InitLocalStateForMaterialize(mChunks []*storage.DataChunk) (any, error) {
	return nil, errors.New("not implemented")
}

func (o *Operator) IsPipelineBreaker() bool {
	return false
}

func (o *Operator) IsEnd(state any) bool {
	return false
}

func (o *Operator) GetOperatorType() PhysicalOperatorType {
	return o.Op_type
}
