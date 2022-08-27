package executor

import (
	"errors"
	"tiny-db/src/storage"
)

type PhysicalOperatorType int

//go:generate go run golang.org/x/tools/cmd/stringer -type=PhysicalOperatorType  -trimprefix=Physical
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

/** Operator interface :
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
	GetOperatorType() PhysicalOperatorType
}

/** Op :
 * 	This struct is used to Inherited by a specific operator
 */
type Operator struct {
	Op_type PhysicalOperatorType
}

func (o *Operator) GetData(result *storage.DataChunk, state any) error {
	return errors.New(o.Op_type.String() + "not implemented GetData")
}

func (o *Operator) Execute(input, output *storage.DataChunk, state any) error {
	return errors.New(o.Op_type.String() + "not implemented Execute")
}

func (o *Operator) Materialize(output *storage.DataChunk, state any) error {
	return errors.New(o.Op_type.String() + "not implemented Materialize")
}

func (o *Operator) InitLocalStateForSource() (any, error) {
	return nil, errors.New(o.Op_type.String() + "not implemented InitLocalStateForSource")
}

func (o *Operator) InitLocalStateForExecute() (any, error) {
	return nil, errors.New(o.Op_type.String() + "not implemented InitLocalStateForExecute")
}

func (o *Operator) InitLocalStateForMaterialize() (any, error) {
	return nil, errors.New(o.Op_type.String() + "not implemented InitLocalStateForMaterialize")
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
