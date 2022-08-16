package executor

import (
	"errors"
	"tiny-db/src/storage"
)

type DummyState struct {}

func NewDummyState() *DummyState {
	return &DummyState{}
}

/** Operator state : used in pipeline executor
 *
 */
type opState interface {
	InitLocalStateForSource() any
	InitLocalStateForExecute() any
	InitLocalStateForMaterialize() any
}

type OperatorState struct{}

func (s *OperatorState) InitLocalStateForSource() (DummyState, error) {
	return *NewDummyState(), errors.New("not implemented")
}

func (s *OperatorState) InitLocalStateForExecute() (DummyState, error) {
	return *NewDummyState(), errors.New("not implemented")
}

func (s *OperatorState) InitLocalStateForMaterialize() (DummyState, error) {
	return *NewDummyState(), errors.New("not implemented")
}

type PhysicalOperatorType int

const (
	PhysicalInvalid   PhysicalOperatorType = 0
	PhysicalTableScan PhysicalOperatorType = 1
	PhysicalFilter    PhysicalOperatorType = 2
	PhysicalHashJoin  PhysicalOperatorType = 3
	PhysicalLimit     PhysicalOperatorType = 4
	PhysicalCollector PhysicalOperatorType = 5
)

/** Operator interface : used in pipeline executor
 *
 */
type op interface {
	GetData(result *storage.DataChunk, state opState) error
	Execute(input *storage.DataChunk, state opState) (*storage.DataChunk, error)
	Materialize(input *storage.DataChunk) error
	IsPipelineBreaker() bool
	GetOperatorType() PhysicalOperatorType
}

type Operator struct {
	op_type PhysicalOperatorType
}

func (o *Operator) GetData(result *storage.DataChunk, state opState) error {
	return errors.New("not implemented")
}

func (o *Operator) Execute(input *storage.DataChunk, state opState) (*storage.DataChunk, error) {
	return nil, errors.New("not implemented")
}

func (o *Operator) Materialize(input *storage.DataChunk) error {
	return errors.New("not implemented")
}

func (o *Operator) IsPipelineBreaker() bool {
	return false
}

func (o *Operator) GetOperatorType() PhysicalOperatorType {
	return o.op_type
}
