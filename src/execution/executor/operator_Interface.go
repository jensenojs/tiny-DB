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

/** Operator interface : used in pipeline executor
 *
 */
type op interface {
	GetData(result *storage.DataChunk, state any) error
	Execute(input *storage.DataChunk, state any) (*storage.DataChunk, error)
	Materialize(input *storage.DataChunk) error

	InitLocalStateForSource() (any, error)
	InitLocalStateForExecute() (any, error)
	InitLocalStateForMaterialize() (any, error)

	IsPipelineBreaker() bool
	IsEnd() bool
	GetOperatorType() PhysicalOperatorType
}

type Operator struct {
	Op_type PhysicalOperatorType
}

func (o *Operator) GetData(result *storage.DataChunk, state any) error {
	return errors.New("not implemented")
}

func (o *Operator) Execute(input *storage.DataChunk, state any) (*storage.DataChunk, error) {
	return nil, errors.New("not implemented")
}

func (o *Operator) Materialize(input *storage.DataChunk) error {
	return errors.New("not implemented")
}

func (o *Operator) InitLocalStateForSource() (any, error) {
	return nil, errors.New("not implemented")
}

func (o *Operator) InitLocalStateForExecute() (any, error) {
	return nil, errors.New("not implemented")
}

func (o *Operator) InitLocalStateForMaterialize() (any, error) {
	return nil, errors.New("not implemented")
}

func (o *Operator) IsPipelineBreaker() bool {
	return false
}

func (o *Operator) IsEnd() bool {
	return false
}

func (o *Operator) GetOperatorType() PhysicalOperatorType {
	return o.Op_type
}
