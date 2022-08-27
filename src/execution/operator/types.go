package operator

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

/** Op :
 * 	This struct is used to Inherited by a specific operator
 */
type operator struct {
	Op_type PhysicalOperatorType
}

func (o *operator) GetData(result *storage.DataChunk, state any) error {
	return errors.New(o.Op_type.String() + "not implemented GetData")
}

func (o *operator) Execute(input, output *storage.DataChunk, state any) error {
	return errors.New(o.Op_type.String() + "not implemented Execute")
}

func (o *operator) Materialize(output *storage.DataChunk, state any) error {
	return errors.New(o.Op_type.String() + "not implemented Materialize")
}

func (o *operator) InitLocalStateForSource() (any, error) {
	return nil, errors.New(o.Op_type.String() + "not implemented InitLocalStateForSource")
}

func (o *operator) InitLocalStateForExecute() (any, error) {
	return nil, errors.New(o.Op_type.String() + "not implemented InitLocalStateForExecute")
}

func (o *operator) InitLocalStateForMaterialize() (any, error) {
	return nil, errors.New(o.Op_type.String() + "not implemented InitLocalStateForMaterialize")
}

func (o *operator) IsPipelineBreaker() bool {
	return false
}

func (o *operator) IsEnd(state any) bool {
	return false
}

func (o *operator) GetOperatorType() PhysicalOperatorType {
	return o.Op_type
}
