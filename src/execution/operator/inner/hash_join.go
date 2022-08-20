package operator

import (
	"sync"
	"tiny-db/src/common/value"
	"tiny-db/src/execution/executor"
)

type innerHashState struct {
	l     sync.Mutex
	m     map[any]any
	kType value.PhysicalType
	vType value.PhysicalType
}

type InnerHash struct {
	operator executor.Operator
	ops      innerHashState

	lchild executor.Operator
	rchild executor.Operator
}

func NewInnerJoin(kType, vType value.PhysicalType, lchild, rchild executor.Operator) *InnerHash {
	var pi = new(InnerHash)
	pi.operator.Op_type = executor.PhysicalHashJoin
	pi.lchild = lchild
	pi.rchild = rchild
	pi.ops.kType = kType
	pi.ops.vType = vType
	return pi
}

func (i *InnerHash) InitLocalStateForMaterialize() (any, error) {
	i.ops.l = sync.Mutex{}
	

	return i.ops, nil
}

func (i *InnerHash) IsPipelineBreaker() bool {
	return true
}
