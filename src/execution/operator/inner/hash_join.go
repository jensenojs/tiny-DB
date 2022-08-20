package operator

import (
	"sync"
	"tiny-db/src/common/types"
	"tiny-db/src/execution/executor"
)

type innerHashState struct {
	l     sync.Mutex
	m     map[int][]int
}

type InnerHash struct {
	executor.Operator

	kType types.PhysicalType
	vType types.PhysicalType
	lchild executor.Operator
	rchild executor.Operator
}

func NewInnerJoin(kType, vType types.PhysicalType, lchild, rchild executor.Operator) *InnerHash {
	var pi = new(InnerHash)
	pi.Op_type = executor.PhysicalHashJoin
	pi.kType = kType
	pi.vType = vType
	pi.lchild = lchild
	pi.rchild = rchild
	return pi
}

func (i *InnerHash) InitLocalStateForMaterialize() (any, error) {
	s :=  &innerHashState{

		// TODO(Jensen): Hard code for now, Need Customized hash map 
		m : make(map[int][]int),
	}	
	return s, nil
}

func (i *InnerHash) IsPipelineBreaker() bool {
	return true
}
