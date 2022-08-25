package operator

import (
	"errors"
	"sync"
	"tiny-db/src/common/types"
	"tiny-db/src/common/value"
	"tiny-db/src/common/vector"
	"tiny-db/src/execution/executor"
	"tiny-db/src/storage"
)

type mapValue struct {
	ithChunk   int // mark which chunk store the value
	idxInChunk int // mark the idx in this chunk
}

type innerHashState struct {
	l  sync.Mutex           // this mutex is used in join state in parallel build the hash table.
	bs []*storage.DataChunk // As we can't guarantee that the data that the build table can match is in one chunk, so we need store it in slice.

	// TODO(Jensen) : Need a better way to implement hash map, to support different types of Value and multiple keys
	// For now we have to use value.ToString() as keys, and the value of map is used to record the idxs of key appear
	m map[string]*mapValue
}

type InnerHash struct {
	executor.Operator

	lchild executor.Operator
	rchild executor.Operator
}

func newMapValue(ith, idx int) *mapValue {
	return &mapValue{
		ithChunk:   ith,
		idxInChunk: idx,
	}
}

func NewInnerJoin(mChunk []*storage.DataChunk, lchild, rchild executor.Operator) *InnerHash {
	var pi = new(InnerHash)
	pi.Op_type = executor.PhysicalHashJoin
	pi.lchild = lchild
	pi.rchild = rchild
	return pi
}

func (i *InnerHash) InitLocalStateForMaterialize() (any, error) {
	s := &innerHashState{
		// TODO(Jensen): Hard code for now, Need Customized hash map
		m:  make(map[string]*mapValue),
		bs: make([]*storage.DataChunk, 0),
	}
	return s, nil
}

// the Build state of inner hash join.
func (i *InnerHash) Materialize(build *storage.DataChunk, state any) error {
	ops := (state).(*innerHashState)
	hashMap := ops.m
	ithChunk := len(ops.bs)
	ops.bs = append(ops.bs, build)

	for i := 0; i < build.Count(); i++ {
		// TODO(Jensen): 1. Equal Conditional Expression to build the table. 2. select which col to build
		key := build.Cols[0].GetValue(i).ToString()
		value := newMapValue(ithChunk, i)
		hashMap[key] = value
	}

	return nil
}

// the Probe state of inner hash join.
func (op *InnerHash) Execute(probe, result *storage.DataChunk, state any) error {
	ops := (state).(*innerHashState)
	buildChunks := ops.bs
	hashMap := ops.m
	probeSels := vector.NewSelVector()
	buildSels := make([]*mapValue, 0)

	// probe
	resultCount := 0
	for i := 0; i < probe.Count(); i++ {
		// TODO(Jensen) : Need expression calculation to decide whether this key in hashMap
		// also need to decide which column
		// chosen to be key, now just simply hard code and use first column as key.
		key := probe.Cols[0].GetValue(i).ToString()
		if mv, ok := hashMap[key]; ok {
			probeSels.SetIndex(resultCount, i)
			buildSels = append(buildSels, mv)
			resultCount++
		}
	}

	// Hard code : Currently join all column from probe and build table.
	// probe table
	for i := 0; i < probe.ColumnCount(); i++ {
		pcol := probe.Cols[i]
		result.Cols[i].SliceOther(pcol, probeSels, resultCount)
	}

	// build table
	bias := probe.ColumnCount()
	for _, mv := range buildSels {
		ith, idx := mv.ithChunk, mv.idxInChunk
		for i := 0; i < buildChunks[0].ColumnCount(); i++ {
			bcol := result.Cols[i+bias]
			bv := buildChunks[ith].Cols[i].GetValue(idx)
			ptype := bcol.GetPhyType()
			if ptype == types.INT32 {
				rawVec := vector.GetColumn[int32](bcol)
				rawVec[idx] = int32(*bv.(*value.IntValue))
			} else {
				return errors.New(op.Op_type.String() + ": Unspport phyType")
			}

		}
	}

	result.SetCount(resultCount)

	return nil
}

func (i *InnerHash) IsPipelineBreaker() bool {
	return true
}
