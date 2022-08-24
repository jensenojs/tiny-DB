package operator

import (
	"sync"
	"tiny-db/src/common/vector"
	"tiny-db/src/execution/executor"
	"tiny-db/src/storage"
)

type innerHashState struct {
	l sync.Mutex  // this mutex is used in join state in parallel build the hash table.
	b *storage.DataChunk // build table

	// TODO(Jensen) : Need a better way to implement hash map, to support different types of Value and multiple keys
	// For now we have to use value.ToString() as keys, and the value of map is used to record the idxs of key appear
	m map[string][]int
}

type InnerHash struct {
	executor.Operator

	lchild executor.Operator
	rchild executor.Operator
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
		m: make(map[string][]int),
	}
	return s, nil
}

// the Build state of inner hash join.
func (i *InnerHash) Materialize(build *storage.DataChunk, state any) error {
	ops := (state).(innerHashState)
	ops.b = build
	hashMap := ops.m
	for i := 0; i < build.Count(); i++ {
		key := build.Cols[0].GetValue(i).ToString()
		// TODO(Jensen): Need Equal Conditional Expression to build the table.
		hashMap[key] = append(hashMap[key], i)
	}

	return nil
}

// the Probe state of inner hash join.
func (i *InnerHash) Execute(probe, result *storage.DataChunk, state any) error {
	ops := (state).(innerHashState)
	hashMap := ops.m
	sels := vector.NewSelVector()

	resultCount := 0
	for i := 0; i < probe.Count(); i++ {
		// TODO(Jensen) : Need expression calculation to decide whether this key in hashMap
		// also need to decide which column
		// chosen to be key, now just simply hard code and use first column as key.
		key := probe.Cols[0].GetValue(i).ToString()
		if idxs, ok := hashMap[key]; ok {
			sels.SetIndex(resultCount, idxs[0])
			resultCount++
		}
	}

	// Need to concate two datachunk but not SLice from input.
	result.Slice(probe, sels, resultCount)

	return nil
}

func (i *InnerHash) IsPipelineBreaker() bool {
	return true
}
