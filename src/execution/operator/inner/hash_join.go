package operator

import (
	"sync"
	"tiny-db/src/common/vector"
	"tiny-db/src/execution/executor"
	"tiny-db/src/storage"
)

type innerHashState struct {
	l sync.Mutex  // this mutex is used in join state in parallel build the hash table.
	Sources []*storage.DataChunk
	// TODO(Jensen) : Need a better way to implement hash map, to support different types of Value and multiple keys
	// For now we have to use value.ToString() as keys, and the value of map is used to record the idxs of key appear
	Map map[string][]int
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

func (i *InnerHash) InitLocalStateForMaterialize(mChunks []*storage.DataChunk) (any, error) {
	s := &innerHashState{
		// TODO(Jensen): Hard code for now, Need Customized hash map
		Sources: mChunks,
		Map: make(map[string][]int),
	}
	return s, nil
}

// the Build state of inner hash join.
func (i *InnerHash) Materialize(input *storage.DataChunk, state any) error {
	ops := (state).(innerHashState)
	hashMap := ops.Map
	for i := 0; i < input.Count(); i++ {
		key := input.Cols[0].GetValue(i).ToString()
		// TODO(Jensen): Need Equal Conditional Expression to build the table.
		hashMap[key] = []int{i}
	}

	return nil
}

// the Probe state of inner hash join.
func (i *InnerHash) Execute(input, output *storage.DataChunk, state any) error {
	ops := (state).(innerHashState)
	hashMap := ops.Map
	sels := vector.NewSelVector()

	resultCount := 0
	for i := 0; i < input.Count(); i++ {
		// TODO(Jensen) : Need expression Calculation to decide whether this key in hashMap, also need to decide which column
		// chosen to be key, now just simply hard code.
		key := input.Cols[0].GetValue(i).ToString()
		if idxs, ok := hashMap[key]; ok {
			sels.SetIndex(resultCount, idxs[0])
			resultCount++
		}
	}
	output.Slice(input, sels, resultCount)

	return nil
}

func (i *InnerHash) IsPipelineBreaker() bool {
	return true
}
