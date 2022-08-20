package vector

type sel_t uint32

// use dictionary
type SelectionVector struct {
	sels []sel_t
}

func NewSelVector() *SelectionVector {
	return &SelectionVector{}
}

func (s *SelectionVector) GetIndex(idx uint64) uint64 {
	if s.sels == nil {
		return idx
	}
	return uint64(s.sels[idx])
}

func (s *SelectionVector) SetIndex(idx, newIdx uint64) {
	if s.sels == nil {
		s.intialize(COMMON_VECTOR_SIZE)
	}
	s.sels[idx] = sel_t(newIdx)
}

func (s *SelectionVector) Slice(other *SelectionVector, count uint64) *SelectionVector {
	var i uint64 = 0
	newSel := NewSelVector()
	newSel.intialize(count) // right?
	for ; i < count; i++ {
		newIdx := other.GetIndex(i)
		idx := s.GetIndex(newIdx)
		newSel.SetIndex(i, idx)
	}
	return newSel
}

func (s *SelectionVector) intialize(count uint64) {
	s.sels = make([]sel_t, count)
}
