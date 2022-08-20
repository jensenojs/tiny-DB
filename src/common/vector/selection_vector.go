package vector

type sel_t int

// use dictionary
type SelectionVector struct {
	sels []sel_t
}

func NewSelVector() *SelectionVector {
	return &SelectionVector{}
}

func (s *SelectionVector) GetIndex(idx int) int {
	if s.sels == nil {
		return idx
	}
	return int(s.sels[idx])
}

func (s *SelectionVector) SetIndex(idx, newIdx int) {
	if s.sels == nil {
		s.intialize(COMMON_VECTOR_SIZE)
	}
	s.sels[idx] = sel_t(newIdx)
}

func (s *SelectionVector) Slice(other *SelectionVector, count int) *SelectionVector {
	newSel := NewSelVector()
	newSel.intialize(count) // right?
	for i := 0; i < count; i++ {
		newIdx := other.GetIndex(i)
		idx := s.GetIndex(newIdx)
		newSel.SetIndex(i, idx)
	}
	return newSel
}

func (s *SelectionVector) intialize(count int) {
	s.sels = make([]sel_t, count)
}
