package vector

func NewDictBuffer(selVec *SelectionVector) *DictVectorBuffer {
	return &DictVectorBuffer{
		selVec,
	}
}

func (b *DictVectorBuffer) GetVectorBufferType() VectorBufferType {
	return DICTIONARY_VECTOR_BUFFER
}

func (b DictVectorBuffer) GetSelVec() *SelectionVector {
	return b.SelVec
}
