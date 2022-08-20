package vector

func NewFlatBuffer(size uint64) *FlatVectorBuffer {
	return &FlatVectorBuffer{
		Data: make([]byte, size),
	}
}

func (b *FlatVectorBuffer) GetVectorBufferType() VectorBufferType {
	return FLAT_VECTOR_BUFFER
}
