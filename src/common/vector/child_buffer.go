package vector

func NewChildBuffer(v *Vector) *ChildVectorBuffer {
	return &ChildVectorBuffer{ChildVec: v}
}

func (b *ChildVectorBuffer) GetVectorBufferType() VectorBufferType {
	return CHILD_VECTOR_BUFFER
}
