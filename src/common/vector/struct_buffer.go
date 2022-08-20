package vector

import "tiny-db/src/common/types"

func NewStructBuffer(phyTypes []types.PhysicalType) *StructBuffer {
	res := &StructBuffer{
		childs: make([]*Vector, len(phyTypes)),
	}

	for i := 0; i < len(phyTypes); i++ {
		res.childs[i] = NewVector(phyTypes[i])
	}
	return res
}

func (b *StructBuffer) GetVectorBufferType() VectorBufferType {
	return STRUCT_VECTOR_BUFFER
}

func (b *StructBuffer) GetChilds() []*Vector {
	return b.childs
}
