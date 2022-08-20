package vector

import "tiny-db/src/common/value"

func NewCacheBuffer(phyType value.PhysicalType) *CacheBuffer {
	switch phyType {
	case value.INT32:
		res := &CacheBuffer{
			Data:    make([]byte, 4*COMMON_VECTOR_SIZE),
			phyType: phyType,
		}
		return res
	case value.STRING:
		panic("TODO(lokax): ")
	default:
		panic("Unsupport phyType!")

	}
}

func (b *CacheBuffer) GetVectorBufferType() VectorBufferType {
	return CACHE_VECTOR_BUFFER
}
