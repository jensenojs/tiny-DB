package vector

import (
	"tiny-db/src/common/types"
)

func NewCacheBuffer(phyType types.PhysicalType) *CacheBuffer {
	switch phyType {
	case types.INT32:
		res := &CacheBuffer{
			Data:    make([]byte, 4*COMMON_VECTOR_SIZE),
			phyType: phyType,
		}
		return res
	case types.STRING:
		panic("TODO(lokax): ")
	default:
		panic("Unsupport phyType!")

	}
}

func (b *CacheBuffer) GetVectorBufferType() VectorBufferType {
	return CACHE_VECTOR_BUFFER
}
