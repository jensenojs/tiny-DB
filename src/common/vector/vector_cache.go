package vector

import (
	"tiny-db/src/common/types"
	"tiny-db/src/common/value"
)

func NewVectorCache(phyType value.PhysicalType) *VectorCache {
	res := &VectorCache{
		buffer:  NewCacheBuffer(phyType),
		phyType: phyType,
	}
	return res
}

func (b *VectorCache) ResetVector(v *Vector) {
	// clear mask
	v.Validality = nil
	// reset to flat vector type
	v.Type = FLAT_VECTOR

	v.buffer = b.buffer

	v.extra = nil

	switch v.phyType {
	case value.INT32:
		v.Column = types.DecodeToInt32(v.buffer.(*CacheBuffer).Data)
	case value.STRING:
		panic("TODO(lokax): ")
	default:
		panic("Unsupport type!")
	}
}
