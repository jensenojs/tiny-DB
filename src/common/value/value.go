package value

import "tiny-db/src/common/types"

type Value interface {
	Size() uintptr

	ToString() string

	GetType() types.PhysicalType
}
