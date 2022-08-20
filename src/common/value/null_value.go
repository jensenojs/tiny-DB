package value

import "tiny-db/src/common/types"

type NullValue struct {
	phyType types.PhysicalType
}

func NewNullValue(phyType types.PhysicalType) Value {
	res := &NullValue{phyType}
	return res
}

func (v *NullValue) Size() uintptr {
	// TODO(lokax): need this functiton ?
	return 0
}

func (v *NullValue) ToString() string {
	return "NULL"
}

func (v *NullValue) GetType() types.PhysicalType {
	return v.phyType
}
