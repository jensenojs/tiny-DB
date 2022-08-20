package value

import (
	"strconv"
	"tiny-db/src/common/types"
)

type IntValue int32

func NewInt(v int32) Value {
	res := IntValue(v)
	return &res
}

func (v *IntValue) Size() uintptr {
	return 4
}

func (v *IntValue) ToString() string {
	return strconv.FormatInt(int64(*v), 10)
}

func (v *IntValue) GetType() types.PhysicalType {
	return types.INT32
}
