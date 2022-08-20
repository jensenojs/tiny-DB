package value

import "strconv"

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

func (v *IntValue) GetType() PhysicalType {
	return INT32
}
