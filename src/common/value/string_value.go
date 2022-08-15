package value

import "unsafe"

type StringValue string

func NewStringValue(v string) Value {
	res := StringValue(v)
	return &res
}

func (v *StringValue) Size() uintptr {
	return unsafe.Sizeof(v) + uintptr(len(*v))
}

func (v *StringValue) ToString() string {
	return string(*v)
}

func (v *StringValue) GetType() PhysicalType {
	return PhysicalString
}
