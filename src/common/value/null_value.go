package value

type NullValue struct {
	phyType PhysicalType
}

func NewNullValue(phyType PhysicalType) Value {
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

func (v *NullValue) GetType() PhysicalType {
	return v.phyType
}
