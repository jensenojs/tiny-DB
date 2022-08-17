package vector

import "tiny-db/src/common/value"

type VectorType int
const (
	ConstantVectorType VectorType = iota
	FlatVectorType      
)

type Vector struct {
	data        interface{}
	size        int
	phy_type    value.PhysicalType
	vector_type VectorType
}

func newFlatVector(column any, size int, data_type value.PhysicalType) *Vector {
	return &Vector{
		data:        column,
		size:        size,
		phy_type:    data_type,
		vector_type: FlatVectorType,
	}
}

func NewVectorInt32(column []int32) *Vector {
	return newFlatVector(column, len(column), value.PhysicalInt32)
}

func NewVectorString(column []string) *Vector {
	return newFlatVector(column, len(column), value.PhysicalString)
}

func NewConstantVector(v value.Value, size int) *Vector {
	value_type := v.GetType()
	if value_type == value.PhysicalInt32 {
		int_value := v.(*value.IntValue)
		return &Vector{
			data:        []int32{int32(*int_value)},
			size:        size,
			phy_type:    value_type,
			vector_type: ConstantVectorType,
		}
	} else if value_type == value.PhysicalString {
		string_value := v.(*value.StringValue)
		return &Vector{
			data:        []string{string(*string_value)},
			size:        size,
			phy_type:    value_type,
			vector_type: ConstantVectorType,
		}
	} else {
		panic("Unsupport type!")
	}
}

func (v *Vector) GetType() value.PhysicalType {
	return v.phy_type
}

func (v *Vector) GetVectorType() VectorType {
	return v.vector_type
}

func (v *Vector) IsConatantVector() bool {
	return v.GetVectorType() == ConstantVectorType
}

func (v *Vector) Size() int {
	return v.size
}

func (v *Vector) GetRawColumn() any {
	return v.data
}

func (v *Vector) Reference(other *Vector) {
	v.data = other.data
	v.size = other.size
}
