package vector

import (
	"errors"
	"tiny-db/src/common/value"
)

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

func (v *Vector) GetValue(idx int) (any, error) {
	if idx < 0 || idx > v.size {
		return nil, errors.New("invalid range")
	}
	switch v.phy_type {
	case value.PhysicalString:
		val := v.data.([]string)
		return val[idx], nil
	case value.PhysicalInt32:
		val := v.data.([]int32)
		return val[idx], nil
	default:
		return nil, errors.New("unsupported value type")
	}
}

func (v *Vector) GetValuesByRange(start, count int) (any, error) {
	if start < 0 || start+count > v.size {
		return nil, errors.New("invalid range")
	}
	switch v.phy_type {
	case value.PhysicalString:
		val := v.data.([]string)
		return val[start : start+count], nil
	case value.PhysicalInt32:
		val := v.data.([]int32)
		return val[start : start+count], nil
	default:
		return nil, errors.New("unsupported value type")
	}
}

func (v *Vector) Dup(s *Vector, start, count int) error {
	if start < 0 || start+count > s.size {
		return errors.New("invalid range")
	}

	if v.phy_type != s.phy_type {
		return errors.New("dup vector with different type")
	}

	switch v.phy_type {
	case value.PhysicalString:
		v.data = append(v.data.([]string), s.data.([]string)[start:start+count]...)
	case value.PhysicalInt32:
		v.data = append(v.data.([]int32), s.data.([]int32)[start:start+count]...)
	default:
		return errors.New("unsupported value type")
	}
	return nil
}
