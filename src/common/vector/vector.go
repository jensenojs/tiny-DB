package vector

import (
	"tiny-db/src/common/types"
	"tiny-db/src/common/value"
)

type VectorType int

// Vector Size
const COMMON_VECTOR_SIZE int = 1024

const (
	CONSTANT_VECTOR VectorType = iota
	FLAT_VECTOR
	DICTIONARY_VECTOR
)

//! Create a vector, it's size is 1024
func NewVector(phyType types.PhysicalType) *Vector {
	switch phyType {
	case types.INT32:
		buffer := NewFlatBuffer(4 * COMMON_VECTOR_SIZE)
		col := types.DecodeToInt32(buffer.Data)
		return &Vector{
			Column:     col,
			buffer:     buffer,
			extra:      nil,
			Validality: NewValidalityMask(),
			phyType:    phyType,
			Type:       FLAT_VECTOR,
		}

	case types.STRING:
		// TODO(lokax):
	}
	panic("Unsupport now!")
}

func NewFromCache(cache *VectorCache) *Vector {
	res := &Vector{}
	cache.ResetVector(res)
	return res
}

func GetColumn[T any](v *Vector) []T {
	return v.Column.([]T)
}

func (v *Vector) GetValue(idx int) value.Value {
	index := idx
	valueVec := v
	search := true
	for search {
		switch v.Type {
		case CONSTANT_VECTOR:
			index = 0
			search = false
		case FLAT_VECTOR:
			search = false
		case DICTIONARY_VECTOR:
			valueVec = v.GetChild() // safe
			index = v.GetSelVec().GetIndex(index)
		}
	}
	switch v.phyType {
	case types.INT32:
		rawVec := GetColumn[int32](valueVec)
		mask := v.Validality
		if !mask.RowIsValid(index) {
			return value.NewNullValue(types.INT32)
		}
		return value.NewInt(rawVec[index])
	case types.STRING:
		panic("TOOD(lokax): ")
	case types.STRUCT:
		panic("TODO(lokax): ")
	default:
		panic("Unsupport type")
	}
}

func (v *Vector) GetSelVec() *SelectionVector {
	if v.Type != DICTIONARY_VECTOR {
		panic("must be dictionary vector")
	}
	return v.buffer.(*DictVectorBuffer).GetSelVec()
}

func (v *Vector) GetChild() *Vector {
	if v.Type != DICTIONARY_VECTOR {
		panic("must be dictionary vector")
	}
	return v.extra.(*ChildVectorBuffer).ChildVec
}

func (v *Vector) StructChilds() []*Vector {
	if v.phyType != types.STRUCT {
		panic("must be struct phyType")
	}
	return v.extra.(*StructBuffer).GetChilds()
}

//! Creates a reference to a slice of the other vector
func (v *Vector) Slice(sel *SelectionVector, count int) {
	switch v.Type {
	case CONSTANT_VECTOR:
		// do nothing
		return
	case FLAT_VECTOR:
		newVec := v.ShallowCopy()
		childBuffer := NewChildBuffer(newVec)
		dictBuffer := NewDictBuffer(sel)
		v.Type = DICTIONARY_VECTOR
		v.buffer = dictBuffer
		v.extra = childBuffer
	case DICTIONARY_VECTOR:
		bf := v.buffer.(*DictVectorBuffer)
		vsel := bf.GetSelVec()
		// Merge selection vector
		newSel := vsel.Slice(sel, count)
		dictBf := NewDictBuffer(newSel)
		childVec := v.ShallowCopy()
		v.buffer = dictBf
		v.extra = NewChildBuffer(childVec)
	default:
		panic("Unsupport vector type!")
	}
}

func (v *Vector) Reference(other *Vector) {
	if v.phyType != other.phyType {
		panic("Type incorrect!")
	}
	v.Type = other.Type
	v.buffer = other.buffer
	v.extra = other.extra
	v.Validality = other.Validality
	v.Column = other.Column
}

func (v *Vector) ShallowCopy() *Vector {
	newVec := &Vector{
		phyType: v.phyType,
	}
	newVec.Reference(v)
	return newVec
}

/*
func newFlatVector(column any, size int, data_type value.PhysicalType) *Vector {
	return &Vector{
		buffer:      nil,
		data:        column,
		size:        size,
		phy_type:    data_type,
		vector_type: FLAT_VECTOR,
		validality:  NewValidalityMask(),
	}
}

func NewVectorInt32(column []int32) *Vector {
	return newFlatVector(column, len(column), value.INT32)
}

func NewVectorString(column []string) *Vector {
	return newFlatVector(column, len(column), value.STRING)
}
*/

/*
func NewConstantVector(v value.Value, size int) *Vector {
	value_type := v.GetType()
	if value_type == value.INT32 {
		int_value := v.(*value.IntValue)
		return &Vector{
			data:        []int32{int32(*int_value)},
			size:        size,
			phy_type:    value_type,
			vector_type: CONSTANT_VECTOR,
		}
	} else if value_type == value.STRING {
		string_value := v.(*value.StringValue)
		return &Vector{
			data:        []string{string(*string_value)},
			size:        size,
			phy_type:    value_type,
			vector_type: CONSTANT_VECTOR,
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
	return v.GetVectorType() == CONSTANT_VECTOR
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

// TODO(lokax): Check vector type, make this function more safe
// if vector type is constant vector, we just return v.data([]int32)[0] instead of ...[idx]
func (v *Vector) GetValue(idx int) (any, error) {
	if idx < 0 || idx > v.size {
		return nil, errors.New("invalid range")
	}
	switch v.phy_type {
	case value.STRING:
		val := v.data.([]string)
		return val[idx], nil
	case value.INT32:
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
	case value.STRING:
		val := v.data.([]string)
		return val[start : start+count], nil
	case value.INT32:
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
	case value.STRING:
		v.data = append(v.data.([]string), s.data.([]string)[start:start+count]...)
	case value.INT32:
		v.data = append(v.data.([]int32), s.data.([]int32)[start:start+count]...)
	default:
		return errors.New("unsupported value type")
	}
	return nil
}
*/
