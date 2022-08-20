package vector

import "tiny-db/src/common/value"

type VectorBufferType int

const (
	FLAT_VECTOR_BUFFER VectorBufferType = iota
	//! For Slice
	DICTIONARY_VECTO_BUFFER
	//! Hold a child vector
	CHILD_VECTOR_BUFFER
	//! Cache buffer for reset DataChunk
	CACHE_VECTOR_BUFFER
)

type VectorBuffer interface {
	//! Return vector buffer type
	GetVectorBufferType() VectorBufferType
}

type FlatVectorBuffer struct {
	Data []byte
}

type DictVectorBuffer struct {
	SelVec *SelectionVector
}

type ChildVectorBuffer struct {
	ChildVec *Vector
}

type Vector struct {
	Column     any
	buffer     VectorBuffer
	extra      VectorBuffer
	Validality *ValidalityMask
	phyType    value.PhysicalType
	Type       VectorType
}

type VectorCache struct {
	buffer  VectorBuffer
	phyType value.PhysicalType
}

type CacheBuffer struct {
	Data    []byte
	phyType value.PhysicalType
}

/*
func NewVectorBufferWithSize(count uint64, phyType value.PhysicalType) *VectorBuffer {
	// Create int32 array
	if phyType == value.INT32 {
		data := make([]int32, count)
		return &VectorBuffer{
			data:       data,
			bufferType: COMMON_VECTOR_BUFFER,
		}
	}
	// Create string array
	if phyType == value.STRING {
		data := make([]string, count)
		return &VectorBuffer{
			data:       data,
			bufferType: COMMON_VECTOR_BUFFER,
		}
	}
	panic("Unsupport type")
}

func (v *VectorBuffer) GetData() any {
	return v.data
}

func (v *VectorBuffer) SetData(b any) {
	v.data = b
}

func (v *VectorBuffer) GetBufferType() VectorBufferType {
	return v.bufferType
}

// Friendly Function
func (v *VectorBuffer) INTs() []int32 {
	return v.data.([]int32)
}

func (v *VectorBuffer) STRINGs() []string {
	return v.data.([]string)
}

func NewDictVectorBuffer() *DictVectorBuffer {
	res := &DictVectorBuffer{
		sel_vec: nil,
	}
	res.data = nil
	res.bufferType = DICTIONARY_VECTO_BUFFER
	return res
}

func NewDictVectorBufferWithSize(count uint64, phyType value.PhysicalType, sel_vec *vector.SelectionVector) *DictVectorBuffer {
	// Create int32 array
	if phyType == value.INT32 {
		data := make([]int32, count)
		res := NewDictVectorBuffer()
		res.data = data
		res.sel_vec = sel_vec
		return res
	}

	// Create string array
	if phyType == value.STRING {
		data := make([]string, count)
		res := NewDictVectorBuffer()
		res.data = data
		res.sel_vec = sel_vec
		return res
	}

	panic("Unsupport type")
}

func (d *DictVectorBuffer) GetSelVec() *vector.SelectionVector {
	return d.sel_vec
}

func (d *DictVectorBuffer) SetSelVec(sel_vec *vector.SelectionVector) {
	d.sel_vec = sel_vec
}
*/
