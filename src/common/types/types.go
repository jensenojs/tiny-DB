package types

type PhysicalType int

const (
	INT32 PhysicalType = iota
	STRING
	STRUCT // Need More Info
	BOOL
)

type StringVec struct {
	Data   []byte
	Offset []int
	Len    []int
}

type Ints interface {
	int8 | int16 | int32 | int64
}

type UInts interface {
	uint8 | uint16 | uint32 | uint64
}

type Float interface {
	float32 | float64
}

type Numeric interface {
	Ints | UInts | Float
}
