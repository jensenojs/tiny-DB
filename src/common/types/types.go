package types

type PhysicalType int

const (
	INT32 PhysicalType = iota
	STRING
	STRUCT // Need More Info
)

type StringVec struct {
	Data   []byte
	Offset []int
	Len    []int
}
