package value

type PhysicalType int

// TODO(lokax): Move to package types
const (
	INT32 PhysicalType = iota
	STRING
)

type Value interface {
	Size() uintptr

	ToString() string

	GetType() PhysicalType
}
