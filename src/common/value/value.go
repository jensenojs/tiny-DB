package value

type PhysicalType int

const (
	PhysicalInt32  PhysicalType = 0
	PhysicalString PhysicalType = 1
)

type Value interface {
	Size() uintptr

	ToString() string

	GetType() PhysicalType
}
