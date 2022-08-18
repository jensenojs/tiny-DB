package value

type PhysicalType int

const (
	PhysicalInt32  PhysicalType = iota
	PhysicalString
)

type Value interface {
	Size() uintptr

	ToString() string

	GetType() PhysicalType
}
