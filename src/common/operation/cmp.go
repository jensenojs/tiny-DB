package operation

import (
	"tiny-db/src/common/types"
	"tiny-db/src/common/vector"
)

type Sel = vector.SelectionVector

func Equal[T types.Numeric](lhs, rhs []T, res []bool, sel *Sel, count int) {
	if sel == nil {
		for i := 0; i < count; i++ {
			res[i] = lhs[i] == rhs[i]
		}
	} else {
		for i := 0; i < count; i++ {
			realIdx := sel.GetIndex(i)
			res[i] = lhs[realIdx] == rhs[realIdx]
		}
	}
}

func NotEqual[T types.Numeric](lhs, rhs []T, res []bool, sel *Sel, count int) {
	if sel == nil {
		for i := 0; i < count; i++ {
			res[i] = lhs[i] != rhs[i]
		}
	} else {
		for i := 0; i < count; i++ {
			realIdx := sel.GetIndex(i)
			res[i] = lhs[realIdx] != rhs[realIdx]
		}
	}
}

func LessThan[T types.Numeric](lhs, rhs []T, res []bool, sel *Sel, count int) {
	if sel == nil {
		for i := 0; i < count; i++ {
			res[i] = lhs[i] < rhs[i]
		}
	} else {
		for i := 0; i < count; i++ {
			realIdx := sel.GetIndex(i)
			res[i] = lhs[realIdx] < rhs[realIdx]
		}
	}
}

func GreaterThan[T types.Numeric](lhs, rhs []T, res []bool, sel *Sel, count int) {
	LessThan(rhs, lhs, res, sel, count)
}
