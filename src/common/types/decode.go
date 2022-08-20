package types

import "unsafe"

func DecodeToInt32(b []byte) []int32 {
	return *(*[]int32)(unsafe.Pointer(&b))
}
