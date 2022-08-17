package main

import (
	// "tiny-db/src/common/value"
	// "tiny-db/src/common/vector"
)

type Plugin interface {
	Init()
	Shutdown()
	Handle()
}

type DefaultPlugin struct{}

func (p DefaultPlugin) Init()     {println("default init")}
func (p DefaultPlugin) Shutdown() {println("default shutdown")}
func (p DefaultPlugin) Handle()   {println("default handle")}

// Foo only runs init()
type FooPlugin struct {
	DefaultPlugin
}

func (p FooPlugin) Init() {
	println("foo init")
}

func (p FooPlugin) Handle() {
	println("foo handle")
}


func main() {
	println("Hello world!")

	// int32_column := []int32{1, 2, 3, 4, 5}
	// int32_vector := vector.NewVectorInt32(int32_column)
	// if int32_vector.GetType() != value.PhysicalInt32 {
	// 	println("error type, should be int32")
	// }
	// if int32_vector.Size() != 5 {
	// 	println("incorrect size")
	// }
	// raw_data := int32_vector.GetRawColumn()
	// int_raw_data := raw_data.([]int32)
	// for i := 0; i < int32_vector.Size(); i++ {
	// 	println("value = ", int_raw_data[i])
	// }

	// varchar_value := value.NewStringValue("lyys")
	// constant_vector := vector.NewConstantVector(varchar_value, 5)
	// if constant_vector.GetType() != value.PhysicalString {
	// 	println("error type, should be int32")
	// }
	// if constant_vector.Size() != 5 {
	// 	println("incorrect size")
	// }
	// constant_raw_data := constant_vector.GetRawColumn()
	// varchar_raw_data := constant_raw_data.([]string)
	// for i := 0; i < constant_vector.Size(); i++ {
	// 	// idx := constant_vector.IsConatantVector() ? 0 : i
	// 	println("value = ", varchar_raw_data[0])
	// }
	
	// var p Plugin = &FooPlugin{}
	var p FooPlugin
	p.Init()
	p.Handle()
	p.Shutdown()
}
