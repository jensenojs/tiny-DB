package main

type Plugin interface {
	Init()
	Shutdown()
	Handle()
}

func main() {
	println("Hello world!")
}
