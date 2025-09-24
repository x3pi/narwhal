package network

type Handler interface {
	HandleRequest(Request) error
}
