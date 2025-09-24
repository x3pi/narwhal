package network

type Request interface {
	Message() Message
	Connection() Connection
	Reset(connection Connection, message Message)
}
