package network

import (
	e_common "github.com/ethereum/go-ethereum/common"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type Marshaler interface {
	Marshal() ([]byte, error)
}

type MessageSender interface {
	SendMessage(
		connection Connection,
		command string,
		pbMessage protoreflect.ProtoMessage,
	) error

	SendBytes(
		connection Connection,
		command string,
		b []byte,
	) error

	BroadcastMessage(
		mapAddressConnections map[e_common.Address]Connection,
		command string,
		marshaler Marshaler,
	) error
}
