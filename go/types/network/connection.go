package network

import (
	"net"

	"github.com/ethereum/go-ethereum/common"
)

type Connection interface {
	// getter
	Address() common.Address
	ConnectionAddress() (string, error)
	TcpLocalAddr() net.Addr
	TcpRemoteAddr() net.Addr

	RequestChan() (chan Request, chan error)
	Type() string
	String() string
	RemoteAddrSafe() string
	RemoteAddr() string
	// setter
	Init(common.Address, string)
	SetRealConnAddr(realConnAddr string)

	// other
	SendMessage(message Message) error
	Connect() error
	Disconnect() error
	IsConnect() bool
	ReadRequest()
	Clone() Connection
}
