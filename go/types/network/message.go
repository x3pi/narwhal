package network

import (
	"github.com/ethereum/go-ethereum/common"
	"google.golang.org/protobuf/reflect/protoreflect"

	cm "github.com/meta-node-blockchain/meta-node/pkg/common"
)

// Message định nghĩa interface cho một tin nhắn mạng.
type Message interface {
	Marshaler
	String() string
	Unmarshal(protoStruct protoreflect.ProtoMessage) error
	Command() string
	Body() []byte
	Pubkey() cm.PublicKey
	Sign() cm.Sign
	ToAddress() common.Address
	ID() string
	Version() string // Thêm phương thức mới vào interface
}
