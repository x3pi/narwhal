package types

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/holiman/uint256"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type Entry interface {
	// general
	Marshal() ([]byte, error)
	Unmarshal([]byte) error
	Proto() protoreflect.ProtoMessage
	FromProto(protoreflect.ProtoMessage)
	String() string

	// getter
	ProtoWithBlockNumber() protoreflect.ProtoMessage
	BlockNumber() *uint256.Int
	Hash() common.Hash
	Packs() []Pack
	Time() uint64
	// setter
}
