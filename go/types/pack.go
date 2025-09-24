package types

import (
	pb "github.com/meta-node-blockchain/meta-node/pkg/proto"
)

type Pack interface {
	// general
	Marshal() ([]byte, error)
	Unmarshal([]byte) error
	Proto() *pb.Pack
	FromProto(*pb.Pack)

	// getter
	Timestamp() uint64
	Id() string
	Transactions() []Transaction
	AggregateSign() []byte
	ValidSign() bool
	NewVerifyPackSignRequest() VerifyPackSignRequest
}

type VerifyPackSignRequest interface {
	Unmarshal([]byte) error
	Marshal() ([]byte, error)
	Id() string
	PublicKeys() [][]byte
	Hashes() [][]byte
	AggregateSign() []byte
	Valid() bool
	Proto() *pb.VerifyPackSignRequest
}

type VerifyPackSignResult interface {
	Unmarshal(b []byte) error
	Marshal() ([]byte, error)
	PackId() string
	Valid() bool
}

type PacksFromLeader interface {
	Packs() []Pack
	Transactions() []Transaction
	BlockNumber() uint64
	TimeStamp() uint64
	IsValidSign() bool
	Marshal() ([]byte, error)
	Unmarshal([]byte) error
}
