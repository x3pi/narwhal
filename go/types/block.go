package types

import (
	e_common "github.com/ethereum/go-ethereum/common"

	pb "github.com/meta-node-blockchain/meta-node/pkg/proto"
)

type Block interface {
	// general
	Marshal() ([]byte, error)
	Unmarshal([]byte) error
	Proto() *pb.Block
	FromProto(*pb.Block)

	Header() BlockHeader
	Transactions() []e_common.Hash
	ExecuteSCResults() []ExecuteSCResult
}

type BlockHeader interface {
	Marshal() ([]byte, error)
	Unmarshal([]byte) error
	Proto() *pb.BlockHeader
	FromProto(*pb.BlockHeader)

	Hash() e_common.Hash
	LastBlockHash() e_common.Hash
	BlockNumber() uint64
	AccountStatesRoot() e_common.Hash
	SetAccountStatesRoot(e_common.Hash)
	ReceiptRoot() e_common.Hash
	TimeStamp() uint64
	LeaderAddress() e_common.Address
	AggregateSignature() []byte
	String() string
	TransactionsRoot() e_common.Hash
}

type ConfirmedBlockData interface {
	Header() BlockHeader
	Receipts() []Receipt
	BranchStateRoot() e_common.Hash
	ValidatorSigns() map[e_common.Address][]byte
	SetBranchStateRoot(e_common.Hash)
	Marshal() ([]byte, error)
	Unmarshal([]byte) error
	Proto() *pb.ConfirmedBlockData
	FromProto(*pb.ConfirmedBlockData)
}
