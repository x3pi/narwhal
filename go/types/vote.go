package types

import (
	e_common "github.com/ethereum/go-ethereum/common"
	"github.com/holiman/uint256"

	"github.com/meta-node-blockchain/meta-node/pkg/common"
	pb "github.com/meta-node-blockchain/meta-node/pkg/proto"
)

type Vote interface {
	Value() interface{}
	Hash() e_common.Hash
	PublicKey() common.PublicKey
	Address() e_common.Address
	Sign() common.Sign
}

type BlockVote interface {
	// general
	Marshal() ([]byte, error)
	Unmarshal([]byte) error
	Proto() *pb.BlockVote
	FromProto(*pb.BlockVote)

	BlockHash() e_common.Hash
	Number() uint64
	PublicKey() common.PublicKey
	Address() e_common.Address
	Sign() common.Sign

	Valid() bool
}

type VerifyPackSignResultVote interface {
	Hash() e_common.Hash
	Value() interface{}
	PublicKey() common.PublicKey
	Address() e_common.Address
	Sign() common.Sign
	PackHash() e_common.Hash
	Valid() bool
}

type VerifyPacksSignResultVote interface {
	Hash() e_common.Hash
	Value() interface{}
	PublicKey() common.PublicKey
	Address() e_common.Address
	Sign() common.Sign
	RequestHash() e_common.Hash
	Valid() bool
}

type VerifyTransactionSignVote interface {
	Hash() e_common.Hash
	Value() interface{}
	PublicKey() common.PublicKey
	Address() e_common.Address
	Sign() common.Sign
	TransactionHash() e_common.Hash
	Valid() bool
}

type ExecuteResultsVote interface {
	GroupId() *uint256.Int
	Value() interface{}
	Hash() e_common.Hash
	PublicKey() common.PublicKey
	Address() e_common.Address
	Sign() common.Sign
}
