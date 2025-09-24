package types

import (
	"math/big"

	e_common "github.com/ethereum/go-ethereum/common"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/meta-node-blockchain/meta-node/pkg/common"
	pb "github.com/meta-node-blockchain/meta-node/pkg/proto"
)

type AccountState interface {
	// general
	Marshal() ([]byte, error)
	Unmarshal([]byte) error
	Proto() *pb.AccountState
	FromProto(*pb.AccountState)
	Copy() AccountState
	String() string
	Nonce() uint64
	SetNonce(uint64)
	PlusOneNonce()
	// getter
	Address() e_common.Address
	PublicKeyBls() []byte
	LastHash() e_common.Hash
	Balance() *big.Int
	PendingBalance() *big.Int
	TotalBalance() *big.Int
	SmartContractState() SmartContractState
	DeviceKey() e_common.Hash
	AccountType() pb.ACCOUNT_TYPE
	SetAccountType(pb.ACCOUNT_TYPE) error
	//
	SubPendingBalance(*big.Int) error
	AddPendingBalance(*big.Int)

	AddBalance(*big.Int)
	SubBalance(*big.Int) error

	SubTotalBalance(*big.Int) error

	SetLastHash(e_common.Hash)
	SetNewDeviceKey(e_common.Hash)
	SetPublicKeyBls([]byte) error

	// smart contract state
	SetSmartContractState(SmartContractState)
	SetCreatorPublicKey(creatorPublicKey common.PublicKey)
	SetCodeHash(codeHash e_common.Hash)
	SetStorageRoot(storageRoot e_common.Hash)
	SetStorageAddress(storageAddress e_common.Address)
	AddLogHash(logsHash e_common.Hash)
}

type SmartContractState interface {
	// general
	Unmarshal([]byte) error
	Marshal() ([]byte, error)
	String() string

	// getter
	Proto() *pb.SmartContractState
	FromProto(*pb.SmartContractState)

	CreatorPublicKey() common.PublicKey
	CreatorAddress() e_common.Address
	StorageAddress() e_common.Address
	CodeHash() e_common.Hash
	StorageRoot() e_common.Hash
	LogsHash() e_common.Hash
	MapFullDbHash() e_common.Hash
	SimpleDbHash() e_common.Hash

	// setter
	SetCreatorPublicKey(common.PublicKey)
	SetStorageAddress(storageAddress e_common.Address)
	SetCodeHash(e_common.Hash)
	SetStorageRoot(e_common.Hash)
	SetLogsHash(e_common.Hash)
	SetMapFullDbHash(e_common.Hash)
	SetSimpleDbHash(e_common.Hash)

	// clone
	Copy() SmartContractState

	// trie
	TrieDatabaseMap() map[string][]byte
	SetTrieDatabaseMap(trieDatabaseMap map[string][]byte)
	SetTrieDatabaseMapValue(key string, value []byte)
	GetTrieDatabaseMapValue(key string) []byte
	DeleteTrieDatabaseMapValue(key string)
}

type UpdateField interface {
	Field() pb.UPDATE_STATE_FIELD
	Value() []byte
}

type UpdateStateFields interface {
	Unmarshal([]byte) error
	Marshal() ([]byte, error)
	String() string
	Proto() protoreflect.ProtoMessage
	FromProto(protoreflect.ProtoMessage)
	Address() e_common.Address
	Fields() []UpdateField
	AddField(field pb.UPDATE_STATE_FIELD, value []byte)
}
