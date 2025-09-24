package types

import (
	"github.com/ethereum/go-ethereum/common"
	"google.golang.org/protobuf/reflect/protoreflect"

	pb "github.com/meta-node-blockchain/meta-node/pkg/proto"
)

// New ExecuteSCResults
type ExecuteSCResult interface {
	// general
	Unmarshal([]byte) error
	Marshal() ([]byte, error)
	Proto() protoreflect.ProtoMessage
	String() string
	// getter
	TransactionHash() common.Hash
	MapAddBalance() map[string][]byte
	MapSubBalance() map[string][]byte
	MapNonce() map[string][]byte
	MapStorageRoot() map[string][]byte
	MapCodeHash() map[string][]byte
	MapStorageAddress() map[string]common.Address
	MapCreatorPubkey() map[string][]byte
	MarshalJSON() ([]byte, error)
	MapStorageAddressTouchedAddresses() map[common.Address][]common.Address
	MapNativeSmartContractUpdateStorage() map[common.Address][][2][]byte
	ReceiptStatus() pb.RECEIPT_STATUS
	Exception() pb.EXCEPTION
	Return() []byte
	GasUsed() uint64
	LogsHash() common.Hash
	EventLogs() []EventLog
	PrintJSON()
}

type ExecuteSCResults interface {
	// general
	Unmarshal([]byte) error
	Marshal() ([]byte, error)
	Proto() protoreflect.ProtoMessage
	String() string
	//
	Results() []ExecuteSCResult
	BlockNumber() uint64
	GroupId() uint64
}

type TouchedAddressesData interface {
	BlockNumber() uint64
	Addresses() []common.Address
}

type SmartContractUpdateData interface {
	SetCode([]byte)
	UpdateStorage(map[string][]byte)
	AddEventLog(eventLog EventLog)
	Code() []byte
	CodeHash() common.Hash
	Storage() map[string][]byte
	EventLogs() []EventLog
	Proto() *pb.SmartContractUpdateData
	FromProto(*pb.SmartContractUpdateData)
}

type SmartContractUpdateDatas interface {
	Data() map[common.Address]SmartContractUpdateData
	BlockNumber() uint64

	Unmarshal([]byte) error
	Marshal() ([]byte, error)
	Proto() *pb.SmartContractUpdateDatas
	FromProto(*pb.SmartContractUpdateDatas)
}
