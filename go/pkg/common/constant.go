package common

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/holiman/uint256"
)

const (
	MAX_VALIDATOR = 101
	// TODO MOVE TO CONFIG
	TRANSFER_GAS_COST     = 20000
	OPEN_CHANNEL_GAS_COST = 20000000
	PUNISH_GAS_COST       = 10000

	BLOCK_GAS_LIMIT                       = 10000000000
	OFF_CHAIN_GAS_LIMIT                   = 1000000000
	BASE_FEE_INCREASE_GAS_USE_THRESH_HOLD = 5000000000
	MINIMUM_BASE_FEE                      = 1000000
	BASE_FEE_CHANGE_PERCENTAGE            = 12.5
	MAX_GASS_FEE                          = 5000000

	MAX_GROUP_GAS    = 9999999999999999999
	MAX_TOTAL_GAS    = 9999999999999999999
	MAX_GROUP_TIME   = 9999999999999999999 // Millisecond giây
	MAX_TOTAL_TIME   = 9999999999999999999 // Millisecond giây
	MIN_TX_TIME      = 1                   // Millisecond giây
	MAX_TIME_PENDING = 5                   // phút

	//Wallet Select
	ACCOUNT_SETTING_ADDRESS_SELECT = "account"

	StartFileUpload   = "START_FILE_UPLOAD"
	FileChunkTransfer = "FILE_CHUNK_TRANSFER"
	EndFileUploadCmd  = "END_FILE_UPLOAD"

	SyncFileRequest  = "SYNC_FILE_REQUEST"
	SyncFileResponse = "SYNC_FILE_RESPONSE" // Mặc dù không phải là một "lệnh" mà là một "phản hồi", việc định nghĩa nó có thể hữu ích

	// === HẰNG SỐ MỚI ===
	RequestDirectory = "REQUEST_DIRECTORY"
)

var (
	VALIDATOR_STAKE_POOL_ADDRESS    = common.Address{}
	ADDRESS_FOR_UPDATE_TYPE_ACCOUNT = common.HexToAddress("0x0000000000000000000000000000000000000000")
	SLASH_VALIDATOR_AMOUNT          = uint256.NewInt(0).SetBytes(common.FromHex("8ac7230489e80000"))
	MINIMUM_VALIDATOR_STAKE_AMOUNT  = uint256.NewInt(0)
	MINIMUM_OPEN_ACCOUNT_AMOUNT     = uint256.NewInt(0).SetBytes(common.FromHex("2386F26FC10000"))
)

type ServiceType string

const (
	ServiceTypeWrite    ServiceType = "SUB-WRITE"
	ServiceTypeMaster   ServiceType = "MASTER"
	ServiceTypeReadonly ServiceType = "SUB-READ"
)

type FolderBackup int

const (
	FolderBackupAcount                 FolderBackup = iota // 0
	FolderBackupBlocks                                     // 1
	FolderBackupCode                                       // 2
	FolderBackupDevicekey                                  // 3
	FolderBackupSmartSontract                              // 4
	FolderBackupTransaction                                // 5
	FolderBackupTransactionBlockNumber                     // 6
	FolderBackupTrieDatabase                               // 7
	FolderBackupReceipts                                   // 8
	FolderMappingDb                                        // 9
	FolderBackupDb                                         // 10

)

// Thêm hàm String() để dễ dàng chuyển đổi enum sang string
func (fb FolderBackup) String() string {
	switch fb {
	case FolderBackupAcount:
		return "/account"
	case FolderBackupBlocks:
		return "/blocks"
	case FolderBackupCode:
		return "/code"
	case FolderBackupDevicekey:
		return "/deviceKey_storage"
	case FolderBackupSmartSontract:
		return "/smart_contract"
	case FolderBackupTransaction:
		return "/transaction"
	case FolderBackupTransactionBlockNumber:
		return "/transaction_block_number"
	case FolderBackupTrieDatabase:
		return "/trie_database"
	case FolderBackupReceipts:
		return "/receipts"
	case FolderMappingDb:
		return "/mapping_db"
	case FolderBackupDb:
		return "/backup_db"
	default:
		return "/" // Hoặc xử lý trường hợp không hợp lệ khác
	}
}

const (
	BlockDataTopic               = "block_data_topic"
	TransactionsFromSubTopic     = "tx_from_sub_topic"
	ReadTransactionsFromSubTopic = "read_tx_from_sub_topic"
	FreeFeeRequestProtocol       = "/mtn/free-fee-request/1.0.0"
)
