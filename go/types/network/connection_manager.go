package network

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/holiman/uint256"
	pb "github.com/meta-node-blockchain/meta-node/pkg/proto"
)

type ConnectionsManager interface {
	// getter
	ConnectionsByType(cType int) map[common.Address]Connection
	ConnectionByTypeAndAddress(cType int, address common.Address) Connection
	ConnectionsByTypeAndAddresses(cType int, addresses []common.Address) map[common.Address]Connection
	FilterAddressAvailable(cType int, addresses map[common.Address]*uint256.Int) map[common.Address]*uint256.Int
	ParentConnection() Connection

	// setter
	AddConnection(Connection, bool)
	RemoveConnection(Connection)
	AddParentConnection(Connection)

	// stats
	Stats() *pb.NetworkStats
}
