package network

import (
	"fmt"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/holiman/uint256"

	p_common "github.com/meta-node-blockchain/meta-node/pkg/common"
	"github.com/meta-node-blockchain/meta-node/pkg/logger"
	pb "github.com/meta-node-blockchain/meta-node/pkg/proto"
	"github.com/meta-node-blockchain/meta-node/types/network"
)

const (
	MaxConnectionTypes  = 20
	numConnectionShards = 32
)

type connectionShard struct {
	mu          sync.RWMutex
	connections map[common.Address]network.Connection
}

type ConnectionsManager struct {
	parentMu         sync.RWMutex
	parentConnection network.Connection
	typeToShards     [][]connectionShard
}

func NewConnectionsManager() network.ConnectionsManager {
	cm := &ConnectionsManager{
		typeToShards: make([][]connectionShard, MaxConnectionTypes),
	}
	for i := range cm.typeToShards {
		cm.typeToShards[i] = make([]connectionShard, numConnectionShards)
		for j := range cm.typeToShards[i] {
			cm.typeToShards[i][j].connections = make(map[common.Address]network.Connection)
		}
	}
	return cm
}

func (cm *ConnectionsManager) getShard(cType int, address common.Address) (*connectionShard, error) {
	if cType < 0 || cType >= len(cm.typeToShards) {
		return nil, fmt.Errorf("cType %d nằm ngoài phạm vi", cType)
	}
	shardIndex := int(address[common.AddressLength-1]) & (numConnectionShards - 1)
	return &cm.typeToShards[cType][shardIndex], nil
}

func (cm *ConnectionsManager) ConnectionsByType(cType int) map[common.Address]network.Connection {
	if cType < 0 || cType >= len(cm.typeToShards) {
		logger.Warn("ConnectionsByType: cType %d nằm ngoài phạm vi hợp lệ.", cType)
		return make(map[common.Address]network.Connection)
	}

	fullMap := make(map[common.Address]network.Connection)
	shardsForType := cm.typeToShards[cType]

	for i := range shardsForType {
		shard := &shardsForType[i]
		shard.mu.RLock()
		for addr, conn := range shard.connections {
			fullMap[addr] = conn
		}
		shard.mu.RUnlock()
	}
	return fullMap
}

func (cm *ConnectionsManager) ConnectionByTypeAndAddress(cType int, address common.Address) network.Connection {
	shard, err := cm.getShard(cType, address)
	if err != nil {
		logger.Warn("ConnectionByTypeAndAddress: Lỗi khi lấy shard cho cType %d: %v", cType, err)
		return nil
	}

	shard.mu.RLock()
	defer shard.mu.RUnlock()

	conn, ok := shard.connections[address]
	if !ok {
		return nil
	}
	return conn
}

func (cm *ConnectionsManager) ConnectionsByTypeAndAddresses(cType int, addresses []common.Address) map[common.Address]network.Connection {
	result := make(map[common.Address]network.Connection, len(addresses))
	for _, addr := range addresses {
		if conn := cm.ConnectionByTypeAndAddress(cType, addr); conn != nil {
			result[addr] = conn
		}
	}
	return result
}

func (cm *ConnectionsManager) FilterAddressAvailable(cType int, addresses map[common.Address]*uint256.Int) map[common.Address]*uint256.Int {
	availableAddresses := make(map[common.Address]*uint256.Int)
	for address, value := range addresses {
		if conn := cm.ConnectionByTypeAndAddress(cType, address); conn != nil && conn.IsConnect() {
			availableAddresses[address] = value
		}
	}
	return availableAddresses
}

func (cm *ConnectionsManager) ParentConnection() network.Connection {
	cm.parentMu.RLock()
	defer cm.parentMu.RUnlock()
	return cm.parentConnection
}

func (cm *ConnectionsManager) Stats() *pb.NetworkStats {
	pbNetworkStats := &pb.NetworkStats{
		TotalConnectionByType: make(map[string]int32, MaxConnectionTypes),
	}

	for cType, shardsForType := range cm.typeToShards {
		total := 0
		for i := range shardsForType {
			shard := &shardsForType[i]
			shard.mu.RLock()
			total += len(shard.connections)
			shard.mu.RUnlock()
		}

		if total > 0 {
			connectionTypeName := p_common.MapIndexToConnectionType(cType)
			if connectionTypeName == "" {
				connectionTypeName = fmt.Sprintf("UNKNOWN_TYPE_%d", cType)
			}
			pbNetworkStats.TotalConnectionByType[connectionTypeName] = int32(total)
		}
	}
	return pbNetworkStats
}

func (cm *ConnectionsManager) AddParentConnection(conn network.Connection) {
	cm.parentMu.Lock()
	defer cm.parentMu.Unlock()
	if conn == nil {
		logger.Warn("AddParentConnection: Cố gắng thêm một kết nối cha nil. Bỏ qua.")
		return
	}
	cm.parentConnection = conn
	logger.Info("AddParentConnection: Đã thêm/cập nhật kết nối cha: %s", conn.String())
}

func (cm *ConnectionsManager) RemoveConnection(conn network.Connection) {
	if conn == nil {
		logger.Warn("RemoveConnection: Cố gắng xóa một kết nối nil. Bỏ qua.")
		return
	}

	address := conn.Address()
	cTypeStr := conn.Type()
	cType := p_common.MapConnectionTypeToIndex(cTypeStr)

	shard, err := cm.getShard(cType, address)
	if err != nil {
		logger.Warn("RemoveConnection: Loại kết nối '%s' không hợp lệ. Lỗi: %v", cTypeStr, err)
		return
	}

	shard.mu.Lock()
	defer shard.mu.Unlock()

	if existingConn, ok := shard.connections[address]; ok && existingConn == conn {
		delete(shard.connections, address)
		logger.Info("RemoveConnection: Đã xóa kết nối: %s", conn.String())
	}

	cm.parentMu.Lock()
	if cm.parentConnection == conn {
		cm.parentConnection = nil
		logger.Info("RemoveConnection: Kết nối cha đã được xóa: %s", conn.String())
	}
	cm.parentMu.Unlock()
}

func (cm *ConnectionsManager) AddConnection(conn network.Connection, replace bool) {
	if conn == nil {
		logger.Warn("AddConnection: Cố gắng thêm một kết nối nil. Bỏ qua.")
		return
	}

	address := conn.Address()
	cTypeStr := conn.Type()
	cType := p_common.MapConnectionTypeToIndex(cTypeStr)

	shard, err := cm.getShard(cType, address)
	if err != nil {
		logger.Error("AddConnection: Loại kết nối '%s' không hợp lệ. Lỗi: %v", cTypeStr, err)
		return
	}

	shard.mu.Lock()
	defer shard.mu.Unlock()

	existingConn, exists := shard.connections[address]

	if !exists || replace {
		if replace && exists && existingConn != nil && existingConn != conn {
			logger.Info("AddConnection: Thay thế kết nối hiện có: %s", existingConn.String())
			if existingConn.IsConnect() {
				go func(oldConn network.Connection) {
					_ = oldConn.Disconnect()
				}(existingConn)
			}
		}
		shard.connections[address] = conn
		logger.Info("AddConnection: Đã thêm/thay thế kết nối: %s", conn.String())
	}
}

func (cm *ConnectionsManager) HealthCheck() {
	ticker := time.NewTicker(30 * time.Second)
	go func() {
		for range ticker.C {
			logger.Info("Running connection health check...")
			for cType, shardsForType := range cm.typeToShards {
				for i := range shardsForType {
					shard := &shardsForType[i]
					shard.mu.Lock()
					for addr, conn := range shard.connections {
						if !conn.IsConnect() {
							delete(shard.connections, addr)
							logger.Info("HealthCheck: Removed dead connection type %d for address %s", cType, addr.Hex())
						}
					}
					shard.mu.Unlock()
				}
			}
		}
	}()
}
