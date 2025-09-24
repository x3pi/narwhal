package network

import (
	"errors"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/meta-node-blockchain/meta-node/pkg/logger"
	pb "github.com/meta-node-blockchain/meta-node/pkg/proto"
	"github.com/meta-node-blockchain/meta-node/types/network"
)

// MessageSender chịu trách nhiệm gửi tin nhắn qua các kết nối mạng.
type MessageSender struct {
	version string // Phiên bản của giao thức mạng được sử dụng.
}

// NewMessageSender tạo một instance mới của MessageSender.
// version là phiên bản giao thức sẽ được sử dụng khi gửi tin nhắn.
func NewMessageSender(
	version string,
) network.MessageSender {
	return &MessageSender{
		version: version,
	}
}

// SendMessage gửi một tin nhắn đã được marshal bằng Protocol Buffers qua một kết nối cụ thể.
// connection: Kết nối mạng để gửi tin nhắn.
// command: Lệnh của tin nhắn.
// pbMessage: Tin nhắn Protocol Buffers cần gửi.
func (s *MessageSender) SendMessage(
	connection network.Connection,
	command string,
	pbMessage protoreflect.ProtoMessage,
) error {
	// Ghi log cho mục đích gỡ lỗi, cho biết hàm SendMessage đang được gọi.
	logger.Debug("SendMessage called for command '%s' to connection %s", command, connection.Address().Hex())
	return SendMessage(
		connection,
		command,
		pbMessage,
		s.version,
	)
}

// SendBytes gửi một mảng byte thô qua một kết nối cụ thể.
// connection: Kết nối mạng để gửi tin nhắn.
// command: Lệnh của tin nhắn.
// b: Mảng byte cần gửi.
func (s *MessageSender) SendBytes(
	connection network.Connection,
	command string,
	b []byte,
) error {
	logger.Debug("SendBytes called for command '%s' to connection %s", command, connection.Address().Hex())
	return SendBytes(
		connection,
		command,
		b,
		s.version,
	)
}

// BroadcastMessage gửi một tin nhắn đến nhiều kết nối đồng thời.
// mapAddressConnections: Một map chứa địa chỉ của các node và kết nối tương ứng của chúng.
// command: Lệnh của tin nhắn.
// marshaler: Interface để marshal tin nhắn thành mảng byte.
func (s *MessageSender) BroadcastMessage(
	mapAddressConnections map[common.Address]network.Connection,
	command string,
	marshaler network.Marshaler,
) error {
	var wg sync.WaitGroup
	bytes, err := marshaler.Marshal()
	if err != nil {
		logger.Error("BroadcastMessage: Lỗi khi marshal tin nhắn cho lệnh '%s': %v", command, err)
		return err
	}

	logger.Debug("Broadcasting message for command '%s' to %d connections", command, len(mapAddressConnections))

	for address, con := range mapAddressConnections {
		if con != nil {
			wg.Add(1)
			go func(addr common.Address, conn network.Connection, wg *sync.WaitGroup, b []byte) {
				defer wg.Done()
				err := s.SendBytes(conn, command, b)
				if err != nil {
					logger.Error("BroadcastMessage: Lỗi khi gửi tin nhắn lệnh '%s' đến địa chỉ %s: %v", command, addr.Hex(), err)
				}
			}(address, con, &wg, bytes)
		}
	}
	wg.Wait() // Chờ tất cả các goroutine gửi tin nhắn hoàn thành.
	return nil
}

// getHeaderForCommand tạo một header tin nhắn chuẩn cho một lệnh cụ thể.
// command: Lệnh của tin nhắn.
// toAddress: Địa chỉ của người nhận.
// version: Phiên bản giao thức.
func getHeaderForCommand(
	command string,
	toAddress common.Address,
	version string,
) *pb.Header {
	return &pb.Header{
		Command:   command,
		Version:   version,
		ToAddress: toAddress.Bytes(),
		ID:        uuid.New().String(), // Tạo ID duy nhất cho mỗi tin nhắn.
	}
}

// generateMessage tạo một đối tượng network.Message từ các thông tin đầu vào.
// toAddress: Địa chỉ của người nhận.
// command: Lệnh của tin nhắn.
// body: Nội dung (body) của tin nhắn dưới dạng mảng byte.
// version: Phiên bản giao thức.
func generateMessage(
	toAddress common.Address,
	command string,
	body []byte,
	version string,
) network.Message {
	messageProto := &pb.Message{
		Header: getHeaderForCommand(
			command,
			toAddress,
			version,
		),
		Body: body,
	}
	return NewMessage(messageProto)
}

// SendMessage là một hàm tiện ích để gửi một tin nhắn đã được marshal bằng Protocol Buffers.
// Nó được sử dụng nội bộ bởi MessageSender và có thể được gọi trực tiếp nếu cần.
// connection: Kết nối mạng để gửi tin nhắn.
// command: Lệnh của tin nhắn.
// pbMessage: Tin nhắn Protocol Buffers cần gửi.
// version: Phiên bản giao thức.
func SendMessage(
	connection network.Connection,
	command string,
	pbMessage proto.Message,
	version string,
) (err error) {

	body := []byte{}
	if pbMessage != nil {
		body, err = proto.Marshal(pbMessage)
		if err != nil {
			logger.Error("SendMessage (utility): Lỗi khi marshal tin nhắn cho lệnh '%s': %v", command, err)
			return err
		}
	}
	errS := SendBytes(connection, command, body, version)
	return errS
}

// SendBytes là một hàm tiện ích để gửi một mảng byte thô.
// Nó được sử dụng nội bộ bởi MessageSender và có thể được gọi trực tiếp nếu cần.
// connection: Kết nối mạng để gửi tin nhắn.
// command: Lệnh của tin nhắn.
// bytes: Mảng byte cần gửi.
// version: Phiên bản giao thức.
func SendBytes(
	connection network.Connection,
	command string,
	bytes []byte,
	version string,
) error {
	if connection == nil {
		// LOG THÊM: Ghi log lỗi khi connection là nil.
		logger.Error("SendBytes (utility): kết nối là nil cho lệnh '%s'", command)
		return errors.New("SendBytes (utility): kết nối là nil cho lệnh '" + command + "'")
	}

	message := generateMessage(connection.Address(), command, bytes, version)
	errS := connection.SendMessage(message)
	return errS
}
