package network

import "github.com/meta-node-blockchain/meta-node/types/network"

// Request đại diện cho một yêu cầu (request) nhận được từ một kết nối mạng.
type Request struct {
	connection network.Connection
	message    network.Message
}

// NewRequest tạo một instance mới của Request.
// Hàm này vẫn hữu ích cho các trường hợp không dùng Pool.
func NewRequest(
	connection network.Connection,
	message network.Message,
) network.Request {
	return &Request{
		connection: connection,
		message:    message,
	}
}

// Message trả về đối tượng tin nhắn của request.
func (r *Request) Message() network.Message {
	return r.message
}

// Connection trả về đối tượng kết nối mà request này đến từ đó.
func (r *Request) Connection() network.Connection {
	return r.connection
}

// CẢI TIẾN: Thêm phương thức Reset để tái sử dụng đối tượng từ sync.Pool
// Phương thức này sẽ thiết lập lại các trường của đối tượng Request,
// tránh việc sử dụng lại dữ liệu cũ từ request trước.
func (r *Request) Reset(connection network.Connection, message network.Message) {
	r.connection = connection
	r.message = message
}
