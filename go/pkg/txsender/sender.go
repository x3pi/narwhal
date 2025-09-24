// File: txsender/sender.go
package txsender

import (
	"encoding/binary"
	"fmt"
	"net"
	"time"
)

// Client quản lý kết nối bền bỉ và tự động kết nối lại khi cần thiết.
type Client struct {
	targetAddress string
	conn          net.Conn // Lưu trữ kết nối TCP bền bỉ
}

// NewClient khởi tạo một client mới trỏ đến địa chỉ node mempool mục tiêu.
func NewClient(targetAddress string) *Client {
	return &Client{targetAddress: targetAddress}
}

// Connect thiết lập kết nối TCP đến node.
// Hàm này được gọi tự động bởi SendTransaction nếu cần,
// nhưng cũng có thể được gọi thủ công để kiểm tra kết nối ban đầu.
func (c *Client) Connect() error {
	// Đóng kết nối cũ nếu tồn tại trước khi mở kết nối mới.
	if c.conn != nil {
		c.conn.Close()
	}

	// Thiết lập kết nối TCP mới.
	conn, err := net.DialTimeout("tcp", c.targetAddress, 5*time.Second) // Thêm timeout để tránh treo vĩnh viễn
	if err != nil {
		return fmt.Errorf("không thể kết nối đến %s: %w", c.targetAddress, err)
	}

	c.conn = conn
	return nil
}

// Close đóng kết nối TCP đang hoạt động.
func (c *Client) Close() error {
	if c.conn != nil {
		err := c.conn.Close()
		c.conn = nil // Đặt lại trạng thái để ensureConnected biết cần kết nối lại.
		return err
	}
	return nil
}

// ensureConnected là hàm nội bộ để đảm bảo kết nối tồn tại trước khi thực hiện thao tác.
func (c *Client) ensureConnected() error {
	if c.conn == nil {
		return c.Connect()
	}
	return nil
}

// writeData thực hiện logic gửi dữ liệu length-prefixed.
func (c *Client) writeData(payload []byte) error {
	// Chuẩn bị tiền tố độ dài (length prefix).
	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(payload)))

	// Gửi tiền tố độ dài.
	if _, err := c.conn.Write(lenBuf); err != nil {
		return fmt.Errorf("lỗi khi gửi độ dài giao dịch: %w", err)
	}

	// Gửi payload giao dịch thực tế.
	if _, err := c.conn.Write(payload); err != nil {
		return fmt.Errorf("lỗi khi gửi payload giao dịch: %w", err)
	}
	return nil
}

// SendTransaction gửi một gói dữ liệu giao dịch.
// Tự động kết nối nếu chưa kết nối, và thử kết nối lại một lần nếu gặp lỗi khi đang gửi.
func (c *Client) SendTransaction(transactionPayload []byte) error {
	// --- Bước 1: Đảm bảo đã kết nối trước khi thử gửi ---
	if err := c.ensureConnected(); err != nil {
		return fmt.Errorf("kết nối ban đầu thất bại: %w", err)
	}

	// --- Bước 2: Thử gửi dữ liệu lần đầu tiên ---
	err := c.writeData(transactionPayload)
	if err == nil {
		return nil // Gửi thành công ngay lần đầu tiên.
	}

	// --- Bước 3: Xử lý lỗi (kết nối có thể đã mất) và thử lại ---
	fmt.Printf("Cảnh báo: Gửi thất bại (%v), đang thử kết nối lại...\n", err)
	c.Close() // Đóng kết nối cũ (đã hỏng).

	// Cố gắng kết nối lại.
	if reconnErr := c.Connect(); reconnErr != nil {
		return fmt.Errorf("kết nối lại thất bại: %w", reconnErr)
	}

	fmt.Println("Kết nối lại thành công, đang gửi lại giao dịch.")

	// Thử gửi lại lần thứ hai trên kết nối mới.
	if finalErr := c.writeData(transactionPayload); finalErr != nil {
		return fmt.Errorf("gửi lại thất bại sau khi kết nối lại: %w", finalErr)
	}

	return nil // Gửi thành công ở lần thử lại.
}
