// File: pkg/txsender/txsender.go
package txsender

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"log"
	"time"

	"github.com/quic-go/quic-go"
)

// Client quản lý kết nối QUIC bền bỉ đến node mempool.
type Client struct {
	targetAddress string
	tlsConfig     *tls.Config
	quicConfig    *quic.Config
	connection    quic.Connection
	stream        quic.Stream // Sửa đổi: Lưu trữ stream dài hạn
}

// NewClient khởi tạo một client QUIC mới.
func NewClient(targetAddress string) *Client {
	// Cần thêm ServerName ("localhost") để khớp với chứng chỉ tự ký của server Rust.
	tlsConfig := &tls.Config{
		InsecureSkipVerify: true,
	}

	quicConfig := &quic.Config{
		MaxIdleTimeout:  time.Minute,
		KeepAlivePeriod: 30 * time.Second,
	}

	return &Client{
		targetAddress: targetAddress,
		tlsConfig:     tlsConfig,
		quicConfig:    quicConfig,
	}
}

// Connect thiết lập kết nối QUIC đến node và mở một stream hai chiều dài hạn.
func (c *Client) Connect() error {
	if c.connection != nil {
		c.Close()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := quic.DialAddr(ctx, c.targetAddress, c.tlsConfig, c.quicConfig)
	if err != nil {
		return fmt.Errorf("không thể kết nối QUIC đến %s: %w", c.targetAddress, err)
	}
	c.connection = conn

	// SỬA ĐỔI CHÍNH: Mở stream hai chiều (BI-DIRECTIONAL stream) ngay sau khi kết nối
	// và lưu trữ để tái sử dụng.
	stream, err := c.connection.OpenStreamSync(ctx)
	if err != nil {
		c.Close()
		return fmt.Errorf("không thể mở stream dài hạn: %w", err)
	}
	c.stream = stream

	return nil
}

// Close đóng stream và kết nối QUIC.
func (c *Client) Close() error {
	if c.stream != nil {
		// Đóng stream khi toàn bộ client đóng.
		c.stream.Close()
		c.stream = nil
	}
	if c.connection != nil {
		err := c.connection.CloseWithError(0, "client closing")
		c.connection = nil
		return err
	}
	return nil
}

// SendTransaction gửi một giao dịch qua stream QUIC đã mở.
func (c *Client) SendTransaction(payload []byte) error {
	// Kiểm tra stream trước khi gửi.
	if c.stream == nil {
		if err := c.Connect(); err != nil {
			return fmt.Errorf("kết nối ban đầu thất bại: %w", err)
		}
	}

	// Cố gắng gửi.
	err := c.trySend(payload)
	if err == nil {
		return nil // Thành công.
	}

	// Nếu thất bại, thử kết nối/mở lại stream và gửi lần nữa.
	log.Printf("Cảnh báo: Gửi thất bại (%v), đang thử kết nối lại...", err)
	if reconnErr := c.Connect(); reconnErr != nil {
		return fmt.Errorf("kết nối lại thất bại: %w", reconnErr)
	}

	log.Println("Kết nối lại thành công, đang gửi lại giao dịch.")

	// Thử gửi lại lần cuối.
	if finalErr := c.trySend(payload); finalErr != nil {
		return fmt.Errorf("gửi lại thất bại sau khi kết nối lại: %w", finalErr)
	}

	return nil
}

// trySend thực hiện logic gửi dữ liệu trên stream dài hạn.
func (c *Client) trySend(payload []byte) error {
	if c.stream == nil {
		return fmt.Errorf("stream QUIC chưa được thiết lập")
	}

	// 1. Gửi 4-byte độ dài của payload.
	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(payload)))
	log.Printf("lenBuf %v", hex.EncodeToString(lenBuf))
	log.Printf("payload %v", hex.EncodeToString(payload))

	// Sử dụng stream đã lưu.
	if _, err := c.stream.Write(lenBuf); err != nil {
		return fmt.Errorf("lỗi khi gửi độ dài giao dịch: %w", err)
	}

	// 2. Gửi payload.
	if _, err := c.stream.Write(payload); err != nil {
		return fmt.Errorf("lỗi khi gửi payload giao dịch: %w", err)
	}

	// KHÔNG đóng stream ở đây. Nó sẽ được tái sử dụng.
	// KHÔNG cần Flush, vì QUIC stream thường không có bộ đệm.

	return nil
}
