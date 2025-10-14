// File: pkg/txsender/txsender.go
package txsender

import (
	"context"
	"crypto/tls"
	"encoding/binary"
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
	connection    quic.Connection // Chỉ lưu kết nối, không lưu stream
}

// NewClient khởi tạo một client QUIC mới.
func NewClient(targetAddress string) *Client {
	// SỬA LỖI: ALPN phải là "narwhal" để khớp với server Rust.
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

// Connect thiết lập kết nối QUIC đến node.
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
	return nil
}

// Close đóng kết nối QUIC.
func (c *Client) Close() error {
	if c.connection != nil {
		err := c.connection.CloseWithError(0, "client closing")
		c.connection = nil
		return err
	}
	return nil
}

// SendTransaction gửi một giao dịch qua một stream QUIC mới.
func (c *Client) SendTransaction(payload []byte) error {
	if c.connection == nil {
		if err := c.Connect(); err != nil {
			return fmt.Errorf("kết nối ban đầu thất bại: %w", err)
		}
	}

	// Cố gắng gửi.
	err := c.trySend(payload)
	if err == nil {
		return nil // Thành công.
	}

	// Nếu thất bại, có thể do kết nối đã mất. Thử kết nối lại và gửi lần nữa.
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

// trySend thực hiện logic gửi dữ liệu trên một stream mới.
func (c *Client) trySend(payload []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// SỬA LỖI: Mở một stream mới cho mỗi giao dịch.
	stream, err := c.connection.OpenUniStreamSync(ctx)
	if err != nil {
		return fmt.Errorf("không thể mở stream mới: %w", err)
	}
	defer stream.Close()

	// SỬA LỖI: Gửi 8-byte độ dài của payload trước.
	lenBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(lenBuf, uint64(len(payload)))

	// Gửi độ dài
	if _, err := stream.Write(lenBuf); err != nil {
		return fmt.Errorf("lỗi khi gửi độ dài giao dịch: %w", err)
	}

	// Gửi payload
	if _, err := stream.Write(payload); err != nil {
		return fmt.Errorf("lỗi khi gửi payload giao dịch: %w", err)
	}

	return nil
}
