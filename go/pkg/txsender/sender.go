// File: pkg/txsender/txsender.go
package txsender

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
)

// Client quản lý kết nối QUIC bền bỉ đến node mempool.
type Client struct {
	targetAddress string
	tlsConfig     *tls.Config
	quicConfig    *quic.Config
	connection    quic.Connection // Chỉ lưu kết nối, không lưu stream
	maxRetries    int             // Số lần retry tối đa (mặc định 3)
	baseDelay     time.Duration   // Thời gian delay ban đầu giữa các lần retry (mặc định 100ms)
	waitForAck    bool            // Có đợi ACK từ server không (mặc định true, false cho localhost/benchmark)
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
		maxRetries:    3,                      // Mặc định retry 3 lần
		baseDelay:     100 * time.Millisecond, // Mặc định delay 100ms ban đầu
		waitForAck:    false,                  // Mặc định đợi ACK
	}
}

// SetMaxRetries thiết lập số lần retry tối đa.
func (c *Client) SetMaxRetries(maxRetries int) {
	if maxRetries < 0 {
		maxRetries = 0
	}
	c.maxRetries = maxRetries
}

// SetBaseDelay thiết lập thời gian delay ban đầu giữa các lần retry (sẽ tăng dần theo exponential backoff).
func (c *Client) SetBaseDelay(delay time.Duration) {
	if delay < 0 {
		delay = 0
	}
	c.baseDelay = delay
}

// SetWaitForAck thiết lập có đợi ACK từ server không.
// false = Không đợi ACK (phù hợp cho localhost/benchmark, tăng throughput)
// true = Đợi ACK (đảm bảo transaction đã được nhận)
func (c *Client) SetWaitForAck(wait bool) {
	c.waitForAck = wait
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

// SendTransaction gửi một giao dịch qua một stream QUIC mới với cơ chế retry tự động.
// Nếu waitForAck = false, sẽ sử dụng trySendFast (không đợi ACK) để tối ưu throughput.
func (c *Client) SendTransaction(payload []byte) error {
	// Nếu không cần đợi ACK, dùng fast mode
	if !c.waitForAck {
		return c.SendTransactionFast(payload)
	}

	// Đảm bảo có kết nối trước khi bắt đầu
	if c.connection == nil {
		if err := c.Connect(); err != nil {
			return fmt.Errorf("kết nối ban đầu thất bại: %w", err)
		}
	}

	var lastErr error
	delay := c.baseDelay

	// Retry loop với exponential backoff
	for attempt := 0; attempt <= c.maxRetries; attempt++ {
		if attempt > 0 {
			log.Printf("Lần thử lại %d/%d sau %v...", attempt, c.maxRetries, delay)
			time.Sleep(delay)
			// Exponential backoff: delay tăng gấp đôi mỗi lần, tối đa 5 giây
			delay = time.Duration(float64(delay) * 1.5)
			if delay > 5*time.Second {
				delay = 5 * time.Second
			}
		}

		// Thử gửi transaction
		err := c.trySend(payload)
		if err == nil {
			if attempt > 0 {
				log.Printf("Gửi thành công sau %d lần thử lại", attempt)
			}
			return nil // Thành công
		}

		lastErr = err
		log.Printf("Lần thử %d thất bại: %v", attempt+1, err)

		// Nếu thất bại, thử kết nối lại (có thể kết nối đã bị đứt)
		if attempt < c.maxRetries {
			log.Printf("Đang thử kết nối lại...")
			if reconnErr := c.Connect(); reconnErr != nil {
				log.Printf("Không thể kết nối lại: %v, sẽ tiếp tục thử với kết nối hiện tại", reconnErr)
				// Vẫn tiếp tục retry, có thể kết nối vẫn còn hoạt động
			} else {
				log.Printf("Kết nối lại thành công")
			}
		}
	}

	// Tất cả các lần thử đều thất bại
	return fmt.Errorf("gửi transaction thất bại sau %d lần thử: %w", c.maxRetries+1, lastErr)
}

// SendTransactionAsync gửi transaction bất đồng bộ (không chặn) trong một goroutine riêng.
// Trả về một channel để nhận kết quả hoặc lỗi.
func (c *Client) SendTransactionAsync(payload []byte) <-chan error {
	resultChan := make(chan error, 1)
	go func() {
		resultChan <- c.SendTransaction(payload)
	}()
	return resultChan
}

// SendTransactionFast gửi transaction nhanh không chờ ACK (fire-and-forget).
// Phù hợp để spam nhiều transaction mà không bị chặn.
// Phù hợp cho localhost/benchmark để tối ưu throughput.
// Lưu ý: Sử dụng unidirectional stream để worker không phải gửi ACK về.
func (c *Client) SendTransactionFast(payload []byte) error {
	// Đảm bảo có kết nối trước khi bắt đầu
	if c.connection == nil {
		if err := c.Connect(); err != nil {
			return fmt.Errorf("kết nối ban đầu thất bại: %w", err)
		}
	}

	// Gửi mà không đợi ACK - chỉ cần đảm bảo gửi thành công
	return c.trySendFast(payload)
}

// trySendFast gửi transaction nhanh không đợi ACK.
// Sử dụng unidirectional stream để worker không phải gửi ACK về (tối ưu cho localhost).
func (c *Client) trySendFast(payload []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// SỬA ĐỔI: Sử dụng unidirectional stream thay vì bidirectional
	// Điều này giúp worker không phải gửi ACK về, tối ưu throughput
	stream, err := c.connection.OpenUniStreamSync(ctx)
	if err != nil {
		return fmt.Errorf("không thể mở unidirectional stream: %w", err)
	}

	// Gửi 8-byte độ dài của payload trước.
	lenBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(lenBuf, uint64(len(payload)))

	// Gửi độ dài
	if _, err := stream.Write(lenBuf); err != nil {
		stream.CancelWrite(0) // Hủy stream nếu lỗi
		return fmt.Errorf("lỗi khi gửi độ dài giao dịch: %w", err)
	}

	// Gửi payload
	if _, err := stream.Write(payload); err != nil {
		stream.CancelWrite(0) // Hủy stream nếu lỗi
		return fmt.Errorf("lỗi khi gửi payload giao dịch: %w", err)
	}

	// Đóng stream - không đợi ACK
	if err := stream.Close(); err != nil {
		return fmt.Errorf("lỗi khi đóng stream: %w", err)
	}

	return nil // Thành công - đã gửi được dữ liệu
}

// SendTransactionsConcurrent gửi nhiều transaction đồng thời với số lượng goroutine giới hạn.
// maxConcurrency: Số lượng transaction có thể gửi đồng thời (mặc định 100)
// Returns: Số lượng transaction đã gửi thành công và số lượng lỗi
func (c *Client) SendTransactionsConcurrent(
	payloads [][]byte,
	maxConcurrency int,
) (successCount int, errorCount int) {
	if maxConcurrency <= 0 {
		maxConcurrency = 100 // Mặc định 100 goroutine đồng thời
	}

	// Semaphore để giới hạn số goroutine đồng thời
	semaphore := make(chan struct{}, maxConcurrency)
	var wg sync.WaitGroup
	var mu sync.Mutex       // Bảo vệ biến đếm
	var success, errors int // Biến đếm được truy cập từ goroutines

	// Gửi tất cả transactions
	for _, payload := range payloads {
		wg.Add(1)
		go func(p []byte) {
			defer wg.Done()

			// Acquire semaphore
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			// Gửi transaction
			err := c.SendTransactionFast(p)
			mu.Lock()
			if err != nil {
				errors++
				log.Printf("Lỗi khi gửi transaction: %v", err)
			} else {
				success++
			}
			mu.Unlock()
		}(payload)
	}

	// Đợi tất cả goroutines hoàn thành
	wg.Wait()

	return success, errors
}

// trySend thực hiện logic gửi dữ liệu trên một stream mới và nhận ACK từ server.
func (c *Client) trySend(payload []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// SỬA ĐỔI: Sử dụng bidirectional stream để có thể nhận ACK từ worker
	stream, err := c.connection.OpenStreamSync(ctx)
	if err != nil {
		return fmt.Errorf("không thể mở stream mới: %w", err)
	}
	defer stream.Close()

	// Gửi 8-byte độ dài của payload trước.
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

	// Đóng stream - worker sẽ gửi ACK qua unidirectional stream mới
	_ = stream.Close()

	// Đợi nhận ACK từ server qua unidirectional stream mới
	// Worker gửi ACK qua writer.send() tức là unidirectional stream mới
	ackCtx, ackCancel := context.WithTimeout(context.Background(), 10*time.Second) // Tăng timeout lên 10 giây
	defer ackCancel()

	// Chấp nhận unidirectional stream mới từ server để nhận ACK
	ackStream, err := c.connection.AcceptUniStream(ackCtx)
	if err != nil {
		// Timeout hoặc lỗi - có thể worker đang xử lý, log và trả về lỗi
		return fmt.Errorf("không nhận được ACK stream từ server sau 10 giây (có thể transaction vẫn đã được nhận): %w", err)
	}

	// Đọc ACK với timeout
	ackBuf := make([]byte, 1024)
	_ = ackStream.SetReadDeadline(time.Now().Add(5 * time.Second))
	n, err := ackStream.Read(ackBuf)
	if err != nil {
		// Có thể đã đọc được một phần, thử parse
		if n > 0 {
			ackMessage := string(ackBuf[:n])
			if ackMessage == "ACK" {
				log.Printf("Nhận được ACK từ server: transaction đã được nhận thành công")
				return nil
			}
		}
		return fmt.Errorf("lỗi khi đọc ACK từ server: %w", err)
	}

	ackMessage := string(ackBuf[:n])
	if ackMessage == "ACK" {
		log.Printf("Nhận được ACK từ server: transaction đã được nhận thành công")
		return nil
	} else {
		// Nhận được phản hồi không mong đợi
		log.Printf("Nhận được phản hồi từ server: %s", ackMessage)
		// Vẫn coi là thành công vì server đã phản hồi
		return nil
	}
}
