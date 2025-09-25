package executor

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"

	// Đảm bảo đường dẫn import này khớp với module go của bạn
	pb "github.com/meta-node-blockchain/meta-node/pkg/proto"

	"google.golang.org/protobuf/proto"
)

// Listener là một thành phần có thể tái sử dụng để lắng nghe dữ liệu CommittedEpochData trên một socket unix.
type Listener struct {
	socketPath string
	listener   net.Listener
	dataChan   chan *pb.CommittedEpochData // Channel để gửi dữ liệu đã giải mã ra bên ngoài
	wg         sync.WaitGroup
	quit       chan struct{}
}

// NewListener tạo và khởi tạo một instance Listener mới.
// Nó nhận vào socketID để tạo đường dẫn socket động.
func NewListener(socketID int) *Listener {
	socketPath := fmt.Sprintf("/tmp/executor%d.sock", socketID)
	return &Listener{
		socketPath: socketPath,
		// Channel có bộ đệm để tránh block goroutine gửi nếu bên nhận chưa sẵn sàng
		dataChan: make(chan *pb.CommittedEpochData, 100),
		quit:     make(chan struct{}),
	}
}

// Start bắt đầu lắng nghe các kết nối trên socket đã chỉ định.
// Đây là một non-blocking call.
func (l *Listener) Start() error {
	// Xóa file socket cũ nếu tồn tại
	if err := os.RemoveAll(l.socketPath); err != nil {
		return fmt.Errorf("không thể xóa file socket cũ: %v", err)
	}

	// Lắng nghe trên socket unix
	listener, err := net.Listen("unix", l.socketPath)
	if err != nil {
		return fmt.Errorf("lỗi khi lắng nghe trên socket: %v", err)
	}
	l.listener = listener

	log.Printf("Module Listener đang lắng nghe trên: %s", l.socketPath)

	l.wg.Add(1)
	go l.acceptConnections() // Chạy goroutine để chấp nhận kết nối

	return nil
}

// Stop đóng listener một cách an toàn và chờ tất cả các goroutine kết thúc.
func (l *Listener) Stop() {
	close(l.quit) // Gửi tín hiệu dừng
	if l.listener != nil {
		l.listener.Close() // Đóng listener để ngắt vòng lặp accept
	}
	l.wg.Wait()       // Chờ các goroutine xử lý xong
	close(l.dataChan) // Đóng data channel
	log.Println("Module Listener đã dừng.")
}

// DataChannel trả về một channel chỉ đọc (read-only) để client có thể nhận dữ liệu.
func (l *Listener) DataChannel() <-chan *pb.CommittedEpochData {
	return l.dataChan
}

// acceptConnections chấp nhận các kết nối đến và xử lý chúng trong một goroutine mới.
func (l *Listener) acceptConnections() {
	defer l.wg.Done()
	for {
		conn, err := l.listener.Accept()
		if err != nil {
			// Kiểm tra xem lỗi có phải do listener đã bị đóng không
			select {
			case <-l.quit:
				return // Thoát nếu đã nhận tín hiệu dừng
			default:
				log.Printf("Lỗi khi chấp nhận kết nối: %v", err)
				continue
			}
		}
		// Xử lý mỗi kết nối trong một goroutine riêng để không block việc chấp nhận kết nối mới
		go l.handleConnection(conn)
	}
}

// handleConnection đọc, giải mã dữ liệu từ một kết nối và gửi nó qua dataChan.
func (l *Listener) handleConnection(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	for {
		// Đọc độ dài của message (mã hóa Uvarint)
		msgLen, err := binary.ReadUvarint(reader)
		if err != nil {
			if err != io.EOF {
				log.Printf("Lỗi khi đọc độ dài message: %v", err)
			}
			return // Kết thúc goroutine này khi có lỗi hoặc client đóng kết nối
		}

		// Đọc message vào buffer
		buf := make([]byte, msgLen)
		if _, err := io.ReadFull(reader, buf); err != nil {
			log.Printf("Lỗi khi đọc message vào buffer: %v", err)
			return
		}

		// Giải mã buffer bằng Protobuf
		var epochData pb.CommittedEpochData
		if err := proto.Unmarshal(buf, &epochData); err != nil {
			log.Printf("Lỗi khi giải mã Protobuf: %v", err)
			continue // Tiếp tục vòng lặp để đọc message tiếp theo
		}

		// Gửi dữ liệu đã giải mã qua channel để bên ngoài xử lý
		l.dataChan <- &epochData
	}
}
