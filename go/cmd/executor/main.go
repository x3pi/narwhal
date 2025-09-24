package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
)

// Đường dẫn đến file Unix Domain Socket, phải khớp với client Rust.
const socketPath = "/tmp/executor.sock"

// FullBlock định nghĩa cấu trúc của một block đầy đủ thông tin, khớp với struct Rust.
type FullBlock struct {
	Author       string   `json:"author"`
	Epoch        uint64   `json:"epoch"`
	Height       uint64   `json:"height"`
	Transactions [][]byte `json:"transactions"`
}

// CommittedEpochData định nghĩa cấu trúc dữ liệu cấp cao nhất cho một epoch đã commit.
type CommittedEpochData struct {
	Epoch  uint64      `json:"epoch"`
	Blocks []FullBlock `json:"blocks"`
}

func main() {
	// Xóa file socket cũ nếu nó tồn tại để tránh lỗi "address already in use".
	if err := os.RemoveAll(socketPath); err != nil {
		log.Fatalf("Không thể xóa file socket cũ: %v", err)
	}

	// Lắng nghe trên Unix Domain Socket.
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		log.Fatalf("Lỗi khi lắng nghe trên socket: %v", err)
	}
	// Đảm bảo listener sẽ được đóng khi hàm main kết thúc.
	defer listener.Close()

	log.Printf("Máy chủ đang lắng nghe trên socket: %s", socketPath)

	// Vòng lặp vô hạn để chấp nhận các kết nối mới.
	for {
		// Chấp nhận một kết nối đến.
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Lỗi khi chấp nhận kết nối: %v", err)
			continue // Bỏ qua lỗi và tiếp tục chờ kết nối khác.
		}

		// Xử lý mỗi kết nối trong một goroutine riêng để không làm chặn vòng lặp chính.
		go handleConnection(conn)
	}
}

// handleConnection xử lý việc đọc và giải mã dữ liệu từ một kết nối.
func handleConnection(conn net.Conn) {
	// Đảm bảo kết nối sẽ được đóng sau khi xử lý xong.
	defer conn.Close()
	log.Printf("Đã nhận kết nối từ: %s", conn.RemoteAddr().String())

	// Tạo một bộ giải mã JSON để đọc trực tiếp từ luồng kết nối.
	decoder := json.NewDecoder(conn)

	var epochData CommittedEpochData

	// Giải mã dữ liệu JSON nhận được vào struct CommittedEpochData.
	if err := decoder.Decode(&epochData); err != nil {
		// Nếu có lỗi (ví dụ: client đóng kết nối, dữ liệu không hợp lệ), ghi lại log.
		if err.Error() != "EOF" {
			log.Printf("Lỗi khi giải mã JSON: %v", err)
		}
		return
	}

	// In dữ liệu đã giải mã ra console.
	fmt.Println("=========================================================")
	fmt.Printf("ĐÃ NHẬN DỮ LIỆU EPOCH: %d\n", epochData.Epoch)
	fmt.Printf("Tổng số block: %d\n", len(epochData.Blocks))
	fmt.Println("---------------------------------------------------------")

	for i, block := range epochData.Blocks {
		fmt.Printf("  Block %d:\n", i+1)
		fmt.Printf("    - Author: %s\n", block.Author)
		fmt.Printf("    - Height: %d\n", block.Height)
		fmt.Printf("    - Số lượng giao dịch: %d\n", len(block.Transactions))

		// VÒNG LẶP MỚI ĐỂ IN MÃ BYTE CỦA TỪNG GIAO DỊCH
		for j, tx := range block.Transactions {
			fmt.Printf("      + Giao dịch %d (mã hex): %x\n", j+1, tx)
		}
	}
	fmt.Println("=========================================================")
}
