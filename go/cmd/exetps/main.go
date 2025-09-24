package main

import (
	"bufio"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"time"

	// THAY ĐỔI: Import package protobuf được tạo ra ở Bước 2
	// Đường dẫn này có thể cần được điều chỉnh cho phù hợp với cấu trúc dự án của bạn
	pb "github.com/meta-node-blockchain/meta-node/pkg/proto"

	"google.golang.org/protobuf/proto"
)

const statsInterval = 30 * time.Second

// TransactionCounter không thay đổi
type TransactionCounter struct {
	mu       sync.Mutex
	totalTxs int64
}

func (tc *TransactionCounter) addTransactions(count int) {
	tc.mu.Lock()
	tc.totalTxs += int64(count)
	tc.mu.Unlock()
}

func (tc *TransactionCounter) reset() {
	tc.mu.Lock()
	tc.totalTxs = 0
	tc.mu.Unlock()
}

func main() {
	var socketID int
	flag.IntVar(&socketID, "id", 0, "ID của socket executor")
	flag.Parse()

	socketPath := fmt.Sprintf("/tmp/executor%d.sock", socketID)

	if err := os.RemoveAll(socketPath); err != nil {
		log.Fatalf("Không thể xóa file socket cũ: %v", err)
	}

	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		log.Fatalf("Lỗi khi lắng nghe trên socket: %v", err)
	}
	defer listener.Close()

	log.Printf("Máy chủ đang lắng nghe trên socket: %s", socketPath)

	counter := &TransactionCounter{}
	txChan := make(chan int)

	go processStats(txChan, counter)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Lỗi khi chấp nhận kết nối: %v", err)
			continue
		}
		go handleConnection(conn, txChan)
	}
}

// THAY ĐỔI: Cập nhật hoàn toàn để khớp với cấu trúc Protobuf của Rust
func handleConnection(conn net.Conn, txChan chan<- int) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	for {
		// Đọc độ dài của message (được mã hóa theo Uvarint)
		msgLen, err := binary.ReadUvarint(reader)
		if err != nil {
			if err != io.EOF {
				log.Printf("Lỗi khi đọc độ dài message: %v", err)
			}
			break
		}

		// Đọc đúng số byte của message vào buffer
		buf := make([]byte, msgLen)
		if _, err := io.ReadFull(reader, buf); err != nil {
			log.Printf("Lỗi khi đọc message vào buffer: %v", err)
			break
		}

		// Giải mã buffer bằng Protobuf theo cấu trúc của Rust
		var epochData pb.CommittedEpochData
		if err := proto.Unmarshal(buf, &epochData); err != nil {
			log.Printf("Lỗi khi giải mã Protobuf: %v", err)
			continue
		}

		// Logic xử lý và in log đã được cập nhật
		totalTxsInEpoch := 0
		for _, block := range epochData.Blocks {
			txCount := len(block.Transactions)
			totalTxsInEpoch += txCount

			fmt.Println("=========================================================")
			fmt.Printf("GO: Đã nhận block cho Round/Epoch: %d (Height: %d)\n", block.Epoch, block.Height)
			fmt.Printf("GO: Số lượng giao dịch trong block: %d\n", txCount)
			fmt.Printf("GO: Thời gian nhận: %s\n", time.Now().Format("15:04:05.000"))

			// In chi tiết một vài giao dịch đầu tiên để kiểm tra
			for i, tx := range block.Transactions {
				if i < 2 { // Chỉ in 2 giao dịch đầu tiên để tránh spam log
					// SỬA ĐỔI: Cập nhật tên trường in ra để rõ ràng hơn
					fmt.Printf("  -> Tx %d: Data[0:8]=%x, WorkerID=%d\n", i+1, tx.Digest[:8], tx.WorkerId)
				}
			}
			fmt.Println("=========================================================")
		}
		txChan <- totalTxsInEpoch
	}
}

// processStats không thay đổi
func processStats(txChan <-chan int, counter *TransactionCounter) {
	ticker := time.NewTicker(statsInterval)
	defer ticker.Stop()

	for {
		select {
		case count := <-txChan:
			counter.addTransactions(count)
		case <-ticker.C:
			total := counter.totalTxs
			tps := float64(total) / statsInterval.Seconds()

			fmt.Println("\n--- THỐNG KÊ TPS ---")
			fmt.Printf("Tổng số giao dịch đã nhận (trong %s): %d\n", statsInterval, total)
			fmt.Printf("TPS trung bình (trong %s): %.2f giao dịch/giây\n", statsInterval, tps)
			fmt.Println("--------------------")

			counter.reset()
		}
	}
}
