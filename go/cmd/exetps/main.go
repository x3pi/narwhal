package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	// Giả định 'executor' là một package có thể import được trong cùng project
	"github.com/meta-node-blockchain/meta-node/pkg/executor"
	pb "github.com/meta-node-blockchain/meta-node/pkg/proto"
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

// Hàm mới để thiết lập logger ghi vào file
func setupLogger(socketID int) (*os.File, error) {
	logFileName := fmt.Sprintf("./benchmark/logs/executor_%d.log", socketID)
	file, err := os.OpenFile(logFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		return nil, err
	}
	log.SetOutput(file)
	log.SetFlags(log.LstdFlags | log.Lmicroseconds) // Thêm microsecond để log chi tiết hơn
	log.Printf("------ Logger cho Executor ID %d đã khởi tạo ------", socketID)
	return file, nil
}

func main() {
	var socketID int
	flag.IntVar(&socketID, "id", 0, "ID của socket executor")
	flag.Parse()

	// Thiết lập logger để ghi vào file
	logFile, err := setupLogger(socketID)
	if err != nil {
		log.Fatalf("Lỗi khi khởi tạo file log: %v", err)
	}
	defer logFile.Close()

	// Sử dụng Listener từ module executor
	listener := executor.NewListener(socketID)
	if err := listener.Start(); err != nil {
		log.Fatalf("Lỗi khi khởi động listener: %v", err)
	}
	defer listener.Stop()

	log.Printf("Chương trình executor đang chạy, sử dụng listener cho ID %d", socketID)

	counter := &TransactionCounter{}
	txChan := make(chan int)

	// Chạy goroutine để xử lý thống kê
	go processStats(txChan, counter)
	// Chạy goroutine để xử lý dữ liệu từ listener
	go processEpochData(listener.DataChannel(), txChan)

	// Giữ cho chương trình chính chạy vô hạn
	select {}
}

// Hàm xử lý dữ liệu từ channel, giờ đây sẽ ghi vào file log
func processEpochData(dataChan <-chan *pb.CommittedEpochData, txChan chan<- int) {
	for epochData := range dataChan {
		totalTxsInEpoch := 0
		for _, block := range epochData.Blocks {
			txCount := len(block.Transactions)
			totalTxsInEpoch += txCount

			// Ghi log thông tin block nhận được
			log.Println("=====================================================================")
			if txCount > 0 {
				log.Printf("Đã nhận block cho Round/Epoch: %d (Height: %d)", block.Epoch, block.Height)
				log.Printf("Số lượng giao dịch trong block: %d", txCount)
			} else {
				// Ghi log đặc biệt cho block rỗng
				log.Printf("Đã nhận BLOCK RỖNG cho Round/Epoch: %d (Height: %d)", block.Epoch, block.Height)
			}

			// In chi tiết một vài giao dịch đầu tiên để kiểm tra
			for i, tx := range block.Transactions {
				if i < 3 { // Chỉ log 3 giao dịch đầu tiên
					log.Printf("  -> Tx %d: Data[0:8]=%x, WorkerID=%d", i+1, tx.Digest[:8], tx.WorkerId)
				}
			}
			log.Println("=====================================================================")

		}
		txChan <- totalTxsInEpoch
	}
}

// processStats giờ đây sẽ ghi thống kê vào file log
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

			log.Println("\n--- THỐNG KÊ TPS ---")
			log.Printf("Tổng số giao dịch đã xử lý (trong %s): %d", statsInterval, total)
			log.Printf("TPS trung bình (trong %s): %.2f giao dịch/giây", statsInterval, tps)
			log.Println("--------------------\n")

			counter.reset()
		}
	}
}