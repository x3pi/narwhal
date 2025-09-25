package main

import (
	"flag"
	"fmt"
	"log"
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

func main() {
	var socketID int
	flag.IntVar(&socketID, "id", 0, "ID của socket executor")
	flag.Parse()

	// Sử dụng Listener từ module executor
	listener := executor.NewListener(socketID)
	if err := listener.Start(); err != nil {
		log.Fatalf("Lỗi khi khởi động listener: %v", err)
	}
	defer listener.Stop()

	log.Printf("Chương trình exetps đang chạy và sử dụng listener cho executor ID %d", socketID)

	counter := &TransactionCounter{}
	txChan := make(chan int)

	// Chạy goroutine để xử lý thống kê
	go processStats(txChan, counter)
	// Chạy goroutine để xử lý dữ liệu từ listener
	go processEpochData(listener.DataChannel(), txChan)

	// Giữ cho chương trình chính chạy vô hạn
	// Trong một ứng dụng thực tế, bạn có thể muốn xử lý tín hiệu OS để thoát một cách an toàn
	select {}
}

// Hàm mới để xử lý dữ liệu từ channel của Listener
func processEpochData(dataChan <-chan *pb.CommittedEpochData, txChan chan<- int) {
	for epochData := range dataChan {
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
