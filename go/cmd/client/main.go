package main

import (
	"encoding/binary"
	"log"

	"github.com/meta-node-blockchain/meta-node/pkg/txsender"
	// Thay thế bằng đường dẫn module thực tế của bạn.
)

// createSampleTransaction tạo ra một payload giao dịch mẫu.
func createSampleTransaction(id uint64, size int) []byte {
	payload := make([]byte, size)
	payload[0] = 1 // Loại giao dịch chuẩn
	binary.BigEndian.PutUint64(payload[1:9], id)
	return payload
}

func main() {
	nodeAddress := "127.0.0.1:4003"
	transactionSize := 128

	// 1. Khởi tạo client.
	client := txsender.NewClient(nodeAddress)

	// --- Gửi giao dịch 1 ---
	// Không cần gọi Connect() trước, SendTransaction sẽ tự động làm điều đó.
	log.Println("Đang gửi giao dịch #1...")
	txData1 := createSampleTransaction(101, transactionSize)
	if err := client.SendTransaction(txData1); err != nil {
		log.Fatalf("Gửi giao dịch #1 thất bại: %v", err)
	}
	log.Println("Giao dịch #1 đã gửi thành công.")

	// --- Gửi giao dịch 2 ---
	// Sẽ tái sử dụng kết nối từ lần gửi trước.
	log.Println("Đang gửi giao dịch #2...")
	txData2 := createSampleTransaction(102, transactionSize)
	if err := client.SendTransaction(txData2); err != nil {
		log.Fatalf("Gửi giao dịch #2 thất bại: %v", err)
	}
	log.Println("Giao dịch #2 đã gửi thành công.")

	// (Nếu máy chủ bị khởi động lại giữa giao dịch #1 và #2,
	// client sẽ phát hiện lỗi ở giao dịch #2 và tự động kết nối lại.)

	// 4. Đóng kết nối khi hoàn tất.
	log.Println("Đóng kết nối.")
	client.Close()
}
