// network/config.go

package network

import (
	"runtime"
	"time"
)

// Config chứa tất cả các tham số cấu hình cho module network.
type Config struct {
	MaxMessageLength       uint64
	RequestChanSize        int
	ErrorChanSize          int
	WriteTimeout           time.Duration
	RequestChanWaitTimeout time.Duration
	DialTimeout            time.Duration
	RetryParentInterval    time.Duration
	HandlerWorkerPoolSize  int
	SendChanSize           int // <-- THÊM DÒNG NÀY
}

// DefaultConfig tự động tạo ra một cấu hình mặc định hợp lý
// dựa trên tài nguyên hệ thống có sẵn (cụ thể là số nhân CPU).
func DefaultConfig() *Config {
	numCPU := runtime.NumCPU()
	var numWorkers int
	if numCPU < 4 {
		numWorkers = 128
	} else if numCPU < 16 {
		numWorkers = numCPU * 64
	} else {
		numWorkers = numCPU * 32
		if numWorkers > 2048 {
			numWorkers = 2048
		}
	}

	requestQueueSize := numWorkers * 100

	return &Config{
		MaxMessageLength:       1024 * 1024 * 1024,
		HandlerWorkerPoolSize:  numWorkers,
		RequestChanSize:        requestQueueSize,
		SendChanSize:           65536, // <-- THÊM DÒNG NÀY: Kích thước buffer cho kênh gửi
		ErrorChanSize:          2000,
		WriteTimeout:           10 * time.Second,
		RequestChanWaitTimeout: 5 * time.Second,
		DialTimeout:            10 * time.Second,
		RetryParentInterval:    5 * time.Second,
	}
}
