package logger

import (
	"os"
	"testing"
)

func TestLogToFile(t *testing.T) {
	f, _ := os.OpenFile("./test_log.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	config.Outputs = append(config.Outputs, f)
	Info("Test Log To File", "Oke")
}

func TestLogErrWithTelegram(t *testing.T) {
	config.TelegramChatId = -4102931211
	config.TelegramToken = "6301510657:AAG6llK5jeEzLKg-ZUD87pD_dc2IP1m16Ls"
	Error("Test Log Error With Telegram", 123, "oke")
}

func TestLogTelegram(t *testing.T) {
	config.Identifier = "TEST"
	config.Flag = FLAG_TELEGRAM
	config.TelegramChatId = -4102931211
	config.TelegramToken = "6301510657:AAG6llK5jeEzLKg-ZUD87pD_dc2IP1m16Ls"
	Telegram("Test Log Telegram", "xxx", "oke")
}

func TestLogTopicTelegram(t *testing.T) {
	config.Identifier = "TEST"
	config.Flag = FLAG_TELEGRAM
	config.TelegramThreadId = 18
	config.TelegramChatId = -1002134567419
	config.TelegramToken = "1445784583:AAGIJpLzxGsB7csutW2fj7MVIOQlvQNa1XY"
	Telegram("Test Log Telegram", "xxx", "oke")
}
