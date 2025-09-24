package logger

import (
	"bytes"
	"encoding/json" // Added for robust JSON string escaping
	"fmt"
	"net/http"
	"os"
	"strconv" // Added for int/uint to string conversion
	"time"
)

const (
	FLAG_DEBUGP   = 0
	FLAG_TELEGRAM = 6
	FLAG_TRACE    = 5
	FLAG_DEBUG    = 4
	FLAG_INFO     = 3
	FLAG_WARN     = 2
	FLAG_ERROR    = 1

	Reset  = "\033[0m"
	Red    = "\033[31m"
	Green  = "\033[32m"
	Yellow = "\033[33m"
	Blue   = "\033[34m"
	Purple = "\033[35m"
	Cyan   = "\033[36m"
	Gray   = "\033[37m"
	White  = "\033[97m"
)

type LoggerConfig struct {
	Flag             int
	Identifier       string
	TelegramToken    string
	TelegramChatId   int
	TelegramThreadId uint
	Outputs          []*os.File
}

type Logger struct {
	Config *LoggerConfig
}

var config = &LoggerConfig{
	Flag:             FLAG_INFO,
	Outputs:          []*os.File{os.Stdout},
	TelegramChatId:   0,
	TelegramToken:    "",
	TelegramThreadId: 0, // Default
	// TelegramThreadId: 6010, // Devnet
	// TelegramThreadId: 6759, // Testnet
	// TelegramThreadId: 1, // worknet
}

var logger = &Logger{
	Config: config,
}

func SetConfig(newConfig *LoggerConfig) {
	config = newConfig
}

func SetOutputs(outputs []*os.File) {
	config.Outputs = outputs
}

func SetFlag(flag int) {
	config.Flag = flag
}

func SetTelegramInfo(token string, chatId int) {
	config.TelegramToken = token
	config.TelegramChatId = chatId
	config.TelegramThreadId = 0
}

func SetTelegramGroupInfo(token string, chatId int, threadId uint) {
	config.TelegramToken = token
	config.TelegramChatId = chatId
	config.TelegramThreadId = threadId
}

func SetIdentifier(identifier string) {
	config.Identifier = identifier
}

func DebugP(message interface{}, a ...interface{}) {
	if config.Flag < FLAG_DEBUGP {
		return
	}
	logger.writeToOutputs(
		getLogBuffer(Purple, "DEBUG_P", message, a),
	)
}

func Trace(message interface{}, a ...interface{}) {
	if config.Flag < FLAG_TRACE {
		return
	}
	logger.writeToOutputs(
		getLogBuffer(Blue, "TRACE", message, a),
	)
}

func Debug(message interface{}, a ...interface{}) {
	if config.Flag < FLAG_DEBUG {
		return
	}
	logger.writeToOutputs(
		getLogBuffer(Cyan, "DEBUG", message, a),
	)
}

func Info(message interface{}, a ...interface{}) {
	if config.Flag < FLAG_INFO {
		return
	}
	logger.writeToOutputs(
		getLogBuffer(Green, "INFO", message, a),
	)
}

func Warn(message interface{}, a ...interface{}) {
	if config.Flag < FLAG_WARN {
		return
	}
	logger.writeToOutputs(
		getLogBuffer(Yellow, "WARN", message, a),
	)
}

func Error(message interface{}, a ...interface{}) {
	if config.Flag < FLAG_ERROR {
		return
	}
	logBytes := getLogBuffer("", "ERROR", message, a) // Get log bytes without console colors for Telegram
	if config.TelegramToken != "" && config.TelegramChatId != 0 {
		sendToTelegram(logBytes)
	}
	// For console output, use the version with color
	logger.writeToError(getLogBuffer(Red, "ERROR", message, a))
}

func Telegram(message interface{}, a ...interface{}) {
	if config.Flag < FLAG_TELEGRAM {
		return
	}
	sendToTelegram(
		getLogBuffer("", "TELE", message, a), // Send without console colors
	)
}

func sendToTelegram(messageContent []byte) {
	var jsonPayload bytes.Buffer
	jsonPayload.WriteString(`{"chat_id": "`)
	jsonPayload.WriteString(strconv.Itoa(config.TelegramChatId))
	jsonPayload.WriteString(`", `)

	if config.TelegramThreadId > 0 {
		jsonPayload.WriteString(`"message_thread_id": "`)
		jsonPayload.WriteString(strconv.FormatUint(uint64(config.TelegramThreadId), 10))
		jsonPayload.WriteString(`", `)
	}

	jsonPayload.WriteString(`"text": `)
	// Use json.Marshal to correctly escape the message content for JSON.
	// This will also add the surrounding quotes to the message string.
	escapedMessage, err := json.Marshal(string(messageContent))
	if err != nil {
		// Fallback or log error if marshalling fails, though unlikely for a string.
		// For simplicity, writing the raw string (less safe for complex content).
		// A more robust solution would log this marshalling error.
		fmt.Printf("Error marshalling telegram message content: %v. Sending raw.\n", err)
		jsonPayload.WriteString(`"`)
		jsonPayload.Write(messageContent) // Might be problematic if content has unescaped quotes
		jsonPayload.WriteString(`"`)
	} else {
		jsonPayload.Write(escapedMessage)
	}

	jsonPayload.WriteString(`}`)

	apiUrl := "https://api.telegram.org/bot" + config.TelegramToken + "/sendMessage"
	resp, err := http.Post(
		apiUrl,
		"application/json",
		bytes.NewBuffer(jsonPayload.Bytes()),
	)

	if err != nil {
		// Use your logger here if it's safe and won't cause a loop,
		// otherwise fmt.Println for critical network errors.
		fmt.Printf("Error sending message to Telegram: %v\n", err)
		if resp != nil {
			resp.Body.Close()
		}
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		var body bytes.Buffer
		_, readErr := body.ReadFrom(resp.Body)
		responseText := body.String()
		if readErr != nil {
			responseText = "could not read error response body"
		}
		// Use your logger here if safe.
		fmt.Printf("Error response from Telegram API: %s, Body: %s\n", resp.Status, responseText)
	}
}

func getLogBuffer(color string, prefix string, message interface{}, a []interface{}) []byte {
	var buffer bytes.Buffer        // This will hold the final log entry metadata + content
	var contentBuffer bytes.Buffer // This will hold just the formatted message content

	// Add color if specified (only for console, Telegram messages get "" for color)
	if color != "" {
		buffer.WriteString(color)
	}

	// Add Identifier if specified
	if config.Identifier != "" {
		buffer.WriteString("[")
		buffer.WriteString(config.Identifier)
		buffer.WriteString("]")
	}

	// Add log level prefix and timestamp
	buffer.WriteString("[")
	buffer.WriteString(prefix)
	buffer.WriteString("][")
	buffer.WriteString(time.Now().Format(time.Stamp))
	buffer.WriteString("] ") // Space after the timestamp and metadata

	// --- Content Formatting Logic ---
	if formatStr, ok := message.(string); ok && len(a) > 0 {
		// If 'message' is a string and there are additional arguments in 'a',
		// treat 'message' as a format string for those arguments.
		contentBuffer.WriteString(fmt.Sprintf(formatStr, a...))
	} else {
		// Otherwise (if 'message' is not a string, or 'a' is empty),
		// print 'message' as is.
		contentBuffer.WriteString(fmt.Sprintf("%v", message))
		// And if there are any arguments in 'a' (this case primarily occurs if 'message' was not a string but 'a' is populated),
		// print them, each prefixed by a newline.
		for _, v_item := range a {
			contentBuffer.WriteString("\n") // Start additional items on a new line
			contentBuffer.WriteString(fmt.Sprintf("%v", v_item))
		}
	}

	// Add a space after the main content, unless the content itself ends with a newline from multi-line 'a' arguments.
	// The original `fmt.Sprintf("%v ", message)` effectively added a space.
	// The new `WriteString(" ")` aims to replicate this for single-line formatted messages.
	// Multi-line messages (from the `else` block with `a` items) will naturally have newlines.
	// This ensures a space separates the content from potential color reset codes or the final newline.
	// Check if contentBuffer already ends with a newline.
	contentBytes := contentBuffer.Bytes()
	if len(contentBytes) > 0 && contentBytes[len(contentBytes)-1] != '\n' {
		buffer.WriteString(" ") // Add trailing space if not ending with newline
	}

	buffer.Write(contentBytes) // Write the formatted content to the main buffer

	// Add color reset if color was used
	if color != "" && color != Reset { // Avoid double Reset if "" was passed and Reset is also ""
		buffer.WriteString(Reset)
	}

	// Add final newline for the log entry
	buffer.WriteString("\n")
	return buffer.Bytes()
}

func (l *Logger) writeToOutputs(buffer []byte) {
	for i := range l.Config.Outputs { // Corrected from config.Outputs to l.Config.Outputs
		if l.Config.Outputs[i] != nil {
			l.Config.Outputs[i].Write(buffer)
		}
	}
}

func (l *Logger) writeToError(buffer []byte) { // Corrected from logger to l
	os.Stderr.Write(buffer)
}
