package common

import (
	"bufio"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math/big"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/holiman/uint256"

	"github.com/meta-node-blockchain/meta-node/pkg/logger"
)

var ErrorInvalidConnectionAddress = errors.New("invalid connection address")

func SplitConnectionAddress(address string) (ip string, port int, err error) {
	splited := strings.Split(address, ":")
	if len(splited) != 2 {
		return "", 0, ErrorInvalidConnectionAddress
	}
	intPort, err := strconv.Atoi(splited[1])
	if err != nil {
		return "", 0, err
	}
	return splited[0], intPort, nil
}

func AddressesToBytes(addresses []common.Address) [][]byte {
	rs := make([][]byte, len(addresses))
	for i, v := range addresses {
		rs[i] = v.Bytes()
	}
	return rs
}

func StringToUint256(str string) (*uint256.Int, bool) {
	bigInt := big.NewInt(0)
	_, success := bigInt.SetString(str, 10)
	return uint256.NewInt(0).SetBytes(bigInt.Bytes()), success
}

func GetRealConnectionAddress(dnsLink string, address common.Address) (string, error) {
	// Create a new HTTP client
	client := &http.Client{}
	strAddress := hex.EncodeToString(address.Bytes())
	// Create a new HTTP request
	req, err := http.NewRequest("GET", dnsLink+strAddress, nil)
	if err != nil {
		logger.Error("Error when get real connection address", err)
		return "", err
	}

	// Send the request and get the response
	resp, err := client.Do(req)
	if err != nil {
		logger.Error("Error when get real connection address", err)
		return "", err
	}

	// Decode the JSON response into a Go struct
	var jsonResponse map[string]string
	err = json.NewDecoder(resp.Body).Decode(&jsonResponse)
	if err != nil {
		logger.Error("Error when get real connection address", err)
		return "", err
	}
	if v, ok := jsonResponse[strAddress]; ok {
		return v, nil
	}
	err = errors.New("real connection address not found")
	logger.Error("Error when get real connection address", err)
	return "", err
}

func ReadLastLine(filePath string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	var lastLine string
	for scanner.Scan() {
		lastLine = scanner.Text()
	}

	if err := scanner.Err(); err != nil {
		return "", err
	}

	return lastLine, nil
}
