package bls

import (
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/meta-node-blockchain/meta-node/pkg/logger"
	"github.com/stretchr/testify/assert"
)

func TestNewKeyPair(t *testing.T) {
	keyPair := NewKeyPair(common.FromHex("7f3c9e7ddf6ddd318a102ae885c5078d1d5456ca55793c1382bbfda4a61e7e24"))
	logger.Info(keyPair)
	assert.NotNil(t, keyPair)
}
