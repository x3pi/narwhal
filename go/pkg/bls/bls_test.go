package bls

import (
	"encoding/hex"
	"fmt"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/assert"

	cm "github.com/meta-node-blockchain/meta-node/pkg/common"
	"github.com/meta-node-blockchain/meta-node/pkg/logger"
)

var (
	testSecret1 = "372e9d6411071707a7e7ba76a51c7907a6c799f0cb972df1671e582d649caabf"
	testSecret2 = "6e65d1ec7a396f422d7cce990485d2f2fc7703a3cda1e0da05806249f7e360c9"
	testSecret3 = "5ef8b3caa1c03827c1a2bfa12236450122d9321d8dd74dbe10a643de18e4fd5c"

	testPubkey1 = common.FromHex(
		"a2702ce6bbfb2e013935781bac50a0e168732bd957861e6fbf185d688c82ade34c9f33fead179decb5953b3382b061df",
	)
	testSign1 = common.FromHex(
		"a507c03ab7ebb69a4b3adc22a0347bb2466788e6a3baa174a62bd74cdff60dfd6d6ba9ec6237098f1ceef6013bfeff1d0c8be716266710e1493c422293a676e7f168007324a23435d4590896f97f8e3686cf0c280240b9406800c1cec6bafb5d",
	)
	testHash1 = common.HexToHash(
		"0x1111111111111111111111111111111111111111111111111111111111111111",
	)
)

func TestGenerateKeyPair(t *testing.T) {
	for range 64 {
		keyPair := GenerateKeyPair()
		fmt.Printf("%v,%v,%v\n", keyPair.PublicKey(), keyPair.PrivateKey(), keyPair.Address())
		assert.NotNil(t, keyPair)
	}
}

func TestGenerateKeyPairFromPrivateKey(t *testing.T) {
	sec1, pub1, address1 := GenerateKeyPairFromSecretKey(testSecret1)
	sec2, pub2, address2 := GenerateKeyPairFromSecretKey(testSecret2)
	sec3, pub3, address3 := GenerateKeyPairFromSecretKey(testSecret3)
	fmt.Printf(
		"Secret: %v\nPublic: %v\nAddress: %v\n",
		common.Bytes2Hex(sec1.Bytes()),
		common.Bytes2Hex(pub1.Bytes()),
		common.Bytes2Hex(address1.Bytes()),
	)
	fmt.Printf(
		"Secret: %v\nPublic: %v\nAddress: %v\n",
		common.Bytes2Hex(sec2.Bytes()),
		common.Bytes2Hex(pub2.Bytes()),
		common.Bytes2Hex(address2.Bytes()),
	)
	fmt.Printf(
		"Secret: %v\nPublic: %v\nAddress: %v\n",
		common.Bytes2Hex(sec3.Bytes()),
		common.Bytes2Hex(pub3.Bytes()),
		common.Bytes2Hex(address3.Bytes()),
	)
}

func TestSign(t *testing.T) {
	sign1 := Sign(cm.PrivateKeyFromBytes(common.FromHex(testSecret1)), testHash1.Bytes())
	sign2 := Sign(cm.PrivateKeyFromBytes(common.FromHex(testSecret2)), testHash1.Bytes())
	sign3 := Sign(cm.PrivateKeyFromBytes(common.FromHex(testSecret3)), testHash1.Bytes())
	fmt.Printf("Sign1: %v\n", common.Bytes2Hex(sign1.Bytes()))
	fmt.Printf("Sign2: %v\n", common.Bytes2Hex(sign2.Bytes()))
	fmt.Printf("Sign3: %v\n", common.Bytes2Hex(sign3.Bytes()))
}

func TestVerifySign(t *testing.T) {
	rs := VerifySign(
		cm.PubkeyFromBytes(testPubkey1),
		cm.SignFromBytes(testSign1),
		testHash1.Bytes(),
	)
	fmt.Println(rs)
}

func TestAggregateSign(t *testing.T) {
	_, pub1, _ := GenerateKeyPairFromSecretKey(testSecret1)
	_, pub2, _ := GenerateKeyPairFromSecretKey(testSecret2)
	_, pub3, _ := GenerateKeyPairFromSecretKey(testSecret3)

	sign1 := Sign(cm.PrivateKeyFromBytes(common.FromHex(testSecret1)), testHash1.Bytes())
	sign2 := Sign(cm.PrivateKeyFromBytes(common.FromHex(testSecret2)), testHash1.Bytes())
	sign3 := Sign(cm.PrivateKeyFromBytes(common.FromHex(testSecret3)), testHash1.Bytes())
	aggSign := CreateAggregateSign([][]byte{sign1.Bytes(), sign2.Bytes(), sign3.Bytes()})
	rs := VerifyAggregateSign(
		[][]byte{pub1[:], pub2[:], pub3[:]},
		aggSign,
		[][]byte{testHash1.Bytes(), testHash1.Bytes(), testHash1.Bytes()},
	)
	fmt.Println(pub1, pub2, pub3, hex.EncodeToString(aggSign))
	fmt.Println(rs)
}

func TestLargeAggregateSign(t *testing.T) {
	numOfSigns := 100_000
	signs := make([][]byte, numOfSigns)
	pri, _, _ := GenerateKeyPairFromSecretKey(testSecret1)
	for i := 0; i < numOfSigns; i++ {
		sign := Sign(pri, testHash1.Bytes())
		signs[i] = sign.Bytes()
	}
	start := time.Now()
	CreateAggregateSign(signs)
	took := time.Since(start)
	logger.Info("TestLargeAggregateSign", "numOfSigns", numOfSigns, "took", took)
}

func BenchmarkAggregateSign(b *testing.B) {
	numOfSigns := 100_000
	signs := make([][]byte, numOfSigns)
	kp := GenerateKeyPair()
	for i := 0; i < numOfSigns; i++ {
		sign := Sign(kp.PrivateKey(), testHash1.Bytes())
		signs[i] = sign.Bytes()
	}
	logger.Info("BenchmarkAggregateSign", "numOfSigns", numOfSigns)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i >= 100 {
			break
		}
		CreateAggregateSign(signs)
	}
}
