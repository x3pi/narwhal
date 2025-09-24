package bls

import (
	"crypto/rand"
	"encoding/hex"
	"runtime"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	blst "github.com/meta-node-blockchain/meta-node/pkg/bls/blst/bindings/go"
	cm "github.com/meta-node-blockchain/meta-node/pkg/common"
)

type blstPublicKey = blst.P1Affine
type blstSignature = blst.P2Affine
type blstAggregateSignature = blst.P2Aggregate
type blstAggregatePublicKey = blst.P1Aggregate
type blstSecretKey = blst.SecretKey

var dstMinPk = []byte("BLS_SIG_BLS12381G2_XMD:SHA-256_SSWU_RO_POP_")

func Init() {
	blst.SetMaxProcs(runtime.GOMAXPROCS(0))
}

func Sign(bPri cm.PrivateKey, bMessage []byte) cm.Sign {
	sk := new(blstSecretKey).Deserialize(bPri.Bytes())
	sign := new(blstSignature).Sign(sk, bMessage, dstMinPk)
	return cm.SignFromBytes(sign.Compress())
}

func GetByteAddress(pubkey []byte) []byte {
	hash := crypto.Keccak256(pubkey)
	address := hash[12:]
	return address
}

func VerifySign(bPub cm.PublicKey, bSig cm.Sign, bMsg []byte) bool {
	return new(blstSignature).VerifyCompressed(bSig.Bytes(), true, bPub.Bytes(), false, bMsg, dstMinPk)
}

func VerifyAggregateSign(bPubs [][]byte, bSig []byte, bMsgs [][]byte) bool {
	return new(blstSignature).AggregateVerifyCompressed(bSig, true, bPubs, false, bMsgs, dstMinPk)
}

func GenerateKeyPairFromSecretKey(hexSecretKey string) (cm.PrivateKey, cm.PublicKey, common.Address) {
	secByte, _ := hex.DecodeString(hexSecretKey)
	sec := new(blstSecretKey).Deserialize(secByte)
	pub := new(blstPublicKey).From(sec).Compress()
	hash := crypto.Keccak256([]byte(pub))
	return cm.PrivateKeyFromBytes(sec.Serialize()), cm.PubkeyFromBytes(pub), common.BytesToAddress(hash[12:])
}

func randBLSTSecretKey() *blstSecretKey {
	var t [32]byte
	_, _ = rand.Read(t[:])
	secretKey := blst.KeyGen(t[:])
	return secretKey
}

func GenerateKeyPair() *KeyPair {
	sec := randBLSTSecretKey()
	return NewKeyPair(sec.Serialize())
}

func CreateAggregateSign(bSignatures [][]byte) []byte {
	aggregatedSignature := new(blst.P2Aggregate)
	aggregatedSignature.AggregateCompressed(bSignatures, false)
	return aggregatedSignature.ToAffine().Compress()
}
