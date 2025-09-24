package common

import (
	"encoding/hex"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

type PublicKey [48]byte
type PrivateKey [32]byte
type Sign [96]byte

func (pk PublicKey) Bytes() []byte {
	return pk[:]
}

func (pk PublicKey) String() string {
	return hex.EncodeToString(pk[:])
}

func (pk PrivateKey) Bytes() []byte {
	return pk[:]
}

func (pk PrivateKey) String() string {
	return hex.EncodeToString(pk[:])
}

func (s Sign) Bytes() []byte {
	return s[:]
}

func (s Sign) String() string {
	return hex.EncodeToString(s[:])
}

func PubkeyFromBytes(bytes []byte) PublicKey {
	p := PublicKey{}
	copy(p[0:48], bytes)
	return p
}

func SignFromBytes(bytes []byte) Sign {
	s := Sign{}
	copy(s[0:96], bytes)
	return s
}

func PrivateKeyFromBytes(bytes []byte) PrivateKey {
	p := PrivateKey{}
	copy(p[0:32], bytes)
	return p
}

func AddressFromPubkey(pk PublicKey) common.Address {
	hash := crypto.Keccak256(pk[:])
	return common.BytesToAddress(hash[12:])
}
