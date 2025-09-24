package network

import (
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	cm "github.com/meta-node-blockchain/meta-node/pkg/common"
	pb "github.com/meta-node-blockchain/meta-node/pkg/proto"
	"github.com/meta-node-blockchain/meta-node/types/network"
)

type Message struct {
	proto *pb.Message
}

func NewMessage(pbMessage *pb.Message) network.Message {
	if pbMessage == nil {
		pbMessage = &pb.Message{
			Header: &pb.Header{},
		}
	} else if pbMessage.Header == nil {
		pbMessage.Header = &pb.Header{}
	}
	return &Message{
		proto: pbMessage,
	}
}

func (m *Message) Marshal() ([]byte, error) {
	if m == nil || m.proto == nil {
		return nil, errors.New("Message.Marshal: không thể marshal tin nhắn nil")
	}
	return proto.Marshal(m.proto)
}

func (m *Message) Unmarshal(protoStruct protoreflect.ProtoMessage) error {
	if m == nil || m.proto == nil {
		return errors.New("Message.Unmarshal: không thể unmarshal từ tin nhắn nil")
	}
	if protoStruct == nil {
		return errors.New("Message.Unmarshal: protoStruct không được là nil")
	}
	return proto.Unmarshal(m.proto.Body, protoStruct)
}

func (m *Message) String() string {
	if m == nil || m.proto == nil || m.proto.Header == nil {
		return "<Message: nil>"
	}
	return fmt.Sprintf(`
	Message Info:
		ID: %s, Command: %s, Version: %s, ToAddress: %s
	Body (hex): %s
`,
		m.proto.Header.ID,
		m.proto.Header.Command,
		m.proto.Header.Version,
		common.BytesToAddress(m.proto.Header.ToAddress).Hex(),
		hex.EncodeToString(m.proto.Body),
	)
}

func (m *Message) Command() string {
	if m == nil || m.proto == nil || m.proto.Header == nil {
		return ""
	}
	return m.proto.Header.Command
}

func (m *Message) Version() string {
	if m == nil || m.proto == nil || m.proto.Header == nil {
		return ""
	}
	return m.proto.Header.Version
}

func (m *Message) Body() []byte {
	if m == nil || m.proto == nil {
		return nil
	}
	return m.proto.Body
}

func (m *Message) Pubkey() cm.PublicKey {
	if m == nil || m.proto == nil || m.proto.Header == nil || m.proto.Header.Pubkey == nil {
		return cm.PublicKey{}
	}
	return cm.PubkeyFromBytes(m.proto.Header.Pubkey)
}

func (m *Message) Sign() cm.Sign {
	if m == nil || m.proto == nil || m.proto.Header == nil || m.proto.Header.Sign == nil {
		return cm.Sign{}
	}
	return cm.SignFromBytes(m.proto.Header.Sign)
}

func (m *Message) ToAddress() common.Address {
	if m == nil || m.proto == nil || m.proto.Header == nil || m.proto.Header.ToAddress == nil {
		return common.Address{}
	}
	return common.BytesToAddress(m.proto.Header.ToAddress)
}

func (m *Message) ID() string {
	if m == nil || m.proto == nil || m.proto.Header == nil {
		return ""
	}
	return m.proto.Header.ID
}
