package types

type Config interface {
	Version() string
	NodeType() string
	PrivateKey() []byte
	ConnectionAddress() string
}
