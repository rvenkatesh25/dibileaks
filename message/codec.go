package message

// Interface defines the methods for the data object received from
// whistle blowers (pg or ddb)
type Interface interface {
	String() string
	GetAckID() uint64
	ShouldAck() bool
}

// Codec interface defines the methods for a format-specific codec implementation
type Codec interface {
	CreateMessage([]byte, uint64) Interface
}

// NewCodec instantiates a new codec of the specified implementation
func NewCodec(impl string) Codec {
	switch impl {
	default:
		return &PGBabelCodec{}
	}
}
