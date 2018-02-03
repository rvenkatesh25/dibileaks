package comm

import (
	"github.com/thumbtack/go/dibileaks/message"
)

// DataChannels encapsulates the message and ack id channels used to communicate between
// whistle blowers and publishers
type DataChannels struct {
	MsgChan chan message.Interface
	AckChan chan uint64
}
