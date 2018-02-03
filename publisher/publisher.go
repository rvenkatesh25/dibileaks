package publisher

import (
	"os"

	"github.com/thumbtack/go/dibileaks/comm"
)

// Interface interface defines the output distribution APIs
type Interface interface {
	// Start consumes string encoded messages from the input channel, publishes them to an
	// implementation specific medium and writes the successful ack ids to the output channel
	Start(map[string]*comm.DataChannels) error
}

// New returns a publisher implementation based on the specified type
func New(signalChan chan os.Signal, impl string) Interface {
	switch impl {
	case "file":
		return newFilePublisher(signalChan)
	}
	return nil
}
