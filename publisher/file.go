package publisher

import (
	"fmt"
	"os"
	"sync"
	"syscall"

	"github.com/thumbtack/go/lib/alfred/logging"

	"github.com/thumbtack/go/dibileaks/comm"
	"github.com/thumbtack/go/dibileaks/message"
)

// FilePublisher represents the output mode where the updates are appeneded to a file
type FilePublisher struct {
	path       string
	logger     logging.Logger
	signalChan chan os.Signal
	stopping   bool
	stopMux    sync.Mutex
}

// newFilePublisher returns a file publisher that appends data for each db in a separate
// file in the given path. Since this is only used for dev / local testing, we use /tmp
func newFilePublisher(signalChan chan os.Signal) *FilePublisher {
	return &FilePublisher{
		path:       "/tmp",
		logger:     logging.NewLogger(),
		signalChan: signalChan,
		stopping:   false,
	}
}

// Start creates a file publisher thread for each whistle blower config
func (pub *FilePublisher) Start(commChannels map[string]*comm.DataChannels) error {
	for name, commChannel := range commChannels {
		go pub.write(name, commChannel.MsgChan, commChannel.AckChan)
		pub.logger.Info("Started file publisher for DB", logging.Fields{
			"name": name,
		})
	}
	return nil
}

// errorStop is called from one or more of the spawned goroutines when they
// encounter a fatal error
func (pub *FilePublisher) errorStop() {
	pub.stopMux.Lock()
	defer pub.stopMux.Unlock()

	if !pub.stopping {
		pub.stopping = true
		pub.signalChan <- syscall.SIGABRT
	}

}

// write writes a message fetched from the input message channel and writes to a file
// It returns the ackID of the message to the ackOut channel if applicable
func (pub *FilePublisher) write(
	name string,
	msgIn <-chan message.Interface,
	ackOut chan<- uint64,
) {
	filename := fmt.Sprintf("%s/%s.log", pub.path, name)
	f, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		pub.logger.Error("Error opening file", logging.Fields{
			"error":    err.Error(),
			"filename": filename,
		})
		pub.errorStop()
		return
	}
	defer f.Close()

	for {
		msg, more := <-msgIn
		if !more {
			pub.logger.Info("Stopping publisher thread for DB", logging.Fields{
				"name": name,
			})

			// close the ack channel, this will terminate the lsn commit thread for pg
			close(ackOut)
			return
		}
		_, err := f.WriteString(msg.String() + "\n")
		if err != nil {
			pub.logger.Error("Error writing message to file", logging.Fields{
				"error": err.Error(),
			})
			continue
		}

		err = f.Sync()
		if err != nil {
			pub.logger.Error("Error syncing file to disk", logging.Fields{
				"error": err.Error(),
			})
			continue
		}

		if msg.ShouldAck() {
			ackOut <- msg.GetAckID()
		}

		pub.logger.Debug("Wrote message to file", logging.Fields{
			"message": msg.String(),
		})
	}
}
