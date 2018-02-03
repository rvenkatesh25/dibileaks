package message

import (
	"encoding/json"

	"github.com/thumbtack/go/lib/alfred/logging"
)

var logger = logging.NewLogger()

// PGBabelMessage represents the pgbabel payload and its associated lsn
type PGBabelMessage struct {
	babel *pgBabelData
	lsn   uint64
}

// pgBabelData represents the json received in each update from PG logical decoding plugin
type pgBabelData struct {
	Table     string                 `json:"table,omitempty"`
	Action    string                 `json:"action,omitempty"`
	PK        []string               `json:"pk,omitempty"`
	Timestamp int64                  `json:"timestamp,omitempty"`
	Data      map[string]interface{} `json:"data,omitempty"`
}

// String returns a string representation of the data structre
func (p *PGBabelMessage) String() string {
	s, _ := json.Marshal(p.babel)
	return string(s)
}

// GetAckID returns the LSN associated with this update
func (p *PGBabelMessage) GetAckID() uint64 {
	return uint64(p.lsn)
}

// ShouldAck returns true for COMMIT type updates, since the LSN should only
// be checkpointed on Commit
func (p *PGBabelMessage) ShouldAck() bool {
	return p.babel.Action == "COMMIT"
}

// PGBabelCodec holds the message encoding/decoding functions for babel
type PGBabelCodec struct{}

// CreateMessage create a PGBabelMessage from wal data and lsn
func (c *PGBabelCodec) CreateMessage(walData []byte, startLsn uint64) Interface {
	babel := &pgBabelData{}

	if len(walData) > 0 {
		err := json.Unmarshal(walData, babel)
		if err != nil {
			logger.Error("Error decoding babel update", logging.Fields{
				"error": err.Error(),
				"input": string(walData),
			})
			return nil
		}
	}

	return &PGBabelMessage{
		babel: babel,
		lsn:   startLsn,
	}
}
