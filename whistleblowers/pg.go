package whistleblowers

import (
	"context"
	"fmt"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx"

	"github.com/thumbtack/go/lib/alfred/logging"
	"github.com/thumbtack/go/lib/config"

	"github.com/thumbtack/go/dibileaks/comm"
	"github.com/thumbtack/go/dibileaks/message"
)

const (
	heartbeatIntervalInSeconds = 10
)

// PG holds configuration for all databases for which logical replication is configured
type PG struct {
	dbConfigs  map[string]*dbConfig
	logger     logging.Logger
	signalChan chan os.Signal

	stopping bool
	stopMux  sync.Mutex
}

// NewPostgresConfig reads the json config file and loads the logical replication
// configuration for all specified databases
func NewPostgresConfig(signalChan chan os.Signal) *PG {
	p := &PG{
		dbConfigs:  make(map[string]*dbConfig),
		logger:     logging.NewLogger(),
		signalChan: signalChan,
		stopping:   false,
	}
	p.readConfigs()

	return p
}

// Start connects to PG over a replication connection and begins streaming
// updates for each configured db
// It returns the communication channels for message and ack ids/lsn
func (p *PG) Start() (map[string]*comm.DataChannels, error) {
	p.logger.Info("Starting PG Whistle Blowers", logging.Fields{})

	commChannels := make(map[string]*comm.DataChannels)

	for name, dbCfg := range p.dbConfigs {
		if err := dbCfg.start(); err != nil {
			return nil, err
		}

		commChannels[name] = &comm.DataChannels{
			MsgChan: dbCfg.msgChan,
			AckChan: dbCfg.lsnChan,
		}
	}

	return commChannels, nil
}

// Stop signals all spawned goroutines to stop
func (p *PG) Stop(onError bool) {
	p.logger.Info("Stopping PG Whistle Blowers", logging.Fields{})

	for _, dbCfg := range p.dbConfigs {
		dbCfg.stop(onError)
	}
}

// errorStop is called from one or more of the spawned goroutines when they
// encounter a fatal error
func (p *PG) errorStop() {
	p.stopMux.Lock()
	defer p.stopMux.Unlock()

	if !p.stopping {
		p.stopping = true
		p.signalChan <- syscall.SIGABRT
	}

}

// readConfigs reads the json config specifying connection and replication slot details
// of each db that needs to be tailed
func (p *PG) readConfigs() {
	cfgList := config.NewMustJSON("postgres_databases.json")

	for _, cfg := range cfgList.MustArray("active_dbs") {
		c := &pgx.ConnConfig{
			Host:      cfg.MustString("host"),
			Port:      uint16(cfg.MustInt("port")),
			Database:  cfg.MustString("name"),
			User:      cfg.MustString("user"),
			Password:  cfg.MustString("pass"),
			TLSConfig: nil, //TODO: handle TLS for non-local configs
		}

		p.dbConfigs[c.Database] = &dbConfig{
			name:         c.Database,
			slotName:     cfg.MustString("slot"),
			connCfg:      c,
			codec:        message.NewCodec(cfg.MustString("codec")),
			conn:         nil,
			stopRecvChan: make(chan struct{}, 1),
			stopDone:     make(chan struct{}),
			msgChan:      make(chan message.Interface),
			lsnChan:      make(chan uint64),
			parent:       p,
			logger:       logging.NewLogger(),
		}
	}
}

// dbConfig holds per-database state
type dbConfig struct {
	// values populated from config file
	name     string          // db name and identifier of this config object
	slotName string          // name of replication slot in PG
	connCfg  *pgx.ConnConfig // per-connection configuration
	codec    message.Codec   // message decoder

	// values initialized at object creation
	stopRecvChan chan struct{}          // channel to shutdown pg replication thread
	stopDone     chan struct{}          // channel to indicate full shutdown of pg connector
	msgChan      chan message.Interface // channel on which updates from PG will be forwarded
	lsnChan      chan uint64            // channel on which lsn to be committed should be sent back

	// runtime state
	conn         *pgx.ReplicationConn // actual connection object
	startLsn     uint64               // lsn value from which this instance of replication started
	cancelReadFn func()               // function to cancel the pending read operation
	parent       *PG                  // back-pointer to propagate abort signals
	logger       logging.Logger
}

// start creates the replication connection for this db, and spawns off routines to
// receive updates and commit lsn
func (d *dbConfig) start() error {
	// create replication connection
	if err := d.initReplication(d.connCfg); err != nil {
		return err
	}

	// start a receive thread
	go d.getMessages()
	d.logger.Info("Started PG replication receiver for DB", logging.Fields{
		"name": d.name,
	})

	// start a checkpoint thread
	go d.commitLSN()
	d.logger.Info("Started PG LSN commit thread for DB", logging.Fields{
		"name": d.name,
	})

	return nil
}

// stop attempts to gracefully stop all comm channels and clean up resources
func (d *dbConfig) stop(onError bool) {
	d.logger.Info("Stopping receiver thread for PG DB", logging.Fields{
		"name": d.name,
	})

	// stop the receive thread from any further receive loops
	// NOTE: this has to be a buffered channel, becasue the receive loop can be in one of
	// these two states - doing post read operations (i.e. publish) or within the read
	// operation. For the former, this channel will immediately stop the loop. For the latter
	// case, we should first send a non-blocking message on the channel to stop future loops
	// and then proceed to cancel the read context. If the order of these operations is
	// reversed the loop might not get terminated.
	d.stopRecvChan <- struct{}{}

	// cancel the pending read operation to exit the loop if it is waiting on the server
	if d.cancelReadFn != nil {
		d.cancelReadFn()
	}

	// indicate the publisher that there are no more messages coming
	// closing the message channel indicates the publisher to stop and close the lsn
	// channel
	close(d.msgChan)

	if !onError {
		// wait for the lsn commit thread to terminate, flush pending lsn if any
		// and indicate stop completion on the stopDone channel
		// (note: don't wait on this in error cases)
		<-d.stopDone
	}
}

// errorStop propagates the error signal to parent level PG object
func (d *dbConfig) errorStop() {
	d.parent.errorStop()
}

// initReplication creates a replication connection and starts the streaming replication
func (d *dbConfig) initReplication(connCfg *pgx.ConnConfig) error {
	var err error
	d.conn, err = pgx.ReplicationConnect(*connCfg)
	if err != nil {
		d.logger.Error("Error connecting to DB for replication", logging.Fields{
			"error": err.Error(),
			"name":  d.name,
		})
		return err
	}

	d.startLsn, err = getStartLsn(d.slotName, d.connCfg)
	if err != nil {
		d.logger.Warn("Could not get last checkpointed LSN for DB", logging.Fields{
			"error": err.Error(),
			"name":  d.name,
		})
		d.startLsn = 0
	}

	d.logger.Info("Starting PG replication from LSN for DB", logging.Fields{
		"startLsn": d.startLsn,
		"name":     d.name,
	})

	// start the replication connection. this will cause PG to send a init message with
	// replication slot info. the call succeeds only if the initial message was received
	err = d.conn.StartReplication(d.slotName, d.startLsn, -1)
	if err != nil {
		d.logger.Error("Error starting replication", logging.Fields{
			"error": err.Error(),
			"name":  d.name,
		})
		return err
	}

	return nil
}

// getMessages loops over a receive loop to fetch updates from PG
func (d *dbConfig) getMessages() {
	ctx, cancelFn := context.WithCancel(context.Background())
	d.cancelReadFn = cancelFn

	for {
		select {
		case <-d.stopRecvChan:
			return
		default:
			msg, err := d.conn.WaitForReplicationMessage(ctx)
			if err != nil {
				d.logger.Error("Error receiving replication msg from DB", logging.Fields{
					"error": err.Error(),
					"name":  d.name,
				})

				if !d.conn.IsAlive() {
					d.logger.Error("Connection to DB is dead", logging.Fields{
						"name": d.name,
					})

					// TODO: re-connect and retry
					d.errorStop()
				}
			} else {
				if msg.WalMessage != nil {
					d.logger.Debug("Received update from PG", logging.Fields{
						"walString": string(msg.WalMessage.WalData),
						"name":      d.name,
					})
					d.msgChan <- d.codec.CreateMessage(
						msg.WalMessage.WalData,
						msg.WalMessage.WalStart,
					)
				}

				if msg.ServerHeartbeat != nil {
					d.logger.Debug("Server heartbeat received", logging.Fields{
						"msg":  msg.ServerHeartbeat,
						"name": d.name,
					})
				}
			}
		}
	}
}

// commitLSN loops over the lsn-to-be-committed channel and checkpoints the LSN with
// PG evert HeartBeatIntervalInSeconds seconds
func (d *dbConfig) commitLSN() {
	var lastLsnReceived, lastLsnCommitted uint64
	lastLsnCommitTimestamp := int64(0)

	for {
		select {
		case lsnValue, more := <-d.lsnChan:
			if !more {
				d.logger.Info("Stopping LSN thread for DB", logging.Fields{
					"name": d.name,
				})

				if d.conn != nil {
					// lsn is continuously read from the channel and acknowledged at
					// a specific interval. if a msg n was checkpointed, and there were a few
					// more messages immediately after but within the interval, and then there
					// are no new messages - there won't be any more checkpoints. on shutdown
					// check if there are trailing messages present and checkpoint them so
					// that they are not re-delivered on reconnection
					if lastLsnReceived != lastLsnCommitted {
						d.logger.Info("Flushing trailing LSN for DB", logging.Fields{
							"last_flushed_lsn": lastLsnCommitted,
							"trailing_lsn":     lastLsnReceived,
							"name":             d.name,
						})
						d.flushLSN(lastLsnReceived)
					}
					d.conn.Close()
				}

				d.stopDone <- struct{}{}
				return
			}

			lastLsnReceived = lsnValue
			now := time.Now().Unix()

			if (now - lastLsnCommitTimestamp) > heartbeatIntervalInSeconds {
				d.flushLSN(lsnValue)
				lastLsnCommitted = lsnValue
				lastLsnCommitTimestamp = now
			}

		}

	}
}

func (d *dbConfig) flushLSN(lsnValue uint64) {
	status, err := pgx.NewStandbyStatus(lsnValue)
	if err != nil {
		d.logger.Error("Error creating standby status", logging.Fields{
			"error": err.Error(),
			"lsn":   lsnValue,
			"name":  d.name,
		})
	}

	err = d.conn.SendStandbyStatus(status)
	if err != nil {
		d.logger.Error("Error sending standby status", logging.Fields{
			"error":  err.Error(),
			"status": status,
			"name":   d.name,
		})

		if !d.conn.IsAlive() {
			d.logger.Error("Connection to DB is dead", logging.Fields{
				"name": d.name,
			})
			// TODO: re-connect and retry
			d.errorStop()
		}
	}

	// TODO: change to logger.Debug
	d.logger.Info("Checkpoint LSN", logging.Fields{
		"lsn":  lsnValue,
		"name": d.name,
	})
}

func getStartLsn(slot string, cfg *pgx.ConnConfig) (uint64, error) {
	conn, err := pgx.Connect(*cfg)
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	rows, err := conn.Query(
		fmt.Sprintf(
			"select confirmed_flush_lsn from pg_replication_slots where slot_name='%s'",
			slot,
		))
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	var restartLsn string
	for rows.Next() {
		err = rows.Scan(&restartLsn)
		if err != nil {
			return 0, err
		}
	}

	return pgx.ParseLSN(restartLsn)
}
