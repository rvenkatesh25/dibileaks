package app

import (
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/thumbtack/go/lib/alfred/logging"

	"github.com/thumbtack/go/dibileaks/publisher"
	"github.com/thumbtack/go/dibileaks/whistleblowers"
)

const (
	maxConcurrentAbortSignals = 3
)

// App is the generic interface extended by this application
type App interface {
	Run()
}

// app encapsulates the data needed to maintain state for dibileaks client
type app struct {
	logger          logging.Logger
	pgWhistleBlower *whistleblowers.PG
	publisher       publisher.Interface
	signalChan      chan os.Signal

	stopping bool
	stopMux  sync.Mutex
}

// NewApp instantiates the app with PG whistle blowers and a configured publisher
func NewApp() App {
	signalChan := make(chan os.Signal, maxConcurrentAbortSignals)
	return &app{
		logger:          logging.NewLogger(),
		pgWhistleBlower: whistleblowers.NewPostgresConfig(signalChan),
		publisher:       publisher.New(signalChan, "file"),
		signalChan:      signalChan,
		stopping:        false,
		stopMux:         sync.Mutex{},
	}
}

// Run starts the main thread of the app
func (a *app) Run() {
	a.logger.Info("Starting Dibileaks Client!", logging.Fields{})
	commChannels, err := a.pgWhistleBlower.Start()
	if err != nil {
		a.logger.Error("Error starting PG whistle blower", logging.Fields{
			"error": err.Error(),
		})
		a.stopGracefully(true)
		return
	}

	err = a.publisher.Start(commChannels)
	if err != nil {
		a.logger.Error("Error starting publisher", logging.Fields{
			"error": err.Error(),
		})
		a.stopGracefully(true)
		return
	}

	// Listen for interrupt signals
	signal.Notify(a.signalChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGABRT)
	recdSig := <-a.signalChan

	a.stopGracefully(recdSig == syscall.SIGABRT)
}

// stopGracefully shuts down all spawned goroutines and cleans up resources
func (a *app) stopGracefully(onError bool) {

	a.stopMux.Lock()
	if !a.stopping {
		a.stopping = true
		a.logger.Info("Received shutdown signal. Beginning graceful stop", logging.Fields{})
		a.pgWhistleBlower.Stop(onError)
	}
	a.stopMux.Unlock()

	if onError {
		os.Exit(1)
	}
}
