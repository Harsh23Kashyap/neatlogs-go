package neatlogs

import (
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

const gracefulSignalTimeout = 30 * time.Second

// shutdownSignalController temporarily intercepts process termination so the
// SDK can flush, then restores and re-delivers the signal. Stop is idempotent
// and safe from the signal goroutine or a normal application shutdown.
type shutdownSignalController struct {
	signals  chan os.Signal
	done     chan struct{}
	stopOnce sync.Once
}

func newShutdownSignalController() *shutdownSignalController {
	return &shutdownSignalController{
		signals: make(chan os.Signal, 1),
		done:    make(chan struct{}),
	}
}

func (c *shutdownSignalController) Start(shutdown func(os.Signal)) {
	signal.Notify(c.signals, os.Interrupt, syscall.SIGTERM)
	go c.run(shutdown, redeliverCurrentProcessSignal)
}

func (c *shutdownSignalController) run(shutdown func(os.Signal), redeliver func(os.Signal)) {
	select {
	case sig := <-c.signals:
		// Restore the default before flushing so a second signal can still force
		// termination, and so re-delivery below retains normal exit semantics.
		c.Stop()
		shutdown(sig)
		redeliver(sig)
	case <-c.done:
	}
}

func (c *shutdownSignalController) Stop() {
	c.stopOnce.Do(func() {
		signal.Stop(c.signals)
		close(c.done)
	})
}

func signalTerminationReason(sig os.Signal) string {
	switch sig {
	case os.Interrupt:
		return "SIGINT"
	case syscall.SIGTERM:
		return "SIGTERM"
	default:
		return sig.String()
	}
}

func redeliverCurrentProcessSignal(sig os.Signal) {
	process, err := os.FindProcess(os.Getpid())
	if err == nil {
		err = process.Signal(sig)
	}
	if err != nil {
		code := 1
		if sig == os.Interrupt {
			code = 130
		} else if sig == syscall.SIGTERM {
			code = 143
		}
		os.Exit(code)
	}
}
