package util

import (
	"context"
	"errors"
	"fmt"
	"github.com/rs/zerolog/log"
	"os"
	"os/signal"
	"syscall"
)

type Closer interface {
	Close()
}

func WaitCloseSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, os.Kill, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	<-signals
}

func WaitCloseSignalsAndRelease(closer Closer) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, os.Kill, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	<-signals
	closer.Close()
}

func WaitSignals(ctx context.Context, f func()) error {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(
		sigChan,
		syscall.SIGINT,
		syscall.SIGTERM,
	)

	select {
	case <-ctx.Done():
		return nil
	case s := <-sigChan:
		log.Info().Msg(fmt.Sprintf("get a SIGNAL:%v", s))
		switch s {

		case syscall.SIGINT:
			if f != nil {
				f()
			}
			return errors.New("Killed!")
		default:
			if f != nil {
				f()
			}
			return errors.New("Killed!")
		}
	}
}
