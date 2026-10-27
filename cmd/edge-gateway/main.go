// Command edge-gateway runs the HTTP API gateway.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nikrybak/edge-gateway/internal/config"
	"github.com/nikrybak/edge-gateway/internal/observability"
	"github.com/nikrybak/edge-gateway/internal/server"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "edge-gateway:", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		configPath = flag.String("config", "configs/example.yaml", "path to the YAML config")
		watch      = flag.Duration("watch", 0, "poll the config file for changes at this interval (0 disables; SIGHUP always reloads)")
		logLevel   = flag.String("log-level", "info", "log level: debug, info, warn, error")
		validate   = flag.Bool("validate", false, "validate the config and exit")
	)
	flag.Parse()

	var level slog.Level
	if err := level.UnmarshalText([]byte(*logLevel)); err != nil {
		return fmt.Errorf("log level: %w", err)
	}
	logger := observability.NewLogger(os.Stdout, level)

	cfg, err := config.Load(*configPath)
	if err != nil {
		return err
	}
	if *validate {
		fmt.Println("config OK")
		return nil
	}
	watcher := config.NewWatcher(*configPath, max(*watch, time.Second))

	gw, err := server.New(cfg, server.Options{Logger: logger})
	if err != nil {
		return err
	}
	public, admin, err := server.Listen(cfg.Server)
	if err != nil {
		gw.Close()
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	reload := func(next *config.Config) {
		if err := gw.Reload(next); err != nil {
			logger.Error("config reload failed, keeping current config", "error", err)
		}
	}
	reloadErr := func(err error) {
		logger.Error("config reload failed, keeping current config", "error", err)
	}

	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)
	defer signal.Stop(hup)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-hup:
				next, err := config.Load(*configPath)
				if err != nil {
					reloadErr(err)
					continue
				}
				reload(next)
			}
		}
	}()
	if *watch > 0 {
		go watcher.Run(ctx, reload, reloadErr)
	}

	err = server.Serve(ctx, gw, cfg.Server, public, admin, logger)
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}
