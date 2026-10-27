package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/nikrybak/edge-gateway/internal/config"
)

// Listen opens the public and admin listeners configured in sc.
func Listen(sc config.Server) (public, admin net.Listener, err error) {
	public, err = net.Listen("tcp", sc.Listen)
	if err != nil {
		return nil, nil, fmt.Errorf("listen %s: %w", sc.Listen, err)
	}
	admin, err = net.Listen("tcp", sc.AdminListen)
	if err != nil {
		_ = public.Close()
		return nil, nil, fmt.Errorf("listen %s: %w", sc.AdminListen, err)
	}
	return public, admin, nil
}

// Serve runs the gateway on the given listeners until ctx is canceled, then shuts down
// gracefully: /readyz turns unready, the gateway keeps serving for DrainDelay so load
// balancers can react, and in-flight requests get up to ShutdownTimeout to finish.
func Serve(ctx context.Context, gw *Gateway, sc config.Server, public, admin net.Listener, logger *slog.Logger) error {
	errLog := slog.NewLogLogger(logger.Handler(), slog.LevelWarn)
	publicSrv := &http.Server{
		Handler:           gw.Handler(),
		ReadHeaderTimeout: sc.ReadHeaderTimeout,
		IdleTimeout:       sc.IdleTimeout,
		ErrorLog:          errLog,
	}
	adminSrv := &http.Server{
		Handler:           gw.AdminHandler(),
		ReadHeaderTimeout: sc.ReadHeaderTimeout,
		ErrorLog:          errLog,
	}

	serveErr := make(chan error, 2)
	serve := func(name string, srv *http.Server, ln net.Listener) {
		logger.Info("listening", "listener", name, "addr", ln.Addr().String())
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr <- fmt.Errorf("%s listener: %w", name, err)
			return
		}
		serveErr <- nil
	}
	go serve("public", publicSrv, public)
	go serve("admin", adminSrv, admin)

	var runErr error
	select {
	case <-ctx.Done():
	case runErr = <-serveErr:
		// A listener died; shut the other one down too rather than run half a gateway.
	}

	logger.Info("shutting down", "drain_delay", sc.DrainDelay, "timeout", sc.ShutdownTimeout)
	gw.SetDraining()
	if runErr == nil && sc.DrainDelay > 0 {
		time.Sleep(sc.DrainDelay)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), sc.ShutdownTimeout)
	defer cancel()
	// Admin goes last so /readyz and /metrics stay observable while traffic drains.
	if err := publicSrv.Shutdown(shutdownCtx); err != nil {
		runErr = errors.Join(runErr, fmt.Errorf("drain public listener: %w", err))
	}
	if err := adminSrv.Shutdown(shutdownCtx); err != nil {
		runErr = errors.Join(runErr, fmt.Errorf("drain admin listener: %w", err))
	}
	gw.Close()
	logger.Info("shutdown complete")
	return runErr
}
