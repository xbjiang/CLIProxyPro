// Package cmd provides command-line interface functionality for the CLI Proxy API server.
// It includes authentication flows for various AI service providers, service startup,
// and other command-line operations.
package cmd

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/api"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/keepalive"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/persistence"
	internalusage "github.com/router-for-me/CLIProxyAPI/v6/internal/usage"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	coreusage "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
	log "github.com/sirupsen/logrus"
)

// defaultDataDir returns ~/.cli-proxy-api (same as the default auth dir).
func defaultDataDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ".cli-proxy-api"
	}
	return filepath.Join(home, ".cli-proxy-api")
}

// StartService builds and runs the proxy service using the exported SDK.
func StartService(cfg *config.Config, configPath string, localPassword string) {
	dataDir := defaultDataDir()

	// --- Persistence setup ---
	var db *sql.DB
	db, err := persistence.Open(dataDir)
	if err != nil {
		log.Warnf("persistence: failed to open db: %v (running without persistence)", err)
		db = nil
	}

	// Usage plugin: persist every request record to SQLite
	if db != nil {
		coreusage.RegisterPlugin(persistence.NewSQLitePersistencePlugin(db))
	}

	// Keepalive scheduler
	apiKey := os.Getenv("CLIPROXY_API_KEY")
	if apiKey == "" {
		apiKey = "demo-key-for-local-testing"
	}
	var sched *keepalive.Scheduler
	if db != nil {
		sched = keepalive.NewScheduler(db, apiKey)
	}

	// Persistence hook (implements coreauth.Hook)
	var hook *persistence.PersistenceHook
	if db != nil {
		hook = persistence.NewPersistenceHook(db, sched)
	}

	builder := cliproxy.NewBuilder().
		WithConfig(cfg).
		WithConfigPath(configPath).
		WithLocalManagementPassword(localPassword)

	if hook != nil {
		builder = builder.WithCoreAuthHook(hook)
	}

	// Capture closures for OnAfterStart
	capturedDB := db
	capturedSched := sched
	capturedHook := hook

	builder = builder.WithHooks(cliproxy.Hooks{
		OnAfterStart: func(svc *cliproxy.Service) {
			mgr := svc.CoreManager()

			// Inject manager into hook and scheduler
			if capturedHook != nil && mgr != nil {
				capturedHook.SetManager(mgr)
			}
			if capturedSched != nil && mgr != nil {
				capturedSched.SetManager(mgr)
			}

			// Restore persisted account states into memory
			if capturedDB != nil && mgr != nil {
				restoreAccountStates(capturedDB, mgr)
			}

			// Seed in-memory usage statistics from SQLite so /v0/management/usage
			// reflects historical data immediately after restart
			if capturedDB != nil {
				if err := persistence.SeedUsageStats(capturedDB, internalusage.GetRequestStatistics()); err != nil {
					log.Warnf("persistence: seed usage stats: %v", err)
				}
			}

			// Wire up management handler
			if capturedDB != nil {
				svc.SetPersistenceDB(capturedDB)
			}
			if capturedSched != nil {
				svc.SetKeepaliveScheduler(capturedSched)
			}

			// Start compensation + normal scheduling
			if capturedSched != nil {
				go capturedSched.StartWithCompensation(context.Background())
			}

			// Start cleanup task (daily, 30-day retention)
			if capturedDB != nil {
				persistence.StartCleanupTask(capturedDB, 30*24*time.Hour)
			}

			log.Info("persistence: keepalive and persistence initialised")
		},
	})

	ctxSignal, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	runCtx := ctxSignal
	if localPassword != "" {
		var keepAliveCancel context.CancelFunc
		runCtx, keepAliveCancel = context.WithCancel(ctxSignal)
		builder = builder.WithServerOptions(api.WithKeepAliveEndpoint(10*time.Second, func() {
			log.Warn("keep-alive endpoint idle for 10s, shutting down")
			keepAliveCancel()
		}))
	}

	service, err := builder.Build()
	if err != nil {
		log.Errorf("failed to build proxy service: %v", err)
		return
	}

	err = service.Run(runCtx)
	if err != nil && !errors.Is(err, context.Canceled) {
		log.Errorf("proxy service exited with error: %v", err)
	}
}

// restoreAccountStates reads persisted next_retry_after values from SQLite and
// injects them back into in-memory Auth entries (only when the auth's value is zero).
func restoreAccountStates(db *sql.DB, mgr *coreauth.Manager) {
	states, err := persistence.LoadAccountStates(db)
	if err != nil {
		log.Warnf("persistence: load account states: %v", err)
		return
	}
	now := time.Now()
	restored := 0
	for _, state := range states {
		if state.NextRetryAfter == nil || state.NextRetryAfter.Before(now) {
			continue // already expired or not set
		}
		auth, ok := mgr.GetByID(state.AuthID)
		if !ok || auth == nil {
			continue
		}
		if !auth.NextRetryAfter.IsZero() {
			continue // live value already present, don't overwrite
		}
		// Inject the persisted retry time
		authCopy := auth.Clone()
		authCopy.NextRetryAfter = *state.NextRetryAfter
		authCopy.Unavailable = true
		ctx := coreauth.WithSkipPersist(context.Background())
		if _, updateErr := mgr.Update(ctx, authCopy); updateErr != nil {
			log.Debugf("persistence: restore auth %s: %v", state.AuthID, updateErr)
		} else {
			restored++
		}
	}
	if restored > 0 {
		log.Infof("persistence: restored next_retry_after for %d accounts", restored)
	}
}

// StartServiceBackground starts the proxy service in a background goroutine
// and returns a cancel function for shutdown and a done channel.
func StartServiceBackground(cfg *config.Config, configPath string, localPassword string) (cancel func(), done <-chan struct{}) {
	builder := cliproxy.NewBuilder().
		WithConfig(cfg).
		WithConfigPath(configPath).
		WithLocalManagementPassword(localPassword)

	ctx, cancelFn := context.WithCancel(context.Background())
	doneCh := make(chan struct{})

	service, err := builder.Build()
	if err != nil {
		log.Errorf("failed to build proxy service: %v", err)
		close(doneCh)
		return cancelFn, doneCh
	}

	go func() {
		defer close(doneCh)
		if err := service.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Errorf("proxy service exited with error: %v", err)
		}
	}()

	return cancelFn, doneCh
}

// WaitForCloudDeploy waits indefinitely for shutdown signals in cloud deploy mode
// when no configuration file is available.
func WaitForCloudDeploy() {
	log.Info("Cloud deploy mode: No config found; standing by for configuration. API server is not started. Press Ctrl+C to exit.")

	ctxSignal, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	<-ctxSignal.Done()
	log.Info("Cloud deploy mode: Shutdown signal received; exiting")
}
