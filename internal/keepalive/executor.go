package keepalive

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/kacontext"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	log "github.com/sirupsen/logrus"
)

// Executor sends keepalive requests using the core auth manager.
type Executor struct {
	db      *sql.DB
	manager *coreauth.Manager
	apiKey  string
}

// NewExecutor creates an Executor. manager may be nil initially and set later via SetManager.
func NewExecutor(db *sql.DB, apiKey string) *Executor {
	return &Executor{db: db, apiKey: apiKey}
}

// SetManager injects the auth manager after startup.
func (e *Executor) SetManager(m *coreauth.Manager) {
	e.manager = m
}

// Execute sends one keepalive request per target account in order, with random
// delays of 3-6 seconds between requests. It returns the number successfully sent.
func (e *Executor) Execute(ctx context.Context) int {
	if e.manager == nil {
		log.Warn("keepalive: manager not set, skipping execution")
		return 0
	}

	targets, err := e.getTargetAccounts(ctx)
	if err != nil {
		log.Errorf("keepalive: get targets: %v", err)
		return 0
	}
	if len(targets) == 0 {
		log.Info("keepalive: no targets found")
		return 0
	}

	log.Infof("keepalive: sending %d requests", len(targets))
	sent := 0
	for i, authID := range targets {
		if ctx.Err() != nil {
			break
		}
		if err := e.sendKeepaliveRequest(ctx, authID); err != nil {
			log.Warnf("keepalive: request for auth %s failed: %v", authID, err)
		} else {
			sent++
			log.Infof("keepalive: sent request %d/%d for auth %s", i+1, len(targets), authID)
		}
		if i < len(targets)-1 {
			delay := time.Duration(3000+rand.Intn(3001)) * time.Millisecond
			select {
			case <-ctx.Done():
				return sent
			case <-time.After(delay):
			}
		}
	}
	return sent
}

// getTargetAccounts returns auth IDs for accounts whose quota window has reset
// (next_retry_after <= now) and are still marked unavailable in SQLite.
func (e *Executor) getTargetAccounts(ctx context.Context) ([]string, error) {
	rows, err := e.db.QueryContext(ctx, `
		SELECT auth_id FROM account_states
		WHERE unavailable = 1
		  AND next_retry_after IS NOT NULL
		  AND next_retry_after <= datetime('now')
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		if id == "" {
			continue
		}
		// Confirm the auth still exists in memory
		if _, ok := e.manager.GetByID(id); !ok {
			continue
		}
		ids = append(ids, id)
	}
	return ids, nil
}

// sendKeepaliveRequest fires a minimal (max_tokens=1) chat request pinned to the given auth.
func (e *Executor) sendKeepaliveRequest(ctx context.Context, authID string) error {
	kaCtx := kacontext.WithKeepaliveContext(ctx)

	payload := map[string]any{
		"model": "gpt-4o-mini",
		"messages": []map[string]string{
			{"role": "user", "content": "hi"},
		},
		"max_tokens": 1,
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}

	opts := cliproxyexecutor.Options{
		Metadata: map[string]any{
			cliproxyexecutor.PinnedAuthMetadataKey: authID,
		},
	}

	req := cliproxyexecutor.Request{
		Model:   "gpt-4o-mini",
		Payload: payloadBytes,
	}

	// Resolve provider from manager
	provider := "codex"
	if a, ok := e.manager.GetByID(authID); ok && a != nil {
		provider = strings.ToLower(strings.TrimSpace(a.Provider))
		if provider == "" {
			provider = "codex"
		}
	}

	_, err = e.manager.Execute(kaCtx, []string{provider}, req, opts)
	return err
}
