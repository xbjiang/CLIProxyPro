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
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
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
	now := time.Now()
	for i, authID := range targets {
		if ctx.Err() != nil {
			break
		}
		if err := e.sendKeepaliveRequest(ctx, authID); err != nil {
			log.Warnf("keepalive: request for auth %s failed: %v", authID, err)
		} else {
			sent++
			log.Infof("keepalive: sent request %d/%d for auth %s", i+1, len(targets), authID)
			// Record the send time in database
			if e.db != nil {
				if err := e.recordKeepaliveSentAt(authID, now); err != nil {
					log.Warnf("keepalive: failed to record sent_at for %s: %v", authID, err)
				}
			}
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
// (next_retry_after <= now) and haven't been keepalived since the reset.
// We check last_keepalive_sent_at < next_retry_after to avoid re-firing for
// accounts already keepalived in a previous service run.
func (e *Executor) getTargetAccounts(ctx context.Context) ([]string, error) {
	rows, err := e.db.QueryContext(ctx, `
		SELECT auth_id FROM account_states
		WHERE next_retry_after IS NOT NULL
		  AND datetime(next_retry_after) <= datetime('now')
		  AND (last_keepalive_sent_at IS NULL
		       OR datetime(last_keepalive_sent_at) < datetime(next_retry_after))
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
		"model": "gpt-5.1-codex-mini",
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
		SourceFormat: sdktranslator.FromString("openai"),
		Metadata: map[string]any{
			cliproxyexecutor.PinnedAuthMetadataKey: authID,
		},
	}

	req := cliproxyexecutor.Request{
		Model:   "gpt-5.1-codex-mini",
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

// KeepaliveResult holds the result of a single keepalive attempt.
type KeepaliveResult struct {
	AuthID  string `json:"auth_id"`
	Email   string `json:"email,omitempty"`
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
}

// ExecuteForAuthIDs sends keepalive requests to specific accounts identified by auth_id,
// with random delays of 3-6 seconds between requests. Returns per-account results.
func (e *Executor) ExecuteForAuthIDs(ctx context.Context, authIDs []string) []KeepaliveResult {
	if e.manager == nil {
		log.Warn("keepalive: manager not set, skipping targeted execution")
		results := make([]KeepaliveResult, len(authIDs))
		for i, id := range authIDs {
			results[i] = KeepaliveResult{AuthID: id, Success: false, Error: "manager not set"}
		}
		return results
	}

	log.Infof("keepalive: targeted execution for %d accounts", len(authIDs))
	results := make([]KeepaliveResult, 0, len(authIDs))
	now := time.Now()

	for i, authID := range authIDs {
		if ctx.Err() != nil {
			break
		}

		r := KeepaliveResult{AuthID: authID}
		// Resolve email for response
		if a, ok := e.manager.GetByID(authID); ok && a != nil {
			r.Email = a.Label
		}

		if err := e.sendKeepaliveRequest(ctx, authID); err != nil {
			r.Success = false
			r.Error = err.Error()
			log.Warnf("keepalive: targeted request for %s failed: %v", authID, err)
		} else {
			r.Success = true
			log.Infof("keepalive: targeted request %d/%d for %s succeeded", i+1, len(authIDs), authID)
			if e.db != nil {
				if err := e.recordKeepaliveSentAt(authID, now); err != nil {
					log.Warnf("keepalive: failed to record sent_at for %s: %v", authID, err)
				}
			}
		}
		results = append(results, r)

		if i < len(authIDs)-1 {
			delay := time.Duration(3000+rand.Intn(3001)) * time.Millisecond
			select {
			case <-ctx.Done():
				return results
			case <-time.After(delay):
			}
		}
	}
	return results
}

// recordKeepaliveSentAt updates the last_keepalive_sent_at timestamp for the given auth.
func (e *Executor) recordKeepaliveSentAt(authID string, sentAt time.Time) error {
	_, err := e.db.Exec(`
		UPDATE account_states
		SET last_keepalive_sent_at = ?
		WHERE auth_id = ?
	`, sentAt.UTC().Format(time.RFC3339), authID)
	return err
}
