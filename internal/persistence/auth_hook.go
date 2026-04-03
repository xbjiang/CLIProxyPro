package persistence

import (
	"context"
	"database/sql"
	"sync"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/kacontext"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	log "github.com/sirupsen/logrus"
)

// Rescheduler is satisfied by keepalive.Scheduler.
type Rescheduler interface {
	Reschedule(ctx context.Context)
}

// PersistenceHook implements coreauth.Hook, persisting auth state changes
// to SQLite and triggering keepalive rescheduling.
type PersistenceHook struct {
	coreauth.NoopHook
	db        *sql.DB
	scheduler Rescheduler
	managerMu sync.RWMutex
	manager   *coreauth.Manager
}

// NewPersistenceHook creates a hook backed by db and the given scheduler.
// Call SetManager after the auth manager is created.
func NewPersistenceHook(db *sql.DB, scheduler Rescheduler) *PersistenceHook {
	return &PersistenceHook{db: db, scheduler: scheduler}
}

// SetManager injects the auth manager reference (called after service start).
func (h *PersistenceHook) SetManager(m *coreauth.Manager) {
	h.managerMu.Lock()
	h.manager = m
	h.managerMu.Unlock()
}

// OnAuthRegistered implements coreauth.Hook.
func (h *PersistenceHook) OnAuthRegistered(ctx context.Context, auth *coreauth.Auth) {
	h.upsert([]*coreauth.Auth{auth})
}

// OnAuthUpdated implements coreauth.Hook.
func (h *PersistenceHook) OnAuthUpdated(ctx context.Context, auth *coreauth.Auth) {
	h.upsert([]*coreauth.Auth{auth})
	if h.scheduler != nil {
		h.scheduler.Reschedule(ctx)
	}
}

// OnResult implements coreauth.Hook.
// It fetches the latest auth snapshot from the manager (which has already applied
// MarkResult state) and persists it, then triggers rescheduling.
func (h *PersistenceHook) OnResult(ctx context.Context, result coreauth.Result) {
	// For successful keepalive requests, record last_keepalive_sent_at BEFORE
	// the upsert so the next_retry_after CASE logic can correctly detect that
	// a keepalive was sent and clear the stale value.
	if kacontext.IsKeepaliveContext(ctx) && result.Success && h.db != nil {
		_ = UpdateLastKeepaliveSentAt(h.db, result.AuthID, time.Now())
	}

	h.managerMu.RLock()
	mgr := h.manager
	h.managerMu.RUnlock()

	if mgr != nil {
		if auth, ok := mgr.GetByID(result.AuthID); ok {
			h.upsert([]*coreauth.Auth{auth})
		}
	}

	// Don't re-trigger scheduling for keepalive requests to avoid recursion
	if !kacontext.IsKeepaliveContext(ctx) && h.scheduler != nil {
		h.scheduler.Reschedule(ctx)
	}
}

func (h *PersistenceHook) upsert(auths []*coreauth.Auth) {
	if h.db == nil {
		return
	}
	if err := UpsertAccountStates(h.db, auths); err != nil {
		log.Debugf("persistence hook: upsert: %v", err)
	}
}
