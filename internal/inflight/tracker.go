// Package inflight tracks in-flight upstream HTTP requests.
// It exposes a global Tracker so executors can register pending upstream calls
// and management handlers can list or cancel them.
package inflight

import (
	"context"
	"sync"
	"time"
)

// Entry represents one in-flight upstream request.
type Entry struct {
	ID        string
	Provider  string
	AuthID    string
	Source    string // base URL of the relay
	StartedAt time.Time
	cancel    context.CancelFunc
}

// DurationSec returns how many seconds this entry has been in-flight.
func (e *Entry) DurationSec() float64 {
	return time.Since(e.StartedAt).Seconds()
}

// Tracker is a thread-safe registry of in-flight upstream requests.
type Tracker struct {
	mu      sync.RWMutex
	entries map[string]*Entry
}

// Global is the process-wide tracker used by executors and management handlers.
var Global = &Tracker{
	entries: make(map[string]*Entry),
}

// Register adds an entry and returns it. Call the returned cancel to abort
// the upstream HTTP call without cancelling the parent request context.
func (t *Tracker) Register(id, provider, authID, source string, cancel context.CancelFunc) *Entry {
	e := &Entry{
		ID:        id,
		Provider:  provider,
		AuthID:    authID,
		Source:    source,
		StartedAt: time.Now(),
		cancel:    cancel,
	}
	t.mu.Lock()
	t.entries[id] = e
	t.mu.Unlock()
	return e
}

// Deregister removes an entry. Safe to call even if the entry was already removed.
func (t *Tracker) Deregister(id string) {
	t.mu.Lock()
	delete(t.entries, id)
	t.mu.Unlock()
}

// Cancel cancels the upstream HTTP call for the given ID and removes the entry.
// Returns false if the ID was not found.
func (t *Tracker) Cancel(id string) bool {
	t.mu.Lock()
	e, ok := t.entries[id]
	if ok {
		delete(t.entries, id)
	}
	t.mu.Unlock()
	if ok && e.cancel != nil {
		e.cancel()
	}
	return ok
}

// List returns a snapshot of all current entries.
func (t *Tracker) List() []*Entry {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make([]*Entry, 0, len(t.entries))
	for _, e := range t.entries {
		out = append(out, e)
	}
	return out
}
