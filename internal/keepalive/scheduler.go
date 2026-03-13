package keepalive

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/persistence"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	log "github.com/sirupsen/logrus"
)

const (
	batchWindowDuration = 30 * time.Minute
	fireDelayAfterBatch = 5 * time.Minute
	compensationDelay   = 5 * time.Minute
	compensationWindow  = 2 * time.Hour
)

// Scheduler computes keepalive batch fire times and executes them automatically.
type Scheduler struct {
	mu         sync.Mutex
	db         *sql.DB
	executor   *Executor
	nextFireAt time.Time
	timer      *time.Timer
	running    bool
}

// NewScheduler creates a new scheduler backed by db.
func NewScheduler(db *sql.DB, apiKey string) *Scheduler {
	return &Scheduler{
		db:       db,
		executor: NewExecutor(db, apiKey),
	}
}

// SetManager injects the auth manager (called after service start).
func (s *Scheduler) SetManager(m *coreauth.Manager) {
	s.executor.SetManager(m)
}

// NextFireAt returns the currently scheduled fire time (zero if not scheduled).
func (s *Scheduler) NextFireAt() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.nextFireAt
}

// IsRunning reports whether a keepalive execution is in progress.
func (s *Scheduler) IsRunning() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.running
}

// IsScheduled reports whether a future fire time is set.
func (s *Scheduler) IsScheduled() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return !s.nextFireAt.IsZero()
}

// Reschedule recomputes the next batch fire time from current account states.
// It is idempotent: if the new fire time is within 1s of the current one it is a no-op.
// If a sooner fire time is found the existing timer is replaced.
func (s *Scheduler) Reschedule(ctx context.Context) {
	fireAt, err := s.computeNextBatch(ctx)
	if err != nil {
		log.Debugf("keepalive: compute batch: %v", err)
		return
	}
	if fireAt.IsZero() {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.nextFireAt.IsZero() {
		diff := s.nextFireAt.Sub(fireAt)
		if diff < 0 {
			diff = -diff
		}
		if diff < time.Second {
			return // same fire time, skip
		}
		// Keep the sooner one
		if fireAt.After(s.nextFireAt) {
			return
		}
	}

	s.scheduleAt(ctx, fireAt)
}

// scheduleAt sets or replaces the timer to fire at fireAt (must hold s.mu).
func (s *Scheduler) scheduleAt(ctx context.Context, fireAt time.Time) {
	if s.timer != nil {
		s.timer.Stop()
	}
	s.nextFireAt = fireAt
	delay := time.Until(fireAt)
	if delay < 0 {
		delay = 0
	}
	log.Infof("keepalive: scheduled at %s (in %s)", fireAt.Local().Format(time.RFC3339), delay.Round(time.Second))
	s.timer = time.AfterFunc(delay, func() {
		s.fire(ctx)
	})
}

// fire executes keepalive and clears scheduling state.
func (s *Scheduler) fire(ctx context.Context) {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		log.Warn("keepalive: fire called while already running, skipping")
		return
	}
	s.running = true
	s.nextFireAt = time.Time{}
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		s.running = false
		s.mu.Unlock()
	}()

	log.Info("keepalive: starting execution")
	sent := s.executor.Execute(ctx)
	log.Infof("keepalive: execution complete, sent=%d", sent)

	_ = persistence.SetSetting(s.db, "last_keepalive_at", time.Now().UTC().Format(time.RFC3339))
}

// StartWithCompensation checks for accounts that reset while the service was
// down (within the last compensationWindow) and schedules a catch-up execution
// after compensationDelay if any are found.
func (s *Scheduler) StartWithCompensation(ctx context.Context) {
	lastKA, _ := s.getLastKeepaliveAt()

	rows, err := s.db.QueryContext(ctx, `
		SELECT next_retry_after FROM account_states
		WHERE unavailable = 1
		  AND next_retry_after IS NOT NULL
		  AND next_retry_after > datetime('now', ? || ' hours')
		  AND next_retry_after <= datetime('now')`,
		fmt.Sprintf("-%d", int(compensationWindow.Hours())),
	)
	if err != nil {
		log.Debugf("keepalive: compensation query: %v", err)
		s.Reschedule(ctx)
		return
	}
	defer rows.Close()

	needComp := false
	for rows.Next() {
		var nra string
		if err := rows.Scan(&nra); err != nil {
			continue
		}
		t, err := time.Parse(time.RFC3339, nra)
		if err != nil {
			continue
		}
		// Already covered by a keepalive after the reset?
		if !lastKA.IsZero() && lastKA.After(t) {
			continue
		}
		needComp = true
		break
	}
	_ = rows.Err()

	if needComp {
		fireAt := time.Now().Add(compensationDelay)
		log.Infof("keepalive: compensation scheduled at %s", fireAt.Local().Format(time.RFC3339))
		s.mu.Lock()
		s.scheduleAt(ctx, fireAt)
		s.mu.Unlock()
	}

	// Also schedule normally for future resets
	s.Reschedule(ctx)
}

func (s *Scheduler) getLastKeepaliveAt() (time.Time, error) {
	v, err := persistence.GetSetting(s.db, "last_keepalive_at")
	if err != nil || v == "" {
		return time.Time{}, err
	}
	return time.Parse(time.RFC3339, v)
}

// computeNextBatch computes the next batch fire time from future next_retry_after values.
// Algorithm: sort future reset times ascending, group into 30-min windows,
// fireAt = last time in first batch + 5 min.
func (s *Scheduler) computeNextBatch(ctx context.Context) (time.Time, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT next_retry_after FROM account_states
		WHERE unavailable = 1
		  AND next_retry_after IS NOT NULL
		  AND next_retry_after > datetime('now')
		ORDER BY next_retry_after ASC
	`)
	if err != nil {
		return time.Time{}, err
	}
	defer rows.Close()

	var times []time.Time
	for rows.Next() {
		var ts string
		if err := rows.Scan(&ts); err != nil {
			continue
		}
		t, err := time.Parse(time.RFC3339, ts)
		if err != nil {
			continue
		}
		times = append(times, t)
	}

	if len(times) == 0 {
		return time.Time{}, nil
	}

	// Group into first 30-min window
	batchStart := times[0]
	batchEnd := batchStart
	for _, t := range times {
		if t.Sub(batchStart) <= batchWindowDuration {
			if t.After(batchEnd) {
				batchEnd = t
			}
		} else {
			break
		}
	}

	return batchEnd.Add(fireDelayAfterBatch), nil
}
