package management

import (
	"context"
	"database/sql"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/keepalive"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/persistence"
	log "github.com/sirupsen/logrus"
)

// GetUsageMerged returns aggregated statistics from SQLite.
// GET /v0/management/usage/merged
func (h *Handler) GetUsageMerged(c *gin.Context) {
	db := h.getPersistenceDB()
	if db == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "persistence not available"})
		return
	}
	stats, err := persistence.QueryAggregated(c.Request.Context(), db)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, stats)
}

// GetUsageDaily returns per-day statistics from SQLite.
// GET /v0/management/usage/daily?days=N
func (h *Handler) GetUsageDaily(c *gin.Context) {
	db := h.getPersistenceDB()
	if db == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "persistence not available"})
		return
	}
	days := 30
	if v := c.Query("days"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 365 {
			days = n
		}
	}
	rows, err := persistence.QueryDaily(c.Request.Context(), db, days)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"days": rows})
}

// GetKeepaliveStatus returns the current keepalive scheduler state.
// GET /v0/management/keepalive/status
func (h *Handler) GetKeepaliveStatus(c *gin.Context) {
	sched := h.getKeepaliveScheduler()

	db := h.getPersistenceDB()
	lastKA := ""
	if db != nil {
		if v, err := persistence.GetSetting(db, "last_keepalive_at"); err == nil {
			lastKA = v
		}
	}

	resp := gin.H{
		"running":           false,
		"scheduled":         false,
		"next_fire_at":      nil,
		"last_keepalive_at": lastKA,
	}

	if sched != nil {
		resp["running"] = sched.IsRunning()
		resp["scheduled"] = sched.IsScheduled()
		if fireAt := sched.NextFireAt(); !fireAt.IsZero() {
			resp["next_fire_at"] = fireAt.UTC().Format(time.RFC3339)
		}
	}

	c.JSON(http.StatusOK, resp)
}

// GetUsageByDateRange returns aggregated stats and logs for a specific date range.
// GET /v0/management/usage/range?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD
func (h *Handler) GetUsageByDateRange(c *gin.Context) {
	db := h.getPersistenceDB()
	if db == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "persistence not available"})
		return
	}

	startDate := c.Query("start_date")
	endDate := c.Query("end_date")

	if startDate == "" || endDate == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "start_date and end_date are required (format: YYYY-MM-DD)"})
		return
	}

	stats, logs, err := persistence.QueryByDateRange(c.Request.Context(), db, startDate, endDate)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"stats": stats,
		"logs":  logs,
	})
}

// GetUsagePerAccountCycles returns per-account usage within each account's current quota cycle.
// GET /v0/management/usage/per-account-cycles
func (h *Handler) GetUsagePerAccountCycles(c *gin.Context) {
	db := h.getPersistenceDB()
	if db == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "persistence not available"})
		return
	}
	// Query all records (including ghost credentials from replaced auth files).
	// Deduplication by email is done in Go so that accounts which recently
	// re-authenticated show their last meaningful cycle even if the new
	// credential's current cycle has 0 tokens.
	all, err := persistence.QueryPerAccountCycles(c.Request.Context(), db, nil)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if h.authManager == nil {
		c.JSON(http.StatusOK, gin.H{"accounts": all})
		return
	}

	// Build set of currently active emails.
	activeEmails := make(map[string]bool)
	for _, a := range h.authManager.List() {
		if email := authEmail(a); email != "" {
			activeEmails[email] = true
		}
	}

	// Per email and window_type, keep the row with the highest total_tokens (ties broken by latest cycle_end).
	type bestEntry struct {
		row persistence.AccountCycleStat
	}
	byEmail := make(map[string]bestEntry)
	for _, row := range all {
		if row.Email == "" {
			continue
		}
		key := row.Email + "|" + row.WindowType
		cur, exists := byEmail[key]
		if !exists || row.TotalTokens > cur.row.TotalTokens ||
			(row.TotalTokens == cur.row.TotalTokens && row.CycleEnd > cur.row.CycleEnd) {
			byEmail[key] = bestEntry{row: row}
		}
	}

	// Collect results for active emails only, sorted by total_tokens desc.
	result := make([]persistence.AccountCycleStat, 0)
	for key, b := range byEmail {
		email := strings.Split(key, "|")[0]
		if activeEmails[email] {
			result = append(result, b.row)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].TotalTokens > result[j].TotalTokens
	})
	c.JSON(http.StatusOK, gin.H{"accounts": result})
}

// GetAccountCycleHistory returns multi-cycle usage history per account.
// GET /v0/management/usage/account-cycle-history?max_cycles=10
func (h *Handler) GetAccountCycleHistory(c *gin.Context) {
	db := h.getPersistenceDB()
	if db == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "persistence not available"})
		return
	}
	maxCycles := 10
	if v := c.Query("max_cycles"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 52 {
			maxCycles = n
		}
	}
	activeIndexes := h.activeAuthIndexes()
	rows, err := persistence.QueryAccountCycleHistory(c.Request.Context(), db, maxCycles, activeIndexes)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"accounts": rows})
}

// activeAuthIndexes returns the auth_index list of currently loaded auth files.
// Returns nil if authManager is unavailable (callee treats nil as "no filter").
func (h *Handler) activeAuthIndexes() []string {
	if h.authManager == nil {
		return nil
	}
	auths := h.authManager.List()
	indexes := make([]string, 0, len(auths))
	for _, a := range auths {
		if a.Index != "" {
			indexes = append(indexes, a.Index)
		}
	}
	return indexes
}

// getPersistenceDB returns the db field (nil-safe).
func (h *Handler) getPersistenceDB() *sql.DB {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.persistenceDB
}

// TriggerKeepalive manually triggers keepalive for specific accounts.
// POST /v0/management/keepalive/trigger
//
// Request body:
//
//	{
//	  "auth_indexes": ["abc123", "def456"],  // by auth_index (preferred from frontend)
//	  "auth_ids": ["codex-foo@bar.com-free.json"]  // by auth_id (alternative)
//	}
//
// At least one of auth_indexes or auth_ids must be provided.
func (h *Handler) TriggerKeepalive(c *gin.Context) {
	sched := h.getKeepaliveScheduler()
	if sched == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "keepalive scheduler not available"})
		return
	}

	var body struct {
		AuthIndexes []string `json:"auth_indexes"`
		AuthIDs     []string `json:"auth_ids"`
		Force       bool     `json:"force"` // bypass unavailable check to verify actual upstream quota
		Async       bool     `json:"async"` // return immediately, execute keepalive in background
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body"})
		return
	}

	if len(body.AuthIndexes) == 0 && len(body.AuthIDs) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "at least one of auth_indexes or auth_ids must be provided"})
		return
	}

	// Resolve auth_indexes to auth_ids
	authIDs := make([]string, 0, len(body.AuthIDs)+len(body.AuthIndexes))
	authIDs = append(authIDs, body.AuthIDs...)

	if len(body.AuthIndexes) > 0 {
		indexMap, err := sched.ResolveAuthIDsByIndex(c.Request.Context(), body.AuthIndexes)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to resolve auth indexes: " + err.Error()})
			return
		}
		notFound := make([]string, 0)
		for _, idx := range body.AuthIndexes {
			if id, ok := indexMap[idx]; ok {
				authIDs = append(authIDs, id)
			} else {
				notFound = append(notFound, idx)
			}
		}
		if len(notFound) > 0 && len(authIDs) == 0 {
			c.JSON(http.StatusNotFound, gin.H{"error": "no matching accounts found", "not_found": notFound})
			return
		}
	}

	// Deduplicate
	seen := make(map[string]bool)
	unique := make([]string, 0, len(authIDs))
	for _, id := range authIDs {
		if !seen[id] {
			seen[id] = true
			unique = append(unique, id)
		}
	}

	// Async mode: execute in background, record sent_at only after success.
	if body.Async {
		go func() {
			ctx := context.Background()
			var results []keepalive.KeepaliveResult
			if body.Force {
				results = sched.TriggerForAuthIDsForce(ctx, unique)
			} else {
				results = sched.TriggerForAuthIDs(ctx, unique)
			}
			now := time.Now()
			for _, r := range results {
				if r.Success {
					if err := sched.RecordKeepaliveSentAt(r.AuthID, now); err != nil {
						log.Warnf("keepalive: failed to record sent_at after async trigger: %v", err)
					}
				}
			}
		}()
		c.JSON(http.StatusAccepted, gin.H{
			"accepted":  true,
			"triggered": len(unique),
		})
		return
	}

	results := sched.TriggerForAuthIDs(c.Request.Context(), unique)
	if body.Force {
		results = sched.TriggerForAuthIDsForce(c.Request.Context(), unique)
	}

	successCount := 0
	for _, r := range results {
		if r.Success {
			successCount++
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"triggered": len(unique),
		"success":   successCount,
		"results":   results,
	})
}

// getKeepaliveScheduler returns the scheduler field (nil-safe).
func (h *Handler) getKeepaliveScheduler() *keepalive.Scheduler {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.keepaliveScheduler
}
