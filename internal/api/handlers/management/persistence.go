package management

import (
	"database/sql"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/keepalive"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/persistence"
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
	rows, err := persistence.QueryPerAccountCycles(c.Request.Context(), db)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"accounts": rows})
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
	rows, err := persistence.QueryAccountCycleHistory(c.Request.Context(), db, maxCycles)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"accounts": rows})
}

// getPersistenceDB returns the db field (nil-safe).
func (h *Handler) getPersistenceDB() *sql.DB {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.persistenceDB
}

// getKeepaliveScheduler returns the scheduler field (nil-safe).
func (h *Handler) getKeepaliveScheduler() *keepalive.Scheduler {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.keepaliveScheduler
}
