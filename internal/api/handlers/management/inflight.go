package management

import (
	"net/http"
	"sort"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/inflight"
)

type inflightEntryResponse struct {
	ID          string  `json:"id"`
	Provider    string  `json:"provider"`
	AuthID      string  `json:"auth_id"`
	Source      string  `json:"source"`
	StartedAt   string  `json:"started_at"`
	DurationSec float64 `json:"duration_sec"`
}

// GetInflight returns all currently in-flight upstream requests.
func (h *Handler) GetInflight(c *gin.Context) {
	entries := inflight.Global.List()
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].StartedAt.Before(entries[j].StartedAt)
	})
	out := make([]inflightEntryResponse, len(entries))
	for i, e := range entries {
		out[i] = inflightEntryResponse{
			ID:          e.ID,
			Provider:    e.Provider,
			AuthID:      e.AuthID,
			Source:      e.Source,
			StartedAt:   e.StartedAt.UTC().Format("2006-01-02T15:04:05.000Z"),
			DurationSec: e.DurationSec(),
		}
	}
	c.JSON(http.StatusOK, gin.H{"entries": out})
}

// CancelInflight cancels an in-flight upstream request by ID.
// CLIProxyPro will abort the upstream HTTP call and retry with the next relay.
func (h *Handler) CancelInflight(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "missing id"})
		return
	}
	ok := inflight.Global.Cancel(id)
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "not found or already completed"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"cancelled": true})
}
