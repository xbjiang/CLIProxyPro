package management

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/persistence"
)

const relayTagPoolSettingKey = "relay_tag_pool"

// GetRelayTagPool returns the global relay tag pool as a JSON string array.
// The pool is the set of candidate labels users can assign to relays.
// Stored in the persistence settings table under the key "relay_tag_pool".
func (h *Handler) GetRelayTagPool(c *gin.Context) {
	if h.persistenceDB == nil {
		c.JSON(http.StatusOK, gin.H{"tags": []string{}})
		return
	}
	raw, err := persistence.GetSetting(h.persistenceDB, relayTagPoolSettingKey)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to read tag pool: " + err.Error()})
		return
	}
	var tags []string
	if raw != "" {
		if err := json.Unmarshal([]byte(raw), &tags); err != nil {
			// Treat corrupt value as empty.
			tags = []string{}
		}
	}
	if tags == nil {
		tags = []string{}
	}
	c.JSON(http.StatusOK, gin.H{"tags": tags})
}

// PutRelayTagPool replaces the global relay tag pool.
// Body: {"tags": ["tag1", "tag2", ...]}
func (h *Handler) PutRelayTagPool(c *gin.Context) {
	var body struct {
		Tags []string `json:"tags"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid body"})
		return
	}
	if h.persistenceDB == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "persistence DB not initialized"})
		return
	}

	// Deduplicate + trim whitespace; preserve order of first occurrence.
	seen := make(map[string]struct{}, len(body.Tags))
	normalized := make([]string, 0, len(body.Tags))
	for _, t := range body.Tags {
		trimmed := strings.TrimSpace(t)
		if trimmed == "" {
			continue
		}
		if _, dup := seen[trimmed]; dup {
			continue
		}
		seen[trimmed] = struct{}{}
		normalized = append(normalized, trimmed)
	}

	data, err := json.Marshal(normalized)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to encode tag pool"})
		return
	}
	if err := persistence.SetSetting(h.persistenceDB, relayTagPoolSettingKey, string(data)); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to save tag pool: " + err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"tags": normalized})
}
