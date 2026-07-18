package management

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/persistence"
)

// GetSessionBindings returns all persistent session-to-relay bindings.
// GET /v0/management/routing/session-bindings
func (h *Handler) GetSessionBindings(c *gin.Context) {
	if h.persistenceDB == nil {
		c.JSON(http.StatusOK, gin.H{"bindings": []struct{}{}})
		return
	}
	bindings, err := persistence.ListSessionBindings(h.persistenceDB)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"bindings": bindings})
}

// PutSessionBinding creates or updates a session binding.
// PUT /v0/management/routing/session-bindings
// Body: {"session_id": "...", "auth_index": "...", "provider": "codex|claude", "note": "..."}
func (h *Handler) PutSessionBinding(c *gin.Context) {
	if h.persistenceDB == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "persistence not available"})
		return
	}
	var body struct {
		SessionID string `json:"session_id"`
		AuthIndex string `json:"auth_index"`
		Provider  string `json:"provider"`
		Note      string `json:"note"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if body.SessionID == "" || body.AuthIndex == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "session_id and auth_index are required"})
		return
	}
	if err := persistence.UpsertSessionBinding(h.persistenceDB, body.SessionID, body.AuthIndex, body.Provider, body.Note); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

// DeleteSessionBinding removes a persistent session binding and invalidates the in-memory affinity cache.
// DELETE /v0/management/routing/session-bindings/:session_id
func (h *Handler) DeleteSessionBinding(c *gin.Context) {
	sessionID := c.Param("session_id")
	if sessionID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "session_id is required"})
		return
	}

	if h.persistenceDB != nil {
		if err := persistence.DeleteSessionBinding(h.persistenceDB, sessionID); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
	}

	h.authManager.InvalidateSessionCache(sessionID)
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

// GetRecentSessions returns recently seen sessions for a given provider.
// GET /v0/management/routing/recent-sessions?provider=codex
func (h *Handler) GetRecentSessions(c *gin.Context) {
	if h.persistenceDB == nil {
		c.JSON(http.StatusOK, gin.H{"sessions": []struct{}{}})
		return
	}
	provider := c.Query("provider")
	sessions, err := persistence.ListRecentSessions(h.persistenceDB, provider, 50)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"sessions": sessions})
}
