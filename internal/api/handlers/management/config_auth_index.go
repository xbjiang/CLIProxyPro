package management

import (
	"fmt"
	"strings"
	"time"

	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/watcher/synthesizer"
)

type authRuntimeStatus struct {
	Unavailable   bool              `json:"unavailable"`
	Status        string            `json:"status,omitempty"`
	StatusMessage string            `json:"status_message,omitempty"`
	NextRetryAfter *time.Time       `json:"next_retry_after,omitempty"`
	ModelStates   map[string]*modelStateSnapshot `json:"model_states,omitempty"`
}

type modelStateSnapshot struct {
	Unavailable    bool       `json:"unavailable"`
	StatusMessage  string     `json:"status_message,omitempty"`
	NextRetryAfter *time.Time `json:"next_retry_after,omitempty"`
	QuotaExceeded  bool       `json:"quota_exceeded,omitempty"`
}

func buildRuntimeStatus(auth *coreauth.Auth) *authRuntimeStatus {
	if auth == nil {
		return nil
	}
	// Check if auth-level is unavailable
	authUnavailable := auth.Unavailable || auth.Status != coreauth.StatusActive

	// Also check if any model state has issues
	modelIssues := false
	var modelStates map[string]*modelStateSnapshot
	if len(auth.ModelStates) > 0 {
		for model, state := range auth.ModelStates {
			if state == nil {
				continue
			}
			if state.Unavailable || state.Quota.Exceeded {
				modelIssues = true
				if modelStates == nil {
					modelStates = make(map[string]*modelStateSnapshot)
				}
				snap := &modelStateSnapshot{
					Unavailable:   state.Unavailable,
					StatusMessage: state.StatusMessage,
					QuotaExceeded: state.Quota.Exceeded,
				}
				if !state.NextRetryAfter.IsZero() {
					t := state.NextRetryAfter
					snap.NextRetryAfter = &t
				}
				modelStates[model] = snap
			}
		}
	}

	// Only return status if there's an issue at auth or model level
	if !authUnavailable && !modelIssues {
		return nil
	}

	rs := &authRuntimeStatus{
		Unavailable:   authUnavailable,
		Status:        string(auth.Status),
		StatusMessage: auth.StatusMessage,
	}
	if !auth.NextRetryAfter.IsZero() {
		t := auth.NextRetryAfter
		rs.NextRetryAfter = &t
	}
	if modelStates != nil {
		rs.ModelStates = modelStates
	}
	return rs
}

type geminiKeyWithAuthIndex struct {
	config.GeminiKey
	AuthIndex string `json:"auth-index,omitempty"`
}

type claudeKeyWithAuthIndex struct {
	config.ClaudeKey
	AuthIndex      string             `json:"auth-index,omitempty"`
	RuntimeStatus  *authRuntimeStatus `json:"runtime_status,omitempty"`
}

type codexKeyWithAuthIndex struct {
	config.CodexKey
	AuthIndex      string             `json:"auth-index,omitempty"`
	RuntimeStatus  *authRuntimeStatus `json:"runtime_status,omitempty"`
}

type vertexCompatKeyWithAuthIndex struct {
	config.VertexCompatKey
	AuthIndex string `json:"auth-index,omitempty"`
}

type openAICompatibilityAPIKeyWithAuthIndex struct {
	config.OpenAICompatibilityAPIKey
	AuthIndex string `json:"auth-index,omitempty"`
}

type openAICompatibilityWithAuthIndex struct {
	Name          string                                   `json:"name"`
	Priority      int                                      `json:"priority,omitempty"`
	Prefix        string                                   `json:"prefix,omitempty"`
	BaseURL       string                                   `json:"base-url"`
	APIKeyEntries []openAICompatibilityAPIKeyWithAuthIndex `json:"api-key-entries,omitempty"`
	Models        []config.OpenAICompatibilityModel        `json:"models,omitempty"`
	Headers       map[string]string                        `json:"headers,omitempty"`
	AuthIndex     string                                   `json:"auth-index,omitempty"`
}

func (h *Handler) liveAuthIndexByID() map[string]string {
	out := map[string]string{}
	if h == nil {
		return out
	}
	h.mu.Lock()
	manager := h.authManager
	h.mu.Unlock()
	if manager == nil {
		return out
	}
	for _, auth := range manager.List() {
		if auth == nil {
			continue
		}
		id := strings.TrimSpace(auth.ID)
		if id == "" {
			continue
		}
		idx := strings.TrimSpace(auth.Index)
		if idx == "" {
			idx = auth.EnsureIndex()
		}
		if idx == "" {
			continue
		}
		out[id] = idx
	}
	return out
}

type liveAuthEntry struct {
	Index string
	Auth  *coreauth.Auth
}

func (h *Handler) liveAuthEntriesByID() map[string]liveAuthEntry {
	out := map[string]liveAuthEntry{}
	if h == nil {
		return out
	}
	h.mu.Lock()
	manager := h.authManager
	h.mu.Unlock()
	if manager == nil {
		return out
	}
	for _, auth := range manager.List() {
		if auth == nil {
			continue
		}
		id := strings.TrimSpace(auth.ID)
		if id == "" {
			continue
		}
		idx := strings.TrimSpace(auth.Index)
		if idx == "" {
			idx = auth.EnsureIndex()
		}
		if idx == "" {
			continue
		}
		out[id] = liveAuthEntry{Index: idx, Auth: auth}
	}
	return out
}

func (h *Handler) geminiKeysWithAuthIndex() []geminiKeyWithAuthIndex {
	if h == nil {
		return nil
	}
	liveIndexByID := h.liveAuthIndexByID()

	h.mu.Lock()
	defer h.mu.Unlock()
	if h.cfg == nil {
		return nil
	}

	idGen := synthesizer.NewStableIDGenerator()
	out := make([]geminiKeyWithAuthIndex, len(h.cfg.GeminiKey))
	for i := range h.cfg.GeminiKey {
		entry := h.cfg.GeminiKey[i]
		authIndex := ""
		if key := strings.TrimSpace(entry.APIKey); key != "" {
			id, _ := idGen.Next("gemini:apikey", key, entry.BaseURL)
			authIndex = liveIndexByID[id]
		}
		out[i] = geminiKeyWithAuthIndex{
			GeminiKey: entry,
			AuthIndex: authIndex,
		}
	}
	return out
}

func (h *Handler) claudeKeysWithAuthIndex() []claudeKeyWithAuthIndex {
	if h == nil {
		return nil
	}
	liveEntries := h.liveAuthEntriesByID()

	h.mu.Lock()
	defer h.mu.Unlock()
	if h.cfg == nil {
		return nil
	}

	idGen := synthesizer.NewStableIDGenerator()
	out := make([]claudeKeyWithAuthIndex, len(h.cfg.ClaudeKey))
	for i := range h.cfg.ClaudeKey {
		entry := h.cfg.ClaudeKey[i]
		var authIndex string
		var runtimeStatus *authRuntimeStatus
		if key := strings.TrimSpace(entry.APIKey); key != "" {
			id, _ := idGen.Next("claude:apikey", key, entry.BaseURL)
			if e, ok := liveEntries[id]; ok {
				authIndex = e.Index
				runtimeStatus = buildRuntimeStatus(e.Auth)
			}
		}
		out[i] = claudeKeyWithAuthIndex{
			ClaudeKey:     entry,
			AuthIndex:     authIndex,
			RuntimeStatus: runtimeStatus,
		}
	}
	return out
}

func (h *Handler) codexKeysWithAuthIndex() []codexKeyWithAuthIndex {
	if h == nil {
		return nil
	}
	liveEntries := h.liveAuthEntriesByID()

	h.mu.Lock()
	defer h.mu.Unlock()
	if h.cfg == nil {
		return nil
	}

	idGen := synthesizer.NewStableIDGenerator()
	out := make([]codexKeyWithAuthIndex, len(h.cfg.CodexKey))
	for i := range h.cfg.CodexKey {
		entry := h.cfg.CodexKey[i]
		var authIndex string
		var runtimeStatus *authRuntimeStatus
		if key := strings.TrimSpace(entry.APIKey); key != "" {
			id, _ := idGen.Next("codex:apikey", key, entry.BaseURL)
			if e, ok := liveEntries[id]; ok {
				authIndex = e.Index
				runtimeStatus = buildRuntimeStatus(e.Auth)
			}
		}
		out[i] = codexKeyWithAuthIndex{
			CodexKey:      entry,
			AuthIndex:     authIndex,
			RuntimeStatus: runtimeStatus,
		}
	}
	return out
}

func (h *Handler) vertexCompatKeysWithAuthIndex() []vertexCompatKeyWithAuthIndex {
	if h == nil {
		return nil
	}
	liveIndexByID := h.liveAuthIndexByID()

	h.mu.Lock()
	defer h.mu.Unlock()
	if h.cfg == nil {
		return nil
	}

	idGen := synthesizer.NewStableIDGenerator()
	out := make([]vertexCompatKeyWithAuthIndex, len(h.cfg.VertexCompatAPIKey))
	for i := range h.cfg.VertexCompatAPIKey {
		entry := h.cfg.VertexCompatAPIKey[i]
		id, _ := idGen.Next("vertex:apikey", entry.APIKey, entry.BaseURL, entry.ProxyURL)
		authIndex := liveIndexByID[id]
		out[i] = vertexCompatKeyWithAuthIndex{
			VertexCompatKey: entry,
			AuthIndex:       authIndex,
		}
	}
	return out
}

func (h *Handler) openAICompatibilityWithAuthIndex() []openAICompatibilityWithAuthIndex {
	if h == nil {
		return nil
	}
	liveIndexByID := h.liveAuthIndexByID()

	h.mu.Lock()
	defer h.mu.Unlock()
	if h.cfg == nil {
		return nil
	}

	normalized := normalizedOpenAICompatibilityEntries(h.cfg.OpenAICompatibility)
	out := make([]openAICompatibilityWithAuthIndex, len(normalized))
	idGen := synthesizer.NewStableIDGenerator()
	for i := range normalized {
		entry := normalized[i]
		providerName := strings.ToLower(strings.TrimSpace(entry.Name))
		if providerName == "" {
			providerName = "openai-compatibility"
		}
		idKind := fmt.Sprintf("openai-compatibility:%s", providerName)

		response := openAICompatibilityWithAuthIndex{
			Name:      entry.Name,
			Priority:  entry.Priority,
			Prefix:    entry.Prefix,
			BaseURL:   entry.BaseURL,
			Models:    entry.Models,
			Headers:   entry.Headers,
			AuthIndex: "",
		}
		if len(entry.APIKeyEntries) == 0 {
			id, _ := idGen.Next(idKind, entry.BaseURL)
			response.AuthIndex = liveIndexByID[id]
		} else {
			response.APIKeyEntries = make([]openAICompatibilityAPIKeyWithAuthIndex, len(entry.APIKeyEntries))
			for j := range entry.APIKeyEntries {
				apiKeyEntry := entry.APIKeyEntries[j]
				id, _ := idGen.Next(idKind, apiKeyEntry.APIKey, entry.BaseURL, apiKeyEntry.ProxyURL)
				response.APIKeyEntries[j] = openAICompatibilityAPIKeyWithAuthIndex{
					OpenAICompatibilityAPIKey: apiKeyEntry,
					AuthIndex:                 liveIndexByID[id],
				}
			}
		}
		out[i] = response
	}
	return out
}
