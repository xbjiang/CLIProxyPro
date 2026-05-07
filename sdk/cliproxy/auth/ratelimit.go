package auth

import (
	"net/http"
	"strconv"
	"strings"
	"time"
)

// ParseRateLimitHeaders extracts rate limit information from upstream HTTP
// response headers. It supports two formats:
//
//  1. Standard OpenAI x-ratelimit-* headers (e.g. GPT API)
//  2. Codex-specific X-Codex-* headers (e.g. Codex free tier)
//
// Returns nil when no relevant headers are present.
func ParseRateLimitHeaders(h http.Header) *RateLimitInfo {
	if h == nil {
		return nil
	}

	// Try standard OpenAI headers first
	if rl := parseOpenAIRateLimitHeaders(h); rl != nil {
		return rl
	}
	// Fall back to Codex-specific headers
	return parseCodexRateLimitHeaders(h)
}

// parseOpenAIRateLimitHeaders handles standard x-ratelimit-* headers.
func parseOpenAIRateLimitHeaders(h http.Header) *RateLimitInfo {
	limitReq := h.Get("x-ratelimit-limit-requests")
	remainReq := h.Get("x-ratelimit-remaining-requests")
	resetReq := h.Get("x-ratelimit-reset-requests")
	limitTok := h.Get("x-ratelimit-limit-tokens")
	remainTok := h.Get("x-ratelimit-remaining-tokens")
	resetTok := h.Get("x-ratelimit-reset-tokens")

	if limitReq == "" && remainReq == "" && resetReq == "" &&
		limitTok == "" && remainTok == "" && resetTok == "" {
		return nil
	}

	now := time.Now()
	rl := &RateLimitInfo{UpdatedAt: now}

	if v, err := strconv.Atoi(limitReq); err == nil {
		rl.LimitRequests = v
	}
	if v, err := strconv.Atoi(remainReq); err == nil {
		rl.RemainingRequests = v
	}
	if d, err := time.ParseDuration(resetReq); err == nil {
		rl.ResetRequests = now.Add(d)
	}
	if v, err := strconv.Atoi(limitTok); err == nil {
		rl.LimitTokens = v
	}
	if v, err := strconv.Atoi(remainTok); err == nil {
		rl.RemainingTokens = v
	}
	if d, err := time.ParseDuration(resetTok); err == nil {
		rl.ResetTokens = now.Add(d)
	}

	return rl
}

// parseCodexRateLimitHeaders handles Codex-specific X-Codex-* headers.
//
// Relevant headers:
//   - X-Codex-Primary-Used-Percent: percentage of primary window used (0-100)
//   - X-Codex-Primary-Window-Minutes: primary window duration in minutes
//   - X-Codex-Primary-Reset-At: Unix timestamp when primary window resets
//   - X-Codex-Primary-Reset-After-Seconds: seconds until primary window resets
//   - X-Codex-Secondary-Used-Percent: percentage of secondary window used
//   - X-Codex-Secondary-Window-Minutes: secondary window duration in minutes
//   - X-Codex-Secondary-Reset-At: Unix timestamp (may be empty)
//   - X-Codex-Secondary-Reset-After-Seconds: seconds until secondary resets
//
// We map primary window → requests, secondary window → tokens (conceptual mapping).
func parseCodexRateLimitHeaders(h http.Header) *RateLimitInfo {
	primaryUsedPct := h.Get("X-Codex-Primary-Used-Percent")
	primaryResetAt := h.Get("X-Codex-Primary-Reset-At")
	primaryResetAfter := h.Get("X-Codex-Primary-Reset-After-Seconds")
	secondaryUsedPct := h.Get("X-Codex-Secondary-Used-Percent")
	secondaryResetAt := h.Get("X-Codex-Secondary-Reset-At")
	secondaryResetAfter := h.Get("X-Codex-Secondary-Reset-After-Seconds")

	if primaryUsedPct == "" && primaryResetAt == "" && secondaryUsedPct == "" {
		return nil
	}

	now := time.Now()
	rl := &RateLimitInfo{
		LimitRequests: 100, // represent as percentage scale
		LimitTokens:   100,
		UpdatedAt:     now,
	}

	// Primary window → mapped to "requests"
	if v, err := strconv.Atoi(primaryUsedPct); err == nil {
		rl.RemainingRequests = 100 - v
	}
	if v, err := strconv.ParseInt(primaryResetAt, 10, 64); err == nil && v > 0 {
		rl.ResetRequests = time.Unix(v, 0)
	} else if v, err := strconv.Atoi(primaryResetAfter); err == nil && v > 0 {
		rl.ResetRequests = now.Add(time.Duration(v) * time.Second)
	}

	// Secondary window → mapped to "tokens"
	if v, err := strconv.Atoi(secondaryUsedPct); err == nil {
		rl.RemainingTokens = 100 - v
	}
	if v, err := strconv.ParseInt(secondaryResetAt, 10, 64); err == nil && v > 0 {
		rl.ResetTokens = time.Unix(v, 0)
	} else if v, err := strconv.Atoi(secondaryResetAfter); err == nil && v > 0 {
		rl.ResetTokens = now.Add(time.Duration(v) * time.Second)
	}

	// Plan type and window durations
	if pt := strings.TrimSpace(h.Get("X-Codex-Plan-Type")); pt != "" {
		rl.PlanType = strings.ToLower(pt)
	}
	if v, err := strconv.Atoi(h.Get("X-Codex-Primary-Window-Minutes")); err == nil && v > 0 {
		rl.PrimaryWindowMinutes = v
	}
	if v, err := strconv.Atoi(h.Get("X-Codex-Secondary-Window-Minutes")); err == nil && v > 0 {
		rl.SecondaryWindowMinutes = v
	}

	return rl
}
