package cliproxy

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/proxyutil"
)

type upstreamModelsResponse struct {
	Data []struct {
		ID string `json:"id"`
	} `json:"data"`
}

// fetchUpstreamModels performs an HTTP GET request to the upstream /v1/models endpoint
// to dynamically discover the models supported by this specific authentication entry.
func fetchUpstreamModels(ctx context.Context, auth *coreauth.Auth, provider string) ([]*registry.ModelInfo, error) {
	if auth == nil {
		return nil, fmt.Errorf("auth is nil")
	}

	baseURL := ""
	if auth.Attributes != nil {
		baseURL = strings.TrimSpace(auth.Attributes["base_url"])
	}

	if baseURL == "" {
		if provider == "claude" {
			baseURL = "https://api.anthropic.com"
		} else {
			return nil, fmt.Errorf("no base_url configured for discovery")
		}
	}

	// Normalize base URL and append /v1/models
	baseURL = strings.TrimRight(baseURL, "/")
	if !strings.HasSuffix(baseURL, "/v1") {
		baseURL = baseURL + "/v1"
	}
	reqURL := baseURL + "/models"

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	apiKey := ""
	if auth.Attributes != nil {
		apiKey = strings.TrimSpace(auth.Attributes["api_key"])
	}

	if apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+apiKey)
		req.Header.Set("x-api-key", apiKey)
	}
	req.Header.Set("Accept", "application/json")

	client := &http.Client{Timeout: 10 * time.Second}
	if transport, _, errProxy := proxyutil.BuildHTTPTransport(auth.ProxyURL); errProxy == nil && transport != nil {
		client.Transport = transport
	}

	resp, errDo := client.Do(req)
	if errDo != nil {
		return nil, fmt.Errorf("upstream request failed: %w", errDo)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	body, errRead := io.ReadAll(resp.Body)
	if errRead != nil {
		return nil, fmt.Errorf("failed to read response body: %w", errRead)
	}

	var parsed upstreamModelsResponse
	if errParse := json.Unmarshal(body, &parsed); errParse != nil {
		return nil, fmt.Errorf("failed to parse JSON response: %w", errParse)
	}

	var models []*registry.ModelInfo
	now := time.Now().Unix()
	for _, m := range parsed.Data {
		id := strings.TrimSpace(m.ID)
		if id == "" {
			continue
		}

		model := &registry.ModelInfo{
			ID:          id,
			Object:      "model",
			Created:     now,
			OwnedBy:     provider,
			Type:        provider,
			DisplayName: id,
			UserDefined: true,
			Thinking:    &registry.ThinkingSupport{Levels: []string{"low", "medium", "high"}},
		}
		models = append(models, model)
	}

	if len(models) == 0 {
		return nil, fmt.Errorf("no valid models found in response")
	}

	return models, nil
}
