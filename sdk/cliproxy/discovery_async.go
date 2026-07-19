package cliproxy

import (
	"context"
	"strings"
	"time"

	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	log "github.com/sirupsen/logrus"
)

// asyncDiscoverUpstreamModels fetches models from the upstream API asynchronously
// and updates the registry if successful. It completely overrides the existing
// static or config models with the dynamically discovered models from the relay.
//
// Used by the startup / config-reload path. The pin-switch path calls the
// synchronous DiscoverModelsForAuthSync directly so the caller can wait for the
// refreshed model list before returning.
func (s *Service) asyncDiscoverUpstreamModels(auth *coreauth.Auth, providerKey string, excludedModels []string) {
	if s == nil || auth == nil {
		return
	}

	// Background context with timeout to avoid lingering goroutines
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)

	go func() {
		defer cancel()
		if err := s.DiscoverModelsForAuthSync(ctx, auth, providerKey, excludedModels); err != nil {
			log.Debugf("upstream model discovery skipped or failed for auth %s (provider: %s): %v", auth.ID, providerKey, err)
		}
	}()
}

// DiscoverModelsForAuthSync synchronously fetches the upstream model list for the
// given auth and overwrites its registry registration with the discovered models.
// It is the shared core used by both the async startup discovery and the
// pin-switch refresh path. Returns an error only when the upstream fetch or
// registration fails; unsupported providers return nil (no-op).
func (s *Service) DiscoverModelsForAuthSync(ctx context.Context, auth *coreauth.Auth, providerKey string, excludedModels []string) error {
	if s == nil || auth == nil {
		return nil
	}

	// Only discover for providers that make sense for dynamic upstream discovery.
	// We can expand this list if we want openai-compatibility to also be auto-discovered.
	isSupported := false
	if providerKey == "claude" || providerKey == "openai-compatibility" || strings.HasPrefix(providerKey, "openai-compatibility:") {
		isSupported = true
	}
	if !isSupported {
		return nil
	}

	models, err := fetchUpstreamModels(ctx, auth, providerKey)
	if err != nil {
		return err
	}

	// Apply exclusions if any were configured
	models = applyExcludedModels(models, excludedModels)
	if len(models) == 0 {
		log.Debugf("upstream model discovery for auth %s yielded no models after exclusions", auth.ID)
		return nil
	}

	// Apply prefixes just like in the normal registration flow
	var forcePrefix bool
	s.cfgMu.RLock()
	if s.cfg != nil {
		forcePrefix = s.cfg.ForceModelPrefix
	}
	s.cfgMu.RUnlock()
	models = applyModelPrefixes(models, auth.Prefix, forcePrefix)

	// Before applying the new models, check if the auth is still valid/active.
	// If it has been removed or modified, we discard the results.
	latestAuth, ok := s.latestAuthForModelRegistration(auth.ID)
	if !ok || latestAuth.Disabled {
		return nil
	}

	// Register the dynamically discovered models. This overwrites the static models
	// previously registered during registerModelsForAuth.
	s.registerResolvedModelsForAuth(latestAuth, providerKey, models)

	log.Infof("successfully discovered %d upstream models for auth %s", len(models), auth.ID)
	return nil
}
