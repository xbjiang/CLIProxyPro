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
func (s *Service) asyncDiscoverUpstreamModels(auth *coreauth.Auth, providerKey string, excludedModels []string) {
	if s == nil || auth == nil {
		return
	}

	// Make sure we only discover for providers that make sense for dynamic upstream discovery.
	// We can expand this list if we want openai-compatibility to also be auto-discovered.
	isSupported := false
	if providerKey == "claude" || providerKey == "openai-compatibility" || strings.HasPrefix(providerKey, "openai-compatibility:") {
		isSupported = true
	}
	if !isSupported {
		return
	}

	// Background context with timeout to avoid lingering goroutines
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)

	go func() {
		defer cancel()

		models, err := fetchUpstreamModels(ctx, auth, providerKey)
		if err != nil {
			log.Debugf("upstream model discovery skipped or failed for auth %s (provider: %s): %v", auth.ID, providerKey, err)
			return
		}

		// Apply exclusions if any were configured
		models = applyExcludedModels(models, excludedModels)
		if len(models) == 0 {
			log.Debugf("upstream model discovery for auth %s yielded no models after exclusions", auth.ID)
			return
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
			return
		}

		// Register the dynamically discovered models. This will overwrite the static models
		// previously registered during registerModelsForAuth.
		s.registerResolvedModelsForAuth(latestAuth, providerKey, models)

		log.Infof("successfully discovered %d upstream models for auth %s", len(models), auth.ID)
	}()
}
