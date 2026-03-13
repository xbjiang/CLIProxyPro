// Package keepalive provides automatic keepalive scheduling for quota-exhausted accounts.
package keepalive

import (
	"context"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/kacontext"
)

// WithKeepaliveContext returns a derived context marked as a keepalive request.
func WithKeepaliveContext(ctx context.Context) context.Context {
	return kacontext.WithKeepaliveContext(ctx)
}

// IsKeepaliveContext reports whether ctx was created by WithKeepaliveContext.
func IsKeepaliveContext(ctx context.Context) bool {
	return kacontext.IsKeepaliveContext(ctx)
}
