// Package kacontext provides the keepalive context key shared between the
// persistence and keepalive packages to avoid circular imports.
package kacontext

import "context"

type keepaliveCtxKey struct{}

// WithKeepaliveContext returns a derived context marked as a keepalive request.
func WithKeepaliveContext(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, keepaliveCtxKey{}, true)
}

// IsKeepaliveContext reports whether ctx was created by WithKeepaliveContext.
func IsKeepaliveContext(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	v, ok := ctx.Value(keepaliveCtxKey{}).(bool)
	return ok && v
}
