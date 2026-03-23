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

type forceProbeCtxKey struct{}

// WithForceProbeContext returns a derived context that instructs the scheduler
// to bypass the unavailable/blocked check for pinned-auth requests.
// Used by manual "force probe" keepalive to verify actual upstream quota status.
func WithForceProbeContext(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, forceProbeCtxKey{}, true)
}

// IsForceProbeContext reports whether ctx carries the force-probe flag.
func IsForceProbeContext(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	v, ok := ctx.Value(forceProbeCtxKey{}).(bool)
	return ok && v
}
