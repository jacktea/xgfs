package vfs

import "context"

type contextKey string

var userKey contextKey = "xgfs/user"

// NewContextWithUser returns a context carrying user identity.
func NewContextWithUser(ctx context.Context, user User) context.Context {
	return context.WithValue(ctx, userKey, user)
}

// UserFromContext extracts vfs.User from ctx, falling back to UID/GID 0.
func UserFromContext(ctx context.Context) User {
	if ctx == nil {
		return User{}
	}
	if v := ctx.Value(userKey); v != nil {
		if user, ok := v.(User); ok {
			return user
		}
	}
	return User{}
}
