package core

import "context"

type cancelKey struct{}

// WithFailure добавляет в ctx механизм fail-fast для stages.
func WithFailure(ctx context.Context) (context.Context, func() error) {
	ctx, cancel := context.WithCancelCause(ctx)
	ctx = context.WithValue(ctx, cancelKey{}, cancel)
	return ctx, func() error {
		return context.Cause(ctx)
	}
}

// Fail останавливает pipeline с ошибкой.
func Fail(ctx context.Context, err error) {
	if err == nil {
		return
	}
	if cancel, ok := ctx.Value(cancelKey{}).(context.CancelCauseFunc); ok {
		cancel(err)
	}
}
