package core_test

import (
	"context"
	"errors"
	"testing"

	"github.com/vacheslavterentev/go-stream/pipeline/core"
)

func TestWithFailure_PropagatesError(t *testing.T) {
	t.Parallel()

	ctx, fail := core.WithFailure(context.Background())
	core.Fail(ctx, errors.New("boom"))

	if err := fail(); err == nil || err.Error() != "boom" {
		t.Fatalf("expected boom, got %v", err)
	}
}

func TestFail_NilErrorIsNoop(t *testing.T) {
	t.Parallel()

	ctx, fail := core.WithFailure(context.Background())
	core.Fail(ctx, nil)

	if err := fail(); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}
