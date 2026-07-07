package core

import (
	"context"
	"fmt"
)

type Runner[T any] struct {
	source Source[T]
	stages []func(ctx context.Context, in <-chan Chunk[T]) (<-chan Chunk[T], error)
}

func NewRunner[T any](src Source[T]) *Runner[T] {
	return &Runner[T]{source: src}
}

func (r *Runner[T]) AddStage(fn func(ctx context.Context, in <-chan Chunk[T]) (<-chan Chunk[T], error)) *Runner[T] {
	r.stages = append(r.stages, fn)
	return r
}

func (r *Runner[T]) Execute(ctx context.Context, sink Sink[T]) error {
	in, err := r.source.Chunks(ctx)
	if err != nil {
		return fmt.Errorf("source: %w", err)
	}

	current := in
	for i, stage := range r.stages {
		next, err := stage(ctx, current)
		if err != nil {
			return fmt.Errorf("stage %d: %w", i, err)
		}
		current = next
	}

	if err := sink.Consume(ctx, current); err != nil {
		return fmt.Errorf("sink: %w", err)
	}
	return nil
}
