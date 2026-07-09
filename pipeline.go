package go_stream

import (
	"context"
	"fmt"

	"github.com/vacheslavterentev/go-stream/adapters/sink"
	"github.com/vacheslavterentev/go-stream/adapters/source"
	"github.com/vacheslavterentev/go-stream/operators"
	"github.com/vacheslavterentev/go-stream/pipeline/core"
)

// Stream is the user-facing fluent builder.
type Stream[T any] struct {
	open func(ctx context.Context) (<-chan core.Chunk[T], error)
}

func FromSlice[T any](items []T) *Stream[T] {
	src := source.Slice[T]{Items: items, ChunkSize: 4}
	return &Stream[T]{
		open: src.Chunks,
	}
}

func Through[T, U any](s *Stream[T], stage core.Stage[T, U]) *Stream[U] {
	prev := s.open
	return &Stream[U]{
		open: func(ctx context.Context) (<-chan core.Chunk[U], error) {
			in, err := prev(ctx)
			if err != nil {
				return nil, err
			}
			return stage.Apply(ctx, in)
		},
	}
}

func (s *Stream[T]) Filter(pred func(T) (bool, error)) *Stream[T] {
	return Through(s, operators.Filter[T]{Predicate: pred})
}

func Map[T, U any](s *Stream[T], fn func(T) (U, error)) *Stream[U] {
	return Through(s, operators.Map[T, U]{Fn: fn})
}

func (s *Stream[T]) Run(ctx context.Context, snk core.Sink[T]) error {
	ctx, fail := core.WithFailure(ctx)

	in, err := s.open(ctx)
	if err != nil {
		return fmt.Errorf("open stream: %w", err)
	}

	if err := snk.Consume(ctx, in); err != nil {
		return err
	}
	if err := fail(); err != nil {
		return fmt.Errorf("stage: %w", err)
	}
	return nil
}

func (s *Stream[T]) Collect(ctx context.Context) ([]T, error) {
	var out []T
	snk := &sink.Slice[T]{Items: &out}
	if err := s.Run(ctx, snk); err != nil {
		return nil, err
	}
	return out, nil
}
