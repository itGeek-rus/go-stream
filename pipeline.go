package go_stream

import (
	"context"

	"github.com/vacheslavterentev/go-stream/adapters/sink"
	"github.com/vacheslavterentev/go-stream/adapters/source"
	"github.com/vacheslavterentev/go-stream/operators"
	"github.com/vacheslavterentev/go-stream/pipeline/core"
)

// Stream is the user-facing fluent builder.
type Stream[T any] struct {
	src    core.Source[T]
	stages []core.Stage[T, T]
}

func FromSlice[T any](items []T) *Stream[T] {
	return &Stream[T]{
		src: source.Slice[T]{Items: items, ChunkSize: 4},
	}
}

func (s *Stream[T]) Filter(pred func(T) (bool, error)) *Stream[T] {
	s.stages = append(s.stages, operators.Filter[T]{Predicate: pred})
	return s
}

func (s *Stream[T]) Run(ctx context.Context, snk core.Sink[T]) error {
	in, err := s.src.Chunks(ctx)
	if err != nil {
		return err
	}

	current := in
	for _, stage := range s.stages {
		current, err = stage.Apply(ctx, current)
		if err != nil {
			return err
		}
	}
	return snk.Consume(ctx, current)
}

func (s *Stream[T]) Collect(ctx context.Context) ([]T, error) {
	var out []T
	snk := &sink.Slice[T]{Items: &out}
	if err := s.Run(ctx, snk); err != nil {
		return nil, err
	}
	return out, nil
}
