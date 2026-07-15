package go_stream

import (
	"context"
	"fmt"
	"io"

	"github.com/vacheslavterentev/go-stream/adapters/parallel"
	"github.com/vacheslavterentev/go-stream/adapters/sink"
	"github.com/vacheslavterentev/go-stream/adapters/source"
	"github.com/vacheslavterentev/go-stream/operators"
	"github.com/vacheslavterentev/go-stream/pipeline/core"
)

const (
	DefaultChunkSize  = 4
	DefaultBufferSize = 4
)

// Stream is the user-facing fluent builder.
type Stream[T any] struct {
	open       func(ctx context.Context) (<-chan core.Chunk[T], error)
	bufferSize int
}

type Option func(*streamConfig)

type streamConfig struct {
	chunkSize  int
	bufferSize int
}

func WithChunkSize(n int) Option {
	return func(c *streamConfig) {
		c.chunkSize = n
	}
}

func WithBufferSize(n int) Option {
	return func(c *streamConfig) {
		c.bufferSize = n
	}
}

func applyOptions(opts ...Option) streamConfig {
	cfg := streamConfig{
		chunkSize:  DefaultChunkSize,
		bufferSize: DefaultBufferSize,
	}
	for _, o := range opts {
		o(&cfg)
	}
	return cfg
}

func FromSlice[T any](items []T, opts ...Option) *Stream[T] {
	cfg := applyOptions(opts...)
	src := source.Slice[T]{
		Items:      items,
		ChunkSize:  cfg.chunkSize,
		BufferSize: cfg.bufferSize,
	}
	return &Stream[T]{
		open:       src.Chunks,
		bufferSize: cfg.bufferSize,
	}
}

func FromCSV(r io.Reader, opts ...Option) *Stream[core.CSVRow] {
	cfg := applyOptions(opts...)
	if cfg.chunkSize == DefaultChunkSize {
		cfg.chunkSize = 0
	}
	src := source.CSV{
		Reader:     r,
		ChunkSize:  cfg.chunkSize,
		BufferSize: cfg.bufferSize,
	}
	return &Stream[core.CSVRow]{
		open:       src.Chunks,
		bufferSize: cfg.bufferSize,
	}
}

func Through[T, U any](s *Stream[T], stage core.Stage[T, U]) *Stream[U] {
	prev := s.open
	buf := s.bufferSize
	return &Stream[U]{
		bufferSize: buf,
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
	return Through(s, operators.Filter[T]{
		Predicate:  pred,
		BufferSize: s.bufferSize,
	})
}

func Map[T, U any](s *Stream[T], fn func(T) (U, error)) *Stream[U] {
	return Through(s, operators.Map[T, U]{
		Fn:         fn,
		BufferSize: s.bufferSize,
	})
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

func WriteCSV(ctx context.Context, s *Stream[core.CSVRow], w io.Writer) error {
	return s.Run(ctx, &sink.CSV{Writer: w})
}

// ParallelMap applies fn concurrently across workers.
// Order of results is not guaranteed.
func ParallelMap[T, U any](s *Stream[T], workers int, fn func(T) (U, error)) *Stream[U] {
	return Through(s, parallel.Map[T, U]{
		Workers:    workers,
		Fn:         fn,
		BufferSize: s.bufferSize,
	})
}

// GroupBy collects stream items into a map keyed by keyFn.
func GroupBy[K comparable, T any](ctx context.Context, s *Stream[T], keyFn func(T) (K, error)) (map[K][]T, error) {
	ctx, fail := core.WithFailure(ctx)

	in, err := s.open(ctx)
	if err != nil {
		return nil, fmt.Errorf("open stream: %w", err)
	}

	groups, err := operators.GroupBy[K, T]{KeyFn: keyFn}.Collect(ctx, in)
	if err != nil {
		return nil, err
	}
	if err := fail(); err != nil {
		return nil, fmt.Errorf("stage: %w", err)
	}
	return groups, nil
}

// OrderedParallelMap applies fn concurrently and preserves input order.
func OrderedParallelMap[T, U any](s *Stream[T], workers int, fn func(T) (U, error)) *Stream[U] {
	return Through(s, parallel.OrderedMap[T, U]{
		Workers:    workers,
		Fn:         fn,
		BufferSize: s.bufferSize,
	})
}

// Join - inner hash-join. Правый stream загружается в память.
func Join[K comparable, L, R any](
	ctx context.Context,
	left *Stream[L],
	right *Stream[R],
	leftKey func(L) (K, error),
	rightKey func(R) (K, error),
) ([]operators.Pair[L, R], error) {
	ctx, fail := core.WithFailure(ctx)

	lCh, err := left.open(ctx)
	if err != nil {
		return nil, fmt.Errorf("open left: %w", err)
	}
	rCh, err := right.open(ctx)
	if err != nil {
		return nil, fmt.Errorf("open right: %w", err)
	}

	pairs, err := operators.InnerJoin[K, L, R]{
		LeftKey:  leftKey,
		RightKey: rightKey,
	}.Collect(ctx, lCh, rCh)
	if err != nil {
		return nil, err
	}
	if err := fail(); err != nil {
		return nil, fmt.Errorf("stage: %w", err)
	}
	return pairs, nil
}
