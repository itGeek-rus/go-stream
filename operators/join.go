package operators

import (
	"context"

	"github.com/vacheslavterentev/go-stream/pipeline/core"
)

// Pair is the result of an inner join.
type Pair[L, R any] struct {
	Left  L
	Right R
}

// InnerJoin builds a hash index over the right stream, then matches left.
// The right stream is fully buffered in memory.
type InnerJoin[K comparable, L, R any] struct {
	LeftKey  func(L) (K, error)
	RightKey func(R) (K, error)
}

func (j InnerJoin[K, L, R]) Collect(
	ctx context.Context,
	left <-chan core.Chunk[L],
	right <-chan core.Chunk[R],
) ([]Pair[L, R], error) {
	index := make(map[K][]R)

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case chunk, ok := <-right:
			if !ok {
				goto match
			}
			for _, item := range chunk {
				k, err := j.RightKey(item)
				if err != nil {
					core.Fail(ctx, err)
					return nil, err
				}
				index[k] = append(index[k], item)
			}
		}
	}

match:
	var out []Pair[L, R]
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case chunk, ok := <-left:
			if !ok {
				return out, nil
			}
			for _, l := range chunk {
				k, err := j.LeftKey(l)
				if err != nil {
					core.Fail(ctx, err)
					return nil, err
				}
				for _, r := range index[k] {
					out = append(out, Pair[L, R]{Left: l, Right: r})
				}
			}
		}
	}
}
