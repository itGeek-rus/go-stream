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

// LeftPair is the result of a left outer join.
// Ok is false when there is no matching right row.
type LeftPair[L, R any] struct {
	Left  L
	Right R
	Ok    bool
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
	index, err := buildRightIndex(ctx, right, j.RightKey)
	if err != nil {
		return nil, err
	}

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

// LeftJoin builds a hash index over the right stream, then emits all left rows.
// Unmatched left rows appear with Ok=false. The right stream is fully buffered in memory.
type LeftJoin[K comparable, L, R any] struct {
	LeftKey  func(L) (K, error)
	RightKey func(R) (K, error)
}

func (j LeftJoin[K, L, R]) Collect(
	ctx context.Context,
	left <-chan core.Chunk[L],
	right <-chan core.Chunk[R],
) ([]LeftPair[L, R], error) {
	index, err := buildRightIndex(ctx, right, j.RightKey)
	if err != nil {
		return nil, err
	}

	var out []LeftPair[L, R]
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
				rights := index[k]
				if len(rights) == 0 {
					var zero R
					out = append(out, LeftPair[L, R]{Left: l, Right: zero, Ok: false})
					continue
				}
				for _, r := range rights {
					out = append(out, LeftPair[L, R]{Left: l, Right: r, Ok: true})
				}
			}
		}
	}
}

func buildRightIndex[K comparable, R any](
	ctx context.Context,
	right <-chan core.Chunk[R],
	rightKey func(R) (K, error),
) (map[K][]R, error) {
	index := make(map[K][]R)
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case chunk, ok := <-right:
			if !ok {
				return index, nil
			}
			for _, item := range chunk {
				k, err := rightKey(item)
				if err != nil {
					core.Fail(ctx, err)
					return nil, err
				}
				index[k] = append(index[k], item)
			}
		}
	}
}
