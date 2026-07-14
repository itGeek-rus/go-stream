package operators

import (
	"context"

	"github.com/vacheslavterentev/go-stream/pipeline/core"
)

type GroupBy[K comparable, T any] struct {
	KeyFn func(T) (K, error)
}

func (g GroupBy[K, T]) Collect(ctx context.Context, in <-chan core.Chunk[T]) (map[K][]T, error) {
	groups := make(map[K][]T)

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case chunk, ok := <-in:
			if !ok {
				return groups, nil
			}
			for _, item := range chunk {
				k, err := g.KeyFn(item)
				if err != nil {
					core.Fail(ctx, err)
					return nil, err
				}
				groups[k] = append(groups[k], item)
			}
		}
	}
}
