package operators

import (
	"context"

	"github.com/vacheslavterentev/go-stream/pipeline/core"
)

type Filter[T any] struct {
	Predicate func(T) (bool, error)
}

func (f Filter[T]) Apply(ctx context.Context, in <-chan core.Chunk[T]) (<-chan core.Chunk[T], error) {
	out := make(chan core.Chunk[T])

	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case chunk, ok := <-in:
				if !ok {
					return
				}
				filtered := make(core.Chunk[T], 0, len(chunk))
				for _, item := range chunk {
					keep, err := f.Predicate(item)
					if err != nil {
						return
					}
					if keep {
						filtered = append(filtered, item)
					}
				}
				if len(filtered) == 0 {
					continue
				}
				select {
				case <-ctx.Done():
					return
				case out <- filtered:
				}
			}
		}
	}()

	return out, nil
}
