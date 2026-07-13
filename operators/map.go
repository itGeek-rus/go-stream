package operators

import (
	"context"

	"github.com/vacheslavterentev/go-stream/pipeline/core"
)

type Map[T, U any] struct {
	Fn         func(T) (U, error)
	BufferSize int
}

func (m Map[T, U]) Apply(ctx context.Context, in <-chan core.Chunk[T]) (<-chan core.Chunk[U], error) {
	out := make(chan core.Chunk[U], core.BufferSize(m.BufferSize))

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
				mapped := make(core.Chunk[U], 0, len(chunk))
				for _, item := range chunk {
					v, err := m.Fn(item)
					if err != nil {
						core.Fail(ctx, err)
						return
					}
					mapped = append(mapped, v)
				}
				select {
				case <-ctx.Done():
					return
				case out <- mapped:
				}
			}
		}
	}()

	return out, nil
}
