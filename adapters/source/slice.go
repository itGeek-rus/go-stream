package source

import (
	"context"

	"github.com/vacheslavterentev/go-stream/pipeline/core"
)

type Slice[T any] struct {
	Items     []T
	ChunkSize int
}

func (s Slice[T]) Chunks(ctx context.Context) (<-chan core.Chunk[T], error) {
	if s.ChunkSize <= 0 {
		s.ChunkSize = len(s.Items)
		if s.ChunkSize == 0 {
			s.ChunkSize = 1
		}
	}

	out := make(chan core.Chunk[T])

	go func() {
		defer close(out)
		for i := 0; i < len(s.Items); i += s.ChunkSize {
			select {
			case <-ctx.Done():
				return
			default:
			}
			end := i + s.ChunkSize
			if end > len(s.Items) {
				end = len(s.Items)
			}
			chunk := core.Chunk[T](s.Items[i:end])
			select {
			case <-ctx.Done():
				return
			case out <- chunk:
			}
		}
	}()

	return out, nil
}
