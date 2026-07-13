package source

import (
	"context"

	"github.com/vacheslavterentev/go-stream/pipeline/core"
)

type Slice[T any] struct {
	Items     []T
	ChunkSize int
	BufferSize int
}

func (s Slice[T]) Chunks(ctx context.Context) (<-chan core.Chunk[T], error) {
	chunkSize := s.ChunkSize
	if chunkSize <= 0 {
		chunkSize = len(s.Items)
		if chunkSize == 0 {
			chunkSize = 1
		}
	}

	out := make(chan core.Chunk[T], core.BufferSize(s.BufferSize))

	go func() {
		defer close(out)
		for i := 0; i < len(s.Items); i += chunkSize {
			select {
			case <-ctx.Done():
				return
			default:
			}
			end := i + chunkSize
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
