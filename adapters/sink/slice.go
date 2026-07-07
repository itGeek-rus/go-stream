package sink

import (
	"context"
	"sync"

	"github.com/vacheslavterentev/go-stream/pipeline/core"
)

type Slice[T any] struct {
	mu    sync.Mutex
	Items *[]T
}

func (s *Slice[T]) Consume(ctx context.Context, in <-chan core.Chunk[T]) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case chunk, ok := <-in:
			if !ok {
				return nil
			}
			s.mu.Lock()
			*s.Items = append(*s.Items, chunk...)
			s.mu.Unlock()
		}
	}
}
