package parallel

import (
	"context"
	"sync"

	"github.com/vacheslavterentev/go-stream/pipeline/core"
)

// Map обрабатывает элементы чанков параллельно. Порядок результатов не гарантируется.
type Map[T, U any] struct {
	Workers    int
	Fn         func(T) (U, error)
	BufferSize int
}

func (m Map[T, U]) Apply(ctx context.Context, in <-chan core.Chunk[T]) (<-chan core.Chunk[U], error) {
	workers := m.Workers
	if workers <= 0 {
		workers = 1
	}

	jobs := make(chan T, core.BufferSize(m.BufferSize))
	out := make(chan core.Chunk[U], core.BufferSize(m.BufferSize))

	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for item := range jobs {
				if ctx.Err() != nil {
					return
				}
				v, err := m.Fn(item)
				if err != nil {
					core.Fail(ctx, err)
					return
				}
				select {
				case <-ctx.Done():
					return
				case out <- core.Chunk[U]{v}:
				}
			}
		}()
	}

	go func() {
		defer func() {
			close(jobs)
			wg.Wait()
			close(out)
		}()

		for {
			select {
			case <-ctx.Done():
				return
			case chunk, ok := <-in:
				if !ok {
					return
				}
				for _, item := range chunk {
					select {
					case <-ctx.Done():
						return
					case jobs <- item:
					}
				}
			}
		}
	}()

	return out, nil
}
