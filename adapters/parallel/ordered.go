package parallel

import (
	"context"
	"sync"

	"github.com/vacheslavterentev/go-stream/pipeline/core"
)

type orderedJob[T any] struct {
	idx  int
	item T
}

type orderedResult[U any] struct {
	idx int
	val U
	err error
}

// OrderedMap обрабатывает элементы параллельно и сохраняет порядок входа.
type OrderedMap[T, U any] struct {
	Workers    int
	Fn         func(T) (U, error)
	BufferSize int
}

func (m OrderedMap[T, U]) Apply(ctx context.Context, in <-chan core.Chunk[T]) (<-chan core.Chunk[U], error) {
	workers := m.Workers
	if workers <= 0 {
		workers = 1
	}
	buf := core.BufferSize(m.BufferSize)

	jobs := make(chan orderedJob[T], buf)
	results := make(chan orderedResult[U], buf)
	out := make(chan core.Chunk[U], buf)

	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				if ctx.Err() != nil {
					return
				}
				v, err := m.Fn(j.item)
				select {
				case <-ctx.Done():
					return
				case results <- orderedResult[U]{idx: j.idx, val: v, err: err}:
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	go func() {
		defer close(jobs)

		next := 0
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
					case jobs <- orderedJob[T]{idx: next, item: item}:
						next++
					}
				}
			}
		}
	}()

	go func() {
		defer close(out)

		pending := make(map[int]U)
		nextEmit := 0

		for {
			select {
			case <-ctx.Done():
				return
			case r, ok := <-results:
				if !ok {
					return
				}
				if r.err != nil {
					core.Fail(ctx, r.err)
					return
				}
				pending[r.idx] = r.val
				for {
					v, ok := pending[nextEmit]
					if !ok {
						break
					}
					delete(pending, nextEmit)
					select {
					case <-ctx.Done():
						return
					case out <- core.Chunk[U]{v}:
						nextEmit++
					}
				}
			}
		}
	}()

	return out, nil
}
