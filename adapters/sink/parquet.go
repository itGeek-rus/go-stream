package sink

import (
	"context"
	"fmt"
	"io"

	"github.com/parquet-go/parquet-go"
	"github.com/vacheslavterentev/go-stream/pipeline/core"
)

type Parquet[T any] struct {
	Writer io.Writer
}

func (p *Parquet[T]) Consume(ctx context.Context, in <-chan core.Chunk[T]) error {
	if p.Writer == nil {
		return fmt.Errorf("parquet sink: writer is nil")
	}

	w := parquet.NewGenericWriter[T](p.Writer)

	for {
		select {
		case <-ctx.Done():
			_ = w.Close()
			return ctx.Err()
		case chunk, ok := <-in:
			if !ok {
				return w.Close()
			}
			if _, err := w.Write([]T(chunk)); err != nil {
				_ = w.Close()
				return err
			}
		}
	}
}
