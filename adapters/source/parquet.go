package source

import (
	"context"
	"fmt"
	"io"

	"github.com/parquet-go/parquet-go"
	"github.com/vacheslavterentev/go-stream/pipeline/core"
)

const defaultParquetChunkSize = 100

// Parquet reads rows of type T (struct with parquet tags / exported fields).
type Parquet[T any] struct {
	Reader     io.ReaderAt
	Size       int64
	ChunkSize  int
	BufferSize int
}

func (p Parquet[T]) Chunks(ctx context.Context) (<-chan core.Chunk[T], error) {
	if p.Reader == nil {
		return nil, fmt.Errorf("parquet source: reader is nil")
	}
	if p.Size <= 0 {
		return nil, fmt.Errorf("parquet source: size must be > 0")
	}

	chunkSize := p.ChunkSize
	if chunkSize <= 0 {
		chunkSize = defaultParquetChunkSize
	}

	pf, err := parquet.OpenFile(p.Reader, p.Size)
	if err != nil {
		return nil, fmt.Errorf("parquet open: %w", err)
	}

	rows := parquet.NewGenericReader[T](pf)
	out := make(chan core.Chunk[T], core.BufferSize(p.BufferSize))

	go func() {
		defer close(out)
		defer func() { _ = rows.Close() }()

		buf := make([]T, chunkSize)
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			n, err := rows.Read(buf)
			if n > 0 {
				chunk := make(core.Chunk[T], n)
				copy(chunk, buf[:n])
				select {
				case <-ctx.Done():
					return
				case out <- chunk:
				}
			}
			if err == io.EOF || (n == 0 && err == nil) {
				return
			}
			if err != nil {
				core.Fail(ctx, err)
				return
			}
		}
	}()

	return out, nil
}
