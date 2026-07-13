package source

import (
	"context"
	"encoding/csv"
	"errors"
	"io"

	"github.com/vacheslavterentev/go-stream/pipeline/core"
)

const defaultCSVChunkSize = 100

type CSV struct {
	Reader     io.Reader
	ChunkSize  int
	BufferSize int
	Comma      rune
}

func (c CSV) Chunks(ctx context.Context) (<-chan core.Chunk[core.CSVRow], error) {
	if c.Reader == nil {
		return nil, errors.New("csv source: reader is nil")
	}

	chunkSize := c.ChunkSize
	if chunkSize <= 0 {
		chunkSize = defaultCSVChunkSize
	}

	r := csv.NewReader(c.Reader)
	if c.Comma != 0 {
		r.Comma = c.Comma
	}

	out := make(chan core.Chunk[core.CSVRow], core.BufferSize(c.BufferSize))

	go func() {
		defer close(out)
		chunk := make(core.Chunk[core.CSVRow], 0, chunkSize)

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			record, err := r.Read()
			if err == io.EOF {
				if len(chunk) > 0 {
					select {
					case <-ctx.Done():
						return
					case out <- chunk:
					}
				}
				return
			}
			if err != nil {
				core.Fail(ctx, err)
				return
			}

			chunk = append(chunk, core.CSVRow(record))
			if len(chunk) >= chunkSize {
				select {
				case <-ctx.Done():
					return
				case out <- chunk:
				}
				chunk = make(core.Chunk[core.CSVRow], 0, chunkSize)
			}
		}
	}()

	return out, nil
}
