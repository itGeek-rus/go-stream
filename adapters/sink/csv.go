package sink

import (
	"context"
	"encoding/csv"
	"io"

	"github.com/vacheslavterentev/go-stream/pipeline/core"
)

type CSV struct {
	Writer io.Writer
	Comma  rune
}

func (c *CSV) Consume(ctx context.Context, in <-chan core.Chunk[core.CSVRow]) error {
	if c.Writer == nil {
		return io.ErrClosedPipe
	}

	w := csv.NewWriter(c.Writer)
	if c.Comma != 0 {
		w.Comma = c.Comma
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case chunk, ok := <-in:
			if !ok {
				w.Flush()
				return w.Error()
			}
			for _, row := range chunk {
				if err := w.Write([]string(row)); err != nil {
					return err
				}
			}
		}
	}
}
