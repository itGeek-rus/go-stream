package sink_test

import (
	"context"
	"strings"
	"testing"

	"github.com/vacheslavterentev/go-stream/adapters/sink"
	"github.com/vacheslavterentev/go-stream/pipeline/core"
)

func TestCSV_Consume(t *testing.T) {
	t.Parallel()

	var buf strings.Builder
	snk := &sink.CSV{Writer: &buf}
	ch := make(chan core.Chunk[core.CSVRow], 1)
	ch <- core.Chunk[core.CSVRow]{core.CSVRow{"a", "b"}, core.CSVRow{"1", "2"}}
	close(ch)

	if err := snk.Consume(context.Background(), ch); err != nil {
		t.Fatalf("Consume: %v", err)
	}

	want := "a,b\n1,2\n"
	if buf.String() != want {
		t.Fatalf("got %q, want %q", buf.String(), want)
	}
}

func TestCSV_NilWriter(t *testing.T) {
	t.Parallel()

	ch := make(chan core.Chunk[core.CSVRow])
	close(ch)
	if err := (&sink.CSV{}).Consume(context.Background(), ch); err == nil {
		t.Fatal("expected error for nil writer")
	}
}
