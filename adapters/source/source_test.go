package source_test

import (
	"context"
	"strings"
	"testing"

	"github.com/vacheslavterentev/go-stream/adapters/source"
)

func collectChunks[T any](t *testing.T, ch <-chan T) []T {
	t.Helper()
	var out []T
	for chunk := range ch {
		out = append(out, chunk)
	}
	return out
}

func TestSlice_Chunks(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	src := source.Slice[int]{Items: []int{1, 2, 3, 4, 5}, ChunkSize: 2}
	ch, err := src.Chunks(ctx)
	if err != nil {
		t.Fatalf("Chunks: %v", err)
	}

	chunks := collectChunks(t, ch)
	if len(chunks) != 3 {
		t.Fatalf("chunks len = %d, want 3", len(chunks))
	}
	if chunks[0].Len() != 2 || chunks[1].Len() != 2 || chunks[2].Len() != 1 {
		t.Fatalf("unexpected chunk sizes: %d, %d, %d", chunks[0].Len(), chunks[1].Len(), chunks[2].Len())
	}
}

func TestSlice_DefaultChunkSize(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	src := source.Slice[int]{Items: []int{1, 2, 3}}
	ch, err := src.Chunks(ctx)
	if err != nil {
		t.Fatalf("Chunks: %v", err)
	}

	chunks := collectChunks(t, ch)
	if len(chunks) != 1 || chunks[0].Len() != 3 {
		t.Fatalf("expected single chunk of 3, got %d chunks", len(chunks))
	}
}

func TestCSV_Chunks(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	input := "a,b\n1,2\n3,4\n"
	src := source.CSV{
		Reader:    strings.NewReader(input),
		ChunkSize: 2,
	}
	ch, err := src.Chunks(ctx)
	if err != nil {
		t.Fatalf("Chunks: %v", err)
	}

	chunks := collectChunks(t, ch)
	if len(chunks) != 2 {
		t.Fatalf("chunks len = %d, want 2", len(chunks))
	}
	if chunks[0][0][0] != "a" || chunks[0][1][0] != "1" {
		t.Fatalf("unexpected first chunk: %#v", chunks[0])
	}
	if chunks[1][0][0] != "3" {
		t.Fatalf("unexpected second chunk: %#v", chunks[1])
	}
}

func TestCSV_NilReader(t *testing.T) {
	t.Parallel()

	_, err := source.CSV{}.Chunks(context.Background())
	if err == nil {
		t.Fatal("expected error for nil reader")
	}
}
