package go_stream_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	go_stream "github.com/vacheslavterentev/go-stream"
	"github.com/vacheslavterentev/go-stream/pipeline/core"
)

func TestFromSlice_Filter(t *testing.T) {
	t.Parallel()

	out, err := go_stream.FromSlice([]int{1, 2, 3, 4, 5}, go_stream.WithChunkSize(2)).
		Filter(func(v int) (bool, error) { return v%2 == 0, nil }).
		Collect(context.Background())
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}

	want := []int{2, 4}
	if len(out) != len(want) {
		t.Fatalf("got %v, want %v", out, want)
	}
	for i := range want {
		if out[i] != want[i] {
			t.Fatalf("got %v, want %v", out, want)
		}
	}
}

func TestFromSlice_Map(t *testing.T) {
	t.Parallel()

	out, err := go_stream.Map(
		go_stream.FromSlice([]int{1, 2, 3}),
		func(v int) (string, error) { return strings.Repeat("x", v), nil },
	).Collect(context.Background())
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}

	want := []string{"x", "xx", "xxx"}
	if len(out) != len(want) {
		t.Fatalf("got %v, want %v", out, want)
	}
	for i := range want {
		if out[i] != want[i] {
			t.Fatalf("got %v, want %v", out, want)
		}
	}
}

func TestFromSlice_StageError(t *testing.T) {
	t.Parallel()

	_, err := go_stream.FromSlice([]int{1}).
		Filter(func(int) (bool, error) { return false, errors.New("fail") }).
		Collect(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestFromCSV_FilterAndWriteCSV(t *testing.T) {
	t.Parallel()

	input := strings.NewReader("name,value\nfoo,1\nbar,2\nbaz,3\n")
	rows, err := go_stream.FromCSV(input, go_stream.WithChunkSize(2)).
		Filter(func(row core.CSVRow) (bool, error) {
			return len(row) > 0 && row[0] != "name", nil
		}).
		Collect(context.Background())
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}

	if len(rows) != 3 {
		t.Fatalf("rows len = %d, want 3", len(rows))
	}

	var buf strings.Builder
	if err := go_stream.WriteCSV(context.Background(),
		go_stream.FromSlice(rows, go_stream.WithChunkSize(10)),
		&buf,
	); err != nil {
		t.Fatalf("WriteCSV: %v", err)
	}

	want := "foo,1\nbar,2\nbaz,3\n"
	if buf.String() != want {
		t.Fatalf("got %q, want %q", buf.String(), want)
	}
}

func TestFromSlice_WithBufferSize(t *testing.T) {
	t.Parallel()

	out, err := go_stream.FromSlice(
		[]int{1, 2, 3, 4},
		go_stream.WithChunkSize(1),
		go_stream.WithBufferSize(2),
	).
		Filter(func(v int) (bool, error) { return true, nil }).
		Collect(context.Background())
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}

	if len(out) != 4 {
		t.Fatalf("got %d items, want 4", len(out))
	}
}
