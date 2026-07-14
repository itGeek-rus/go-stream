package go_stream_test

import (
	"context"
	"errors"
	"sort"
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

func TestParallelMap(t *testing.T) {
	t.Parallel()

	out, err := go_stream.ParallelMap(
		go_stream.FromSlice([]int{1, 2, 3, 4}, go_stream.WithChunkSize(2)),
		3,
		func(v int) (int, error) { return v * 2, nil },
	).Collect(context.Background())
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}

	sort.Ints(out)
	want := []int{2, 4, 6, 8}
	if len(out) != len(want) {
		t.Fatalf("got %v, want %v", out, want)
	}
	for i := range want {
		if out[i] != want[i] {
			t.Fatalf("got %v, want %v", out, want)
		}
	}
}

func TestParallelMap_Error(t *testing.T) {
	t.Parallel()

	_, err := go_stream.ParallelMap(
		go_stream.FromSlice([]int{1, 2, 3}),
		2,
		func(v int) (int, error) {
			if v == 2 {
				return 0, errors.New("boom")
			}
			return v, nil
		},
	).Collect(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestGroupBy(t *testing.T) {
	t.Parallel()

	type row struct{ country string }
	items := []row{{"RU"}, {"US"}, {"RU"}, {"DE"}}

	groups, err := go_stream.GroupBy(
		context.Background(),
		go_stream.FromSlice(items),
		func(r row) (string, error) { return r.country, nil },
	)
	if err != nil {
		t.Fatalf("GroupBy: %v", err)
	}

	if len(groups["RU"]) != 2 || len(groups["US"]) != 1 || len(groups["DE"]) != 1 {
		t.Fatalf("unexpected groups: %#v", groups)
	}
}

func TestGroupBy_Error(t *testing.T) {
	t.Parallel()

	_, err := go_stream.GroupBy(
		context.Background(),
		go_stream.FromSlice([]int{1, 2, 3}),
		func(v int) (string, error) {
			if v == 2 {
				return "", errors.New("bad key")
			}
			return "ok", nil
		},
	)
	if err == nil {
		t.Fatal("expected error")
	}
}
