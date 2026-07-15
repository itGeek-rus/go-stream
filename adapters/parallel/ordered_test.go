package parallel_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/vacheslavterentev/go-stream/adapters/parallel"
	"github.com/vacheslavterentev/go-stream/adapters/source"
	"github.com/vacheslavterentev/go-stream/pipeline/core"
)

func TestOrderedMap_PreservesOrder(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ch, err := source.Slice[int]{Items: []int{1, 2, 3, 4, 5}, ChunkSize: 2}.Chunks(ctx)
	if err != nil {
		t.Fatalf("Chunks: %v", err)
	}

	out, err := parallel.OrderedMap[int, int]{
		Workers: 3,
		Fn: func(v int) (int, error) {
			// Make later items finish first to force reordering without OrderedMap.
			time.Sleep(time.Duration(6-v) * time.Millisecond)
			return v * 10, nil
		},
	}.Apply(ctx, ch)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	var got []int
	for chunk := range out {
		got = append(got, chunk...)
	}

	want := []int{10, 20, 30, 40, 50}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}

func TestOrderedMap_Error(t *testing.T) {
	t.Parallel()

	ctx, fail := core.WithFailure(context.Background())
	ch, err := source.Slice[int]{Items: []int{1, 2, 3}, ChunkSize: 1}.Chunks(ctx)
	if err != nil {
		t.Fatalf("Chunks: %v", err)
	}

	out, err := parallel.OrderedMap[int, int]{
		Workers: 2,
		Fn: func(v int) (int, error) {
			if v == 2 {
				return 0, errors.New("boom")
			}
			return v, nil
		},
	}.Apply(ctx, ch)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	for range out {
	}

	if err := fail(); err == nil {
		t.Fatal("expected stage error")
	}
}
