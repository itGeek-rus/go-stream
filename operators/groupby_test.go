package operators_test

import (
	"context"
	"errors"
	"testing"

	"github.com/vacheslavterentev/go-stream/adapters/source"
	"github.com/vacheslavterentev/go-stream/operators"
	"github.com/vacheslavterentev/go-stream/pipeline/core"
)

func TestGroupBy_Collect(t *testing.T) {
	t.Parallel()

	type item struct{ cat string }

	ctx := context.Background()
	ch, err := source.Slice[item]{
		Items: []item{{"a"}, {"b"}, {"a"}, {"c"}, {"b"}},
	}.Chunks(ctx)
	if err != nil {
		t.Fatalf("Chunks: %v", err)
	}

	groups, err := operators.GroupBy[string, item]{
		KeyFn: func(i item) (string, error) { return i.cat, nil },
	}.Collect(ctx, ch)
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}

	if len(groups["a"]) != 2 || len(groups["b"]) != 2 || len(groups["c"]) != 1 {
		t.Fatalf("unexpected groups: %#v", groups)
	}
}

func TestGroupBy_KeyError(t *testing.T) {
	t.Parallel()

	ctx, fail := core.WithFailure(context.Background())
	ch, err := source.Slice[int]{Items: []int{1, 2, 3}}.Chunks(ctx)
	if err != nil {
		t.Fatalf("Chunks: %v", err)
	}

	_, err = operators.GroupBy[string, int]{
		KeyFn: func(v int) (string, error) {
			if v == 2 {
				return "", errors.New("bad key")
			}
			return "ok", nil
		},
	}.Collect(ctx, ch)
	if err == nil {
		t.Fatal("expected error")
	}

	_ = fail()
}
