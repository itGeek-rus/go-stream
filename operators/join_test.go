package operators_test

import (
	"context"
	"errors"
	"testing"

	"github.com/vacheslavterentev/go-stream/adapters/source"
	"github.com/vacheslavterentev/go-stream/operators"
	"github.com/vacheslavterentev/go-stream/pipeline/core"
)

func TestInnerJoin(t *testing.T) {
	t.Parallel()

	type user struct{ id int }
	type order struct {
		userID int
		amount int
	}

	ctx := context.Background()
	users, err := source.Slice[user]{Items: []user{{1}, {2}}, ChunkSize: 1}.Chunks(ctx)
	if err != nil {
		t.Fatalf("users Chunks: %v", err)
	}
	orders, err := source.Slice[order]{
		Items:     []order{{1, 10}, {1, 20}, {3, 5}},
		ChunkSize: 2,
	}.Chunks(ctx)
	if err != nil {
		t.Fatalf("orders Chunks: %v", err)
	}

	pairs, err := operators.InnerJoin[int, user, order]{
		LeftKey:  func(u user) (int, error) { return u.id, nil },
		RightKey: func(o order) (int, error) { return o.userID, nil },
	}.Collect(ctx, users, orders)
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}

	if len(pairs) != 2 {
		t.Fatalf("got %d pairs, want 2: %#v", len(pairs), pairs)
	}
	for _, p := range pairs {
		if p.Left.id != 1 {
			t.Fatalf("unexpected left user: %#v", p)
		}
		if p.Right.userID != 1 {
			t.Fatalf("unexpected right order: %#v", p)
		}
	}
}

func TestInnerJoin_KeyError(t *testing.T) {
	t.Parallel()

	ctx, fail := core.WithFailure(context.Background())
	left, err := source.Slice[int]{Items: []int{1}}.Chunks(ctx)
	if err != nil {
		t.Fatalf("left Chunks: %v", err)
	}
	right, err := source.Slice[int]{Items: []int{1}}.Chunks(ctx)
	if err != nil {
		t.Fatalf("right Chunks: %v", err)
	}

	_, err = operators.InnerJoin[int, int, int]{
		LeftKey: func(v int) (int, error) { return v, nil },
		RightKey: func(v int) (int, error) {
			return 0, errors.New("bad right key")
		},
	}.Collect(ctx, left, right)
	if err == nil {
		t.Fatal("expected error")
	}
	_ = fail()
}
