package core_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vacheslavterentev/go-stream/adapters/sink"
	"github.com/vacheslavterentev/go-stream/adapters/source"
	"github.com/vacheslavterentev/go-stream/operators"
)

func TestPipeline_Filter(t *testing.T) {
	ctx := context.Background()
	src := source.Slice[int]{Items: []int{1, 2, 3, 4, 5}, ChunkSize: 2}
	var out []int
	snk := &sink.Slice[int]{Items: &out}

	filter := operators.Filter[int]{
		Predicate: func(v int) (bool, error) {
			return v%2 == 0, nil
		},
	}

	in, err := src.Chunks(ctx)
	require.NoError(t, err)

	ch, err := filter.Apply(ctx, in)
	require.NoError(t, err)

	err = snk.Consume(ctx, ch)
	require.NoError(t, err)
	require.Equal(t, []int{2, 4}, out)
}
