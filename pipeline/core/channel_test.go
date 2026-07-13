package core_test

import (
	"testing"

	"github.com/vacheslavterentev/go-stream/pipeline/core"
)

func TestBufferSize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   int
		want int
	}{
		{name: "positive", in: 4, want: 4},
		{name: "zero", in: 0, want: 0},
		{name: "negative", in: -1, want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := core.BufferSize(tt.in); got != tt.want {
				t.Fatalf("BufferSize(%d) = %d, want %d", tt.in, got, tt.want)
			}
		})
	}
}
