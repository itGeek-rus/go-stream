package core

import (
	"context"
	"fmt"
)

// Pipeline wires source → stages → sink.
type Pipeline[T any] struct {
	source Source[T]
	stages []stageAny
}

type stageAny interface {
	apply(ctx context.Context, in any) (any, error)
}

type typedStage[T, U any] struct {
	stage Stage[T, U]
}

func (s typedStage[T, U]) apply(ctx context.Context, in any) (any, error) {
	ch, ok := in.(<-chan Chunk[T])
	if !ok {
		return nil, fmt.Errorf("pipeline: stage type mismatch, expected <-chan Chunk[%T]", *new(T))
	}
	return s.stage.Apply(ctx, ch)
}

// NewPipeline creates a pipeline from source.
func NewPipeline[T any](src Source[T]) *Pipeline[T] {
	return &Pipeline[T]{source: src}
}

// Through appends a transformation stage.
func Through[T, U any](p *Pipeline[T], stage Stage[T, U]) *Pipeline[U] {
	stages := append(append([]stageAny(nil), p.stages...), typedStage[T, U]{stage: stage})
	return &Pipeline[U]{
		source: nil, // only root has source
		stages: stages,
	}
}

// Run executes source -> stages -> sink
func Run[T any](ctx context.Context, p *Pipeline[T], src Source[T], sink Sink[T]) error {
	in, err := src.Chunks(ctx)
	if err != nil {
		return fmt.Errorf("source: %w", err)
	}

	current := any(in)
	stages := p.stages
	if src == nil && len(stages) == 0 {
		return fmt.Errorf("pipeline: nothing to run")
	}

	for i, st := range stages {
		current, err = st.apply(ctx, current)
		if err != nil {
			return fmt.Errorf("stage %d: %w", i, err)
		}
	}

	out, ok := current.(<-chan Chunk[T])
	if !ok {
		return fmt.Errorf("pipeline: final stage output type mismatch")
	}

	if err := sink.Consume(ctx, out); err != nil {
		return fmt.Errorf("sink: %w", err)
	}
	return nil
}
