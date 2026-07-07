package core

import "context"

// Sink потребляет фрагменты до тех пор, пока входной канал не будет закрыт.
type Sink[T any] interface {
	Consume(ctx context.Context, in <-chan Chunk[T]) error
}
