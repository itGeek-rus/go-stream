package core

import "context"

// Stage преобразует поток фрагментов. Каждый этап владеет своим выходным каналом
// и должен закрыть его по завершении обработки.
type Stage[T, U any] interface {
	Apply(ctx context.Context, in <-chan Chunk[T]) (<-chan Chunk[U], error)
}
