package core

import "context"

// Source генерирует фрагменты данных. По завершении работы канал должен быть закрыт.
// Реализации должны учитывать отмену ctx.
type Source[T any] interface {
	Chunks(ctx context.Context) (<-chan Chunk[T], error)
}
