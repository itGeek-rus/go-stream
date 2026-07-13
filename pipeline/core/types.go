package core

// Chunk - это пакет элементов, проходящий по конвейеру.
// Обработка по частям обеспечивает баланс между использованием памяти и пропускной способностью.
type Chunk[T any] []T

// CSVRow is a single CSV record (one row).
type CSVRow []string

func (c Chunk[T]) Len() int {
	return len(c)
}
