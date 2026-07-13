# go-stream

**GoStream** — типобезопасная ETL-библиотека для Go. Позволяет строить потоковые пайплайны обработки данных с минимальным потреблением памяти, используя дженерики (Go 1.18+) и чанковую передачу через каналы.

## Особенности

- **Безопасная типизация** — операции `Map` и `Filter` параметризованы типами, поддерживаются цепочки `T → U`.
- **Потоковая обработка** — данные читаются и обрабатываются чанками, не загружая весь набор в память.
- **Чистая архитектура** — источники и приёмники отделены от логики обработки; легко подменять адаптеры.
- **Готовые адаптеры** — slice (для тестов и in-memory), CSV (stdlib `encoding/csv`).
- **Управление контекстом** — поддержка `context.Context` для отмены, таймаутов и fail-fast при ошибках в stages.

## Архитектура

```
Source → [Filter | Map] → Sink
```

- Данные идут чанками (`Chunk[T]`) через каналы
- Каждый stage работает в отдельной горутине и уважает `context.Context`
- Ошибка в stage останавливает весь pipeline (fail-fast)

## Пример

```go
package main

import (
    "context"
    "fmt"

    go_stream "github.com/vacheslavterentev/go-stream"
)

func main() {
    out, err := go_stream.FromSlice([]int{1, 2, 3, 4, 5},
        go_stream.WithChunkSize(2),
    ).
        Filter(func(v int) (bool, error) { return v%2 == 0, nil }).
        Collect(context.Background())
    if err != nil {
        panic(err)
    }
    fmt.Println(out) // [2 4]
}
```

### CSV

```go
import (
    "github.com/vacheslavterentev/go-stream/pipeline/core"
)

rows, err := go_stream.FromCSV(reader, go_stream.WithChunkSize(100)).
    Filter(func(row core.CSVRow) (bool, error) { return row[0] != "header", nil }).
    Collect(ctx)

err = go_stream.WriteCSV(ctx, go_stream.FromSlice(rows), writer)
```

Опции: `WithChunkSize(n)`, `WithBufferSize(n)` — размер чанка и буфер канала для backpressure.

## Установка

```bash
go get github.com/vacheslavterentev/go-stream
```

## Разработка

```bash
task cleancode   # tidy, vet, lint, test, build
go test -race ./...
```