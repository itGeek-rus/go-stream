# go-stream

**GoStream** — типобезопасная ETL-библиотека для Go. Позволяет строить потоковые пайплайны обработки данных с минимальным потреблением памяти, используя дженерики (Go 1.18+) и чанковую передачу через каналы.

## Особенности

- **Безопасная типизация** — операции `Map`, `Filter`, `ParallelMap`, `OrderedParallelMap`, `GroupBy`, `Join`, `LeftJoin` параметризованы типами, поддерживаются цепочки `T → U`.
- **Потоковая обработка** — данные читаются и обрабатываются чанками, не загружая весь набор в память.
- **Параллелизм** — `ParallelMap` без гарантии порядка; `OrderedParallelMap` сохраняет порядок входа.
- **Join** — inner и left outer hash-join (правый stream целиком в памяти).
- **Чистая архитектура** — источники и приёмники отделены от логики обработки; легко подменять адаптеры.
- **Готовые адаптеры** — slice, CSV (`encoding/csv`), Parquet (`parquet-go`).
- **Управление контекстом** — поддержка `context.Context` для отмены, таймаутов и fail-fast при ошибках в stages.

## Архитектура

```
Source → [Filter | Map | ParallelMap | OrderedParallelMap] → Sink
                         ↘ GroupBy / Join / LeftJoin (терминальные)
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

### Parquet

```go
type Row struct {
    ID   int64  `parquet:"id"`
    Name string `parquet:"name"`
}

err = go_stream.WriteParquet(ctx, go_stream.FromSlice(rows), writer)

out, err := go_stream.FromParquet[Row](readerAt, size).Collect(ctx)
```

### ParallelMap / OrderedParallelMap

```go
// Порядок элементов не гарантируется
out, err := go_stream.ParallelMap(
    go_stream.FromSlice([]int{1, 2, 3, 4, 5, 6}),
    4, // workers
    func(v int) (int, error) { return v * v, nil },
).Collect(ctx)

// Порядок входа сохраняется
ordered, err := go_stream.OrderedParallelMap(
    go_stream.FromSlice([]int{1, 2, 3, 4, 5, 6}),
    4,
    func(v int) (int, error) { return v * v, nil },
).Collect(ctx)
```

### GroupBy

```go
groups, err := go_stream.GroupBy(
    ctx,
    go_stream.FromSlice(items),
    func(item Item) (string, error) { return item.Category, nil },
)
// map[string][]Item
```

### Join / LeftJoin

```go
pairs, err := go_stream.Join(
    ctx,
    leftStream,
    rightStream, // полностью буферизуется в памяти
    func(l Left) (int, error) { return l.ID, nil },
    func(r Right) (int, error) { return r.LeftID, nil },
)
// []operators.Pair[Left, Right]

leftPairs, err := go_stream.LeftJoin(ctx, leftStream, rightStream, leftKey, rightKey)
// []operators.LeftPair[Left, Right] — Ok=false если нет match
```

Опции: `WithChunkSize(n)`, `WithBufferSize(n)` — размер чанка и буфер канала для backpressure.

## Установка

```bash
go get github.com/vacheslavterentev/go-stream
```

## Разработка

```bash
task cleancode   # tidy, vet, lint, gosec, vuln, test, build, examples
go test -race -timeout 60s ./...
go build ./examples/...
```

Нужны локально: `golangci-lint`, `gosec`, `govulncheck`, `task`.

### Git hooks (lefthook)

```bash
# macOS
brew install lefthook

# или
go install github.com/evilmartians/lefthook@latest

lefthook install   # один раз после клонирования
```

Pre-commit запускает `task cleancode`.
