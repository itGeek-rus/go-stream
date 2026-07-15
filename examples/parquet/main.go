package main

import (
	"bytes"
	"context"
	"fmt"
	"log"

	go_stream "github.com/vacheslavterentev/go-stream"
)

type row struct {
	ID   int64  `parquet:"id"`
	Name string `parquet:"name"`
}

func main() {
	ctx := context.Background()
	input := []row{{1, "alice"}, {2, "bob"}}

	var buf bytes.Buffer
	if err := go_stream.WriteParquet(ctx, go_stream.FromSlice(input), &buf); err != nil {
		log.Fatal(err)
	}

	out, err := go_stream.FromParquet[row](bytes.NewReader(buf.Bytes()), int64(buf.Len())).
		Collect(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(out)
}
