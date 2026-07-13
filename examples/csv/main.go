package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	go_stream "github.com/vacheslavterentev/go-stream"
	"github.com/vacheslavterentev/go-stream/pipeline/core"
)

func main() {
	in := strings.NewReader("name,value\nfoo,1\nbar,2\n")
	rows, err := go_stream.FromCSV(in, go_stream.WithChunkSize(10)).
		Filter(func(row core.CSVRow) (bool, error) {
			return len(row) > 0 && row[0] != "name", nil
		}).
		Collect(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	var buf strings.Builder
	stream := go_stream.FromSlice(rows, go_stream.WithChunkSize(10))
	if err := go_stream.WriteCSV(context.Background(), stream, &buf); err != nil {
		log.Fatal(err)
	}
	fmt.Fprint(os.Stdout, buf.String())
}
