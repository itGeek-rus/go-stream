package main

import (
	"context"
	"fmt"
	"log"

	go_stream "github.com/vacheslavterentev/go-stream"
)

func main() {
	out, err := go_stream.FromSlice([]int{1, 2, 3, 4, 5, 6},
		go_stream.WithChunkSize(2),
	).
		Filter(func(v int) (bool, error) { return v%2 == 0, nil }).
		Collect(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(out)
}
