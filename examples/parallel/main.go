package main

import (
	"context"
	"fmt"
	"log"

	go_stream "github.com/vacheslavterentev/go-stream"
)

func main() {
	ctx := context.Background()

	unordered, err := go_stream.ParallelMap(
		go_stream.FromSlice([]int{1, 2, 3, 4, 5, 6, 7, 8}),
		4,
		func(v int) (int, error) { return v * v, nil },
	).Collect(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("parallel:", unordered)

	ordered, err := go_stream.OrderedParallelMap(
		go_stream.FromSlice([]int{1, 2, 3, 4, 5, 6, 7, 8}),
		4,
		func(v int) (int, error) { return v * v, nil },
	).Collect(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("ordered:", ordered)
}
