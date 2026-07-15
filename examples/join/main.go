package main

import (
	"context"
	"fmt"
	"log"

	go_stream "github.com/vacheslavterentev/go-stream"
)

type user struct {
	id   int
	name string
}

type order struct {
	userID int
	amount int
}

func main() {
	users := go_stream.FromSlice([]user{
		{1, "alice"},
		{2, "bob"},
		{3, "carol"},
	})
	orders := go_stream.FromSlice([]order{
		{1, 100},
		{1, 50},
		{2, 200},
		{9, 1},
	})

	pairs, err := go_stream.Join(
		context.Background(),
		users,
		orders,
		func(u user) (int, error) { return u.id, nil },
		func(o order) (int, error) { return o.userID, nil },
	)
	if err != nil {
		log.Fatal(err)
	}

	for _, p := range pairs {
		fmt.Printf("%s -> %d\n", p.Left.name, p.Right.amount)
	}
}
