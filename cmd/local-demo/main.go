package main

import (
	"context"
	"fmt"
	"log"

	"newCache/cache"
)

func main() {
	ctx := context.Background()
	loads := 0

	group := cache.NewGroup("users", 1024, cache.GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		loads++
		log.Printf("load from datasource: key=%s", key)
		return []byte("value:" + key), nil
	}))

	v, err := group.Get(ctx, "1001")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("first get: %s\n", v.String())

	v, err = group.Get(ctx, "1001")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("second get: %s\n", v.String())
	fmt.Printf("datasource loads: %d\n", loads)
}
