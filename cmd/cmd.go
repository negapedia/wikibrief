package main

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/ebonetti/ctxutils"
	"github.com/negapedia/wikibrief"
)

func main() {
	start := time.Now()
	defer func() {
		log.Println("Time elapsed since start: ", time.Since(start))
	}()

	ctx, fail := ctxutils.WithFail(context.Background())
	pages := wikibrief.New(ctx, fail, "/tmp", "it")
	wg := sync.WaitGroup{}
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for p := range pages {
				for range p.Revisions {
					//Do nothing
				}
			}
		}()
	}
	wg.Wait()

	if err := fail(nil); err != nil {
		panic(err)
	}
}
