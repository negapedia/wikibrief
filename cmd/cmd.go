package main

import (
	"context"
	"log"
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
	for p := range wikibrief.New(ctx, fail, "/tmp", "it") {
		go func(p wikibrief.EvolvingPage) {
			for range p.Revisions {
				//Do nothing
			}
		}(p)
	}

	if err := fail(nil); err != nil {
		panic(err)
	}
}
