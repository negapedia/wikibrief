package wikibrief

import (
	"context"
	"math/rand"

	errorsOnSteroids "github.com/pkg/errors"
)

//FanOut creates n copies of the given EvolvingPage channel; if n < 1 panics.
func FanOut(ctx context.Context, in <-chan EvolvingPage, n int) (out []<-chan EvolvingPage) {
	switch {
	case n < 1:
		panic(errorsOnSteroids.New("fanout: len out of range"))
	case n == 1:
		return []<-chan EvolvingPage{in}
	}

	var result []chan<- EvolvingPage
	for i := 0; i < n; i++ {
		ch := make(chan EvolvingPage, pageBufferSize)
		result = append(result, ch)
		out = append(out, ch)
	}

	go fanOutPages(ctx, in, result)

	return out
}

func fanOutPages(ctx context.Context, in <-chan EvolvingPage, out []chan<- EvolvingPage) {
	defer func() {
		for _, ch := range out {
			close(ch)
		}
	}()
	for {
		//Get the next Page
		var p EvolvingPage
		var ok bool
		select {
		case p, ok = <-in:
			if !ok { //channnel closed
				return
			}
		case <-ctx.Done():
			return
		}

		var revisions []chan Revision
		for range out {
			revisions = append(revisions, make(chan Revision, revisionBufferSize))
		}

		go fanOutRevisions(ctx, p.Revisions, revisions)

		//out channel and page iterator, pseudo-randomly selected
		stillWaiting := rand.Perm(len(out))
		next := func() (ch chan<- EvolvingPage, page EvolvingPage) {
			if len(stillWaiting) == 0 {
				return
			}

			page = p
			page.Revisions = revisions[stillWaiting[0]]
			ch, stillWaiting = out[stillWaiting[0]], stillWaiting[1:]

			return
		}

		//Send the pages: iterate the same number of times as the number of channel to send to
		out1, page1 := next()
		out2, page2 := next()
		out3, page3 := next()
		for range out {
			select {
			case out1 <- page1:
				out1, page1 = next()
			case out2 <- page2:
				out2, page2 = next()
			case out3 <- page3:
				out3, page3 = next()
			case <-ctx.Done():
				return
			}
		}
	}
}

func fanOutRevisions(ctx context.Context, in <-chan Revision, out []chan Revision) {
	defer func() {
		for _, ch := range out {
			close(ch)
		}
	}()
	for {
		//Get the next revision
		var r Revision
		var ok bool
		select {
		case r, ok = <-in:
			if !ok { //channnel closed
				return
			}
		case <-ctx.Done():
			return
		}

		//out channel iterator, pseudo-randomly selected
		stillWaiting := rand.Perm(len(out))
		nextChannel := func() (ch chan<- Revision) {
			if len(stillWaiting) == 0 {
				return
			}

			ch, stillWaiting = out[stillWaiting[0]], stillWaiting[1:]
			return
		}

		//Send the revision: iterate the same number of times as the number of channel to send to
		out1, out2, out3 := nextChannel(), nextChannel(), nextChannel()
		for range out {
			select {
			case out1 <- r:
				out1 = nextChannel()
			case out2 <- r:
				out2 = nextChannel()
			case out3 <- r:
				out3 = nextChannel()
			case <-ctx.Done():
				return
			}
		}
	}
}
