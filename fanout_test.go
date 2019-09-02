package wikibrief

import (
	"context"
	"math/rand"
	"sync"
	"testing"
)

func TestFanOut(t *testing.T) {
	for _, s := range []struct {
		nPages uint32
		nOut   int
	}{{1, 1}, {500, 50}, {50, 500}} {
		testFanOutSetting(t, s.nPages, s.nOut)
	}
}

func testFanOutSetting(t *testing.T, nPages uint32, nOut int) {
	pagesTest := generateSuperSimplePages(nPages)
	outs := FanOut(context.Background(), superSimplePages2EvolvingPages(pagesTest), nOut)

	wg := sync.WaitGroup{}
	for _, out := range outs {
		wg.Add(1)
		go func(in <-chan EvolvingPage) {
			defer wg.Done()
			checkEquality(t, in, pagesTest)
		}(out)
	}
	wg.Wait()
}

func checkEquality(t *testing.T, in <-chan EvolvingPage, testPages []superSimplePage) {
	for _, tPage := range testPages {
		page, ok := <-in
		switch {
		case !ok:
			t.Error("Expecting pageID", tPage.PageID, "found closed channel")
		case page.PageID != tPage.PageID:
			t.Error("Expecting pageID", tPage.PageID, "found", page.PageID)
		}

		for _, tRevisionID := range tPage.RevisionIDs {
			revision, ok := <-page.Revisions
			switch {
			case !ok:
				t.Error("Expecting revisionID", tRevisionID, "found closed channel")
			case revision.ID != tRevisionID:
				t.Error("Expecting revisionID", tRevisionID, "found", revision.ID)
			}
		}
		if _, ok := <-page.Revisions; ok {
			t.Error("Expecting closed revisions channel")
		}
	}
	if _, ok := <-in; ok {
		t.Error("Expecting closed page channel")
	}
}

func superSimplePages2EvolvingPages(pages []superSimplePage) <-chan EvolvingPage {
	out := make(chan EvolvingPage, len(pages))
	defer close(out)
	for _, p := range pages {
		revisions := make(chan Revision, len(p.RevisionIDs))
		for _, revisionID := range p.RevisionIDs {
			revisions <- Revision{ID: revisionID}
		}
		close(revisions)
		out <- EvolvingPage{PageID: p.PageID, Revisions: revisions}
	}
	return out
}

func generateSuperSimplePages(nPages uint32) (pages []superSimplePage) {
	for pageID := uint32(1); pageID <= nPages; pageID++ {
		revisionIDs := make([]uint32, pageID)
		for _, ID := range rand.Perm(int(pageID)) {
			revisionIDs = append(revisionIDs, uint32(ID))
		}
		pages = append(pages, superSimplePage{pageID, revisionIDs})
	}
	return
}

type superSimplePage struct {
	PageID      uint32
	RevisionIDs []uint32
}
