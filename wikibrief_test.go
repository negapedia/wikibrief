package wikibrief

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"reflect"
	"sync"
	"testing"

	"github.com/ebonetti/ctxutils"

	"github.com/negapedia/wikibots"
)

func TestUnit(t *testing.T) {
	ctx, fail := ctxutils.WithFail(context.Background())
	pages := New(ctx, fail, "/tmp", "zu", true)
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
		t.Fatalf("%+v", err)
	}
}

func TestRun(t *testing.T) {
	b, err := base64.StdEncoding.DecodeString(holyGrail)
	if err != nil {
		t.Fatalf("Error in holyGrail encoding %+v", err)
	}

	ctx := context.Background()
	ID2Bot, err := wikibots.New(ctx, "en")
	if err != nil {
		t.Fatalf("%+v", err)
	}

	ch := make(chan simpleEvolvingPage)
	go func() {
		defer close(ch)
		err := run(ctx, bBase{xml.NewDecoder(bytes.NewBuffer(b)), func(uint32) (uint32, bool) { return 0, true }, ID2Bot, ch, &errorContext{0, "holyGrail"}})
		if err != nil {
			t.Fatalf("%+v", err)
		}
	}()

	var s summary
	for page := range ch {
		s.PageID = page.PageID
		for r := range page.Revisions {
			s.Revisions = append(s.Revisions, r)
		}
	}

	var st summary
	bSummary, err := base64.StdEncoding.DecodeString(encodedHolyGrailSummary)
	errt := json.Unmarshal(bSummary, &st)

	switch {
	case err != nil:
		t.Fatalf("Error in holyGrail summary encoding %+v", err)
	case errt != nil:
		t.Fatalf("Error while loading summary test info %+v", err)
	case !reflect.DeepEqual(s, st):
		t.Fatal("Error different summaries")
	}
}

type summary struct {
	PageID    uint32
	Revisions []Revision
}
