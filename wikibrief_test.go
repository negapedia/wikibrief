package wikibrief

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"reflect"
	"testing"

	"github.com/ebonetti/ctxutils"

	"github.com/negapedia/wikibots"
)

func TestUnit(t *testing.T) {
	ctx, fail := ctxutils.WithFail(context.Background())
	for p := range New(ctx, fail, "/tmp", "zu") {
		go func(p EvolvingPage) {
			for range p.Revisions {
				//Do nothing
			}
		}(p)
	}

	if err := fail(nil); err != nil {
		panic(err)
	}
}

func TestRun(t *testing.T) {
	b, err := base64.StdEncoding.DecodeString(holyGrail)
	if err != nil {
		t.Error("Error in holyGrail encoding", err)
	}

	ctx := context.Background()
	ID2Bot, err := wikibots.New(ctx, "en")
	if err != nil {
		t.Error(err)
	}

	ch := make(chan simpleEvolvingPage)
	go func() {
		defer close(ch)
		err := run(ctx, bBase{xml.NewDecoder(bytes.NewBuffer(b)), func(uint32) (uint32, bool) { return 0, true }, ID2Bot, ch, &errorContext{0, "holyGrail"}})
		if err != nil {
			t.Error(err)
		}
	}()

	var s Summary
	for page := range ch {
		s.PageID = page.PageID
		for r := range page.Revisions {
			s.Revisions = append(s.Revisions, r)
		}
	}

	var st Summary
	bSummary, err := base64.StdEncoding.DecodeString(encodedHolyGrailSummary)
	errt := json.Unmarshal(bSummary, &st)

	switch {
	case err != nil:
		t.Error("Error in holyGrail summary encoding", err)
	case errt != nil:
		t.Error("Error while loading summary test info", errt)
	case !reflect.DeepEqual(s, st):
		t.Error("Error different summaries")
	}
}

type Summary struct {
	PageID    uint32
	Revisions []Revision
}
