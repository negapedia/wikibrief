package wikibrief

import (
	"encoding/xml"
	"io"
	"time"

	"github.com/pkg/errors"
)

// New returns a wikipedia dump page summarizer reading from the given reader.
func New(r io.Reader, isValidPage func(uint32) bool, weighter func(string) float64) func() (Summary, error) {
	base := bBase{xml.NewDecoder(r), isValidPage, weighter}
	return func() (s Summary, err error) {
		b := base.New()
		var t xml.Token
		for t, err = base.Decoder.Token(); err == nil; t, err = base.Decoder.Token() {
			switch xmlEvent(t) {
			case "page start":
				b, err = b.Start()
			case "title start":
				b, err = b.SetPageTitle(t.(xml.StartElement))
			case "id start":
				b, err = b.SetPageID(t.(xml.StartElement))
			case "revision start":
				b, err = b.AddRevision(t.(xml.StartElement))
			case "page end":
				b, s, err = b.End()
			default:
				err = b.Handle(t)
			}
			if err != nil || len(s.Revisions) > 0 {
				return
			}
		}
		return
	}
}

//Summary represents a page summary.
type Summary struct {
	Title     string
	PageID    uint32
	Revisions []Revision
}

//Revision represent a revision of a page.
type Revision struct {
	ID, UserID, IsRevert uint32
	Weight               float64
	Timestamp            time.Time
}

//AnonimousUserID is the UserID value assumed by revisions done by an anonimous user
const AnonimousUserID uint32 = 0

type builder interface {
	Start() (be builder, err error)
	SetPageTitle(t xml.StartElement) (be builder, err error)
	SetPageID(t xml.StartElement) (be builder, err error)
	AddRevision(t xml.StartElement) (be builder, err error)
	End() (be builder, s Summary, err error)
	Handle(t xml.Token) (err error)
}

//bBase is the base state builder //////////////////////////////////////////////////////////////////////////////////////////////
type bBase struct {
	Decoder     *xml.Decoder
	IsValidPage func(uint32) bool
	Weighter    func(string) float64
}

func (bs *bBase) New() builder {
	be := bBase(*bs)
	return &be
}

func (bs *bBase) Start() (be builder, err error) {
	be = &bStarted{*bs}
	return
}
func (bs *bBase) SetPageTitle(t xml.StartElement) (be builder, err error) {
	be = bs
	return
}
func (bs *bBase) SetPageID(t xml.StartElement) (be builder, err error) {
	be = bs
	return
}
func (bs *bBase) AddRevision(t xml.StartElement) (be builder, err error) {
	be = bs
	return
}
func (bs *bBase) End() (be builder, s Summary, err error) {
	be = bs
	return
}
func (bs *bBase) Handle(t xml.Token) (err error) {
	//Do nothing
	return
}

//bStarted is the state of the builder in which a new page start has been found////////////////////////////////////////////////////
type bStarted struct {
	bBase
}

func (bs *bStarted) Start() (be builder, err error) { //no page nesting
	err = errors.New("Error invalid xml (found tag <page> without matching </page>)")
	return
}
func (bs *bStarted) SetPageTitle(t xml.StartElement) (be builder, err error) {
	var title string
	if err = bs.Decoder.DecodeElement(&title, &t); err != nil {
		return
	}
	be = &bTitled{
		bStarted: *bs,
		Title:    title,
	}
	return
}
func (bs *bStarted) SetPageID(t xml.StartElement) (be builder, err error) { //no obligatory element "title"
	err = errors.New("Error invalid xml (not found obligatory element \"title\")")
	return
}
func (bs *bStarted) AddRevision(t xml.StartElement) (be builder, err error) { //no obligatory element "title"
	err = errors.New("Error invalid xml (not found obligatory element \"title\")")
	return
}
func (bs *bStarted) End() (be builder, s Summary, err error) { //no obligatory element "title"
	err = errors.New("Error invalid xml (not found obligatory element \"title\")")
	return
}
func (bs *bStarted) Handle(t xml.Token) (err error) {
	if _, ok := t.(xml.StartElement); ok {
		bs.Decoder.Skip() //Skipping not matching internal page elements
	}
	return
}

//bTitled is the state of the builder in which has been set a title for the page /////////////////////////////////////////////////
type bTitled struct {
	bStarted
	Title string
}

func (bs *bTitled) Start() (be builder, err error) { //no page nesting
	err = errors.New("Error invalid xml (found tag <page> without matching </page>)")
	return
}
func (bs *bTitled) SetPageTitle(t xml.StartElement) (be builder, err error) { ////////////////
	err = errors.New("Error invalid xml (found a page with two titles)")
	return
}
func (bs *bTitled) SetPageID(t xml.StartElement) (be builder, err error) {
	var pageID uint32
	if err = bs.Decoder.DecodeElement(&pageID, &t); err != nil {
		return
	}

	if bs.IsValidPage(pageID) {
		be = &bSummary{
			bTitled:       *bs,
			PageID:        pageID,
			SHA12SerialID: make(map[string]uint32, 16),
		}
	} else {
		bs.Decoder.Skip() //skip page
		be = bs.New()
	}
	return
}
func (bs *bTitled) AddRevision(t xml.StartElement) (be builder, err error) {
	//A page should contain an ID element, so we discard current page
	bs.Decoder.Skip() //skip page
	be = bs.New()
	return
}
func (bs *bTitled) End() (be builder, s Summary, err error) {
	//A page should contain at least one revision, so we discard current page
	be = bs.New()
	return
}

//bSummary is the state of the builder in which has been set a title and a page ID for the page //////////////////////////////////
type bSummary struct {
	bTitled
	PageID uint32

	revisions []Revision
	//SHA12SerialID maps sha1 to the last revision serial number in which it appears
	SHA12SerialID   map[string]uint32
	currentSerialID uint32
}

func (bs *bSummary) SetPageID(t xml.StartElement) (be builder, err error) {
	err = errors.New("Error invalid xml (found a page with two ids)")
	return
}

func (bs *bSummary) AddRevision(t xml.StartElement) (be builder, err error) {
	var r revision
	if err = bs.Decoder.DecodeElement(&r, &t); err != nil {
		return
	}

	//convert time
	var timestamp time.Time
	const layout = "2006-01-02T15:04:05Z"
	timestamp, err = time.Parse(layout, r.Timestamp)

	//count revisions reverted
	var isRevert uint32
	previousID, ok := bs.SHA12SerialID[r.SHA1]
	switch {
	case ok:
		isRevert = bs.currentSerialID - previousID - 1
		fallthrough
	case len(r.SHA1) == 31:
		bs.SHA12SerialID[r.SHA1] = bs.currentSerialID
		fallthrough
	default:
		bs.currentSerialID++
	}

	bs.revisions = append(bs.revisions, Revision{
		ID:        r.ID,
		UserID:    r.Contributor.ID,
		IsRevert:  isRevert,
		Weight:    bs.Weighter(r.Text),
		Timestamp: timestamp,
	})

	be = bs
	return
}
func (bs *bSummary) End() (be builder, s Summary, err error) {
	s = Summary{bs.Title, bs.PageID, bs.revisions}
	be = bs.New()
	return
}

// A page revision.
type revision struct {
	ID          uint32      `xml:"id"`
	Timestamp   string      `xml:"timestamp"`
	Contributor contributor `xml:"contributor"`
	Text        string      `xml:"text"`
	SHA1        string      `xml:"sha1"`
}

// A revision contributor.
type contributor struct {
	ID       uint32 `xml:"id"`
	Username string `xml:"username"`
	IP       string `xml:"ip"`
}

func xmlEvent(t xml.Token) string {
	switch elem := t.(type) {
	case xml.StartElement:
		return elem.Name.Local + " start"
	case xml.EndElement:
		return elem.Name.Local + " end"
	default:
		return ""
	}
}
