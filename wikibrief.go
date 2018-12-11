package wikibrief

import (
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"time"

	errorsOnSteroids "github.com/pkg/errors"
)

// New returns a wikipedia dump page summarizer reading from the given reader.
func New(r io.Reader, isValidPage func(uint32) bool, weighter func(string) float64) func() (Summary, error) {
	filename := ""
	if namer, ok := r.(interface{ Name() string }); ok {
		filename = namer.Name()
	}

	base := bBase{xml.NewDecoder(r), isValidPage, weighter, errorContext{0, filename}}
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
				break
			}
		}

		causer, errHasCause := err.(interface{ Cause() error })
		switch {
		case err == nil:
			//do nothing
		case err == io.EOF:
			//do nothing
		case errHasCause && causer.Cause() != nil:
			//do nothing
		default:
			err = b.Wrapf(err, "Unexpected error in outer event loop")
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
	ID, UserID uint32
	Weight     float64
	SHA1       string
	Timestamp  time.Time
}

//AnonimousUserID is the UserID value assumed by revisions done by an anonimous user
const AnonimousUserID uint32 = 0

var errInvalidXML = errors.New("Invalid XML")

type builder interface {
	Start() (be builder, err error)
	SetPageTitle(t xml.StartElement) (be builder, err error)
	SetPageID(t xml.StartElement) (be builder, err error)
	AddRevision(t xml.StartElement) (be builder, err error)
	End() (be builder, s Summary, err error)
	Handle(t xml.Token) (err error)
	Wrapf(err error, format string, args ...interface{}) error
}

/////////////////////////////////////////////////////////////////////////////////////

//bBase is the base state builder

type bBase struct {
	Decoder      *xml.Decoder
	IsValidPage  func(uint32) bool
	Weighter     func(string) float64
	ErrorContext errorContext
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

func (bs *bBase) Wrapf(err error, format string, args ...interface{}) error {
	return errorsOnSteroids.Wrapf(err, format+" - %v", append(args, bs.ErrorContext)...)
}

/////////////////////////////////////////////////////////////////////////////////////

//bStarted is the state of the builder in which a new page start has been found
type bStarted struct {
	bBase
}

func (bs *bStarted) Start() (be builder, err error) { //no page nesting
	err = bs.Wrapf(errInvalidXML, "Error invalid xml (found tag <page> without matching </page>)")
	return
}
func (bs *bStarted) SetPageTitle(t xml.StartElement) (be builder, err error) {
	var title string
	if err = bs.Decoder.DecodeElement(&title, &t); err != nil {
		err = bs.Wrapf(err, "Error while decoding the title of a page")
		return
	}
	be = &bTitled{
		bStarted: *bs,
		Title:    title,
	}
	return
}
func (bs *bStarted) SetPageID(t xml.StartElement) (be builder, err error) { //no obligatory element "title"
	err = bs.Wrapf(errInvalidXML, "Error invalid xml (not found obligatory element \"title\")")
	return
}
func (bs *bStarted) AddRevision(t xml.StartElement) (be builder, err error) { //no obligatory element "title"
	err = bs.Wrapf(errInvalidXML, "Error invalid xml (not found obligatory element \"title\")")
	return
}
func (bs *bStarted) End() (be builder, s Summary, err error) { //no obligatory element "title"
	err = bs.Wrapf(errInvalidXML, "Error invalid xml (not found obligatory element \"title\")")
	return
}
func (bs *bStarted) Handle(t xml.Token) (err error) {
	if _, ok := t.(xml.StartElement); ok {
		bs.Decoder.Skip() //Skipping not matching internal page elements
	}
	return
}

func (bs *bStarted) Wrapf(err error, format string, args ...interface{}) error {
	return errorsOnSteroids.Wrapf(err, format+" - %v", append(args, bs.ErrorContext)...)
}

/////////////////////////////////////////////////////////////////////////////////////

//bTitled is the state of the builder in which has been set a title for the page
type bTitled struct {
	bStarted
	Title string
}

func (bs *bTitled) Start() (be builder, err error) { //no page nesting
	err = bs.Wrapf(errInvalidXML, "Error invalid xml (found tag <page> without matching </page>)")
	return
}
func (bs *bTitled) SetPageTitle(t xml.StartElement) (be builder, err error) {
	err = bs.Wrapf(errInvalidXML, "Error invalid xml (found a page with two titles)")
	return
}
func (bs *bTitled) SetPageID(t xml.StartElement) (be builder, err error) {
	var pageID uint32
	if err = bs.Decoder.DecodeElement(&pageID, &t); err != nil {
		err = bs.Wrapf(err, "Error while decoding page ID in page '%s'", bs.Title)
		return
	}

	if bs.IsValidPage(pageID) {
		be = &bSummary{
			bTitled: *bs,
			PageID:  pageID,
		}
	} else {
		bs.Decoder.Skip() //skip page
		be = bs.New()
	}

	bs.ErrorContext.LastPageID = pageID //used for error reporting purposes
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

func (bs *bTitled) Wrapf(err error, format string, args ...interface{}) error {
	return errorsOnSteroids.Wrapf(err, format+" - current page \"%s\", %v", append(args, bs.Title, bs.ErrorContext)...)
}

/////////////////////////////////////////////////////////////////////////////////////

//bSummary is the state of the builder in which has been set a title and a page ID for the page
type bSummary struct {
	bTitled
	PageID uint32

	revisions []revision
}

func (bs *bSummary) SetPageID(t xml.StartElement) (be builder, err error) {
	err = bs.Wrapf(errInvalidXML, "Error invalid xml (found a page with two ids)")
	return
}

func (bs *bSummary) AddRevision(t xml.StartElement) (be builder, err error) {
	var r revision
	if err = bs.Decoder.DecodeElement(&r, &t); err != nil {
		err = bs.Wrapf(err, "Error while decoding the %vth revision", len(bs.revisions)+2)
		return
	}

	//convert time
	const layout = "2006-01-02T15:04:05Z"
	r.timestamp, err = time.Parse(layout, r.Timestamp)
	err = bs.Wrapf(err, "Error while decoding the timestamp %s of %vrd revision", r.timestamp, len(bs.revisions)+2)
	r.Timestamp = ""

	//weight text
	r.weight, r.Text = bs.Weighter(r.Text), ""

	bs.revisions = append(bs.revisions, r)

	be = bs
	return
}
func (bs *bSummary) End() (be builder, s Summary, err error) {
	be = bs.New()

	sort.Sort(byParentIDAndTime(bs.revisions)) //Wikipedia BUG: in the dump some edits are not sorted.
	rr := make([]Revision, len(bs.revisions))
	for i, r := range bs.revisions {
		rr[i] = Revision{r.ID, r.Contributor.ID, r.weight, r.SHA1, r.timestamp}
	}
	s = Summary{bs.Title, bs.PageID, rr}

	return
}

func (bs *bSummary) Wrapf(err error, format string, args ...interface{}) error {
	return errorsOnSteroids.Wrapf(err, format+" - current page \"%s\" %v", append(args, bs.Title, bs.ErrorContext)...)
}

// A page revision.
type revision struct {
	ID          uint32      `xml:"id"`
	ParentID    uint32      `xml:"parentid"`
	Timestamp   string      `xml:"timestamp"`
	Contributor contributor `xml:"contributor"`
	Text        string      `xml:"text"`
	SHA1        string      `xml:"sha1"`
	//converted data
	timestamp time.Time
	weight    float64
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

type byParentIDAndTime []revision

func (p byParentIDAndTime) Len() int {
	return len(p)
}

func (p byParentIDAndTime) Less(i, j int) bool {
	ri, rj := p[i], p[j]
	switch {
	case ri.ID == rj.ParentID:
		return true
	case ri.ParentID == rj.ID:
		return false
	}
	return ri.timestamp.Before(rj.timestamp)
}

func (p byParentIDAndTime) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

type errorContext struct {
	LastPageID uint32 //used for error reporting purposes
	Filename   string //used for error reporting purposes
}

func (ec errorContext) String() string {
	report := fmt.Sprintf("last page ID %v in \"%s\"", ec.LastPageID, ec.Filename)
	if _, err := os.Stat(ec.Filename); os.IsNotExist(err) {
		report += " - WARNING: file not found!"
	}
	return report
}
