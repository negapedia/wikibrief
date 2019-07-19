package wikibrief

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/negapedia/wikibots"

	errorsOnSteroids "github.com/pkg/errors"
)

//EvolvingPage represents a wikipedia page that is being edited. Revisions is closed when there are no more revisions.
type EvolvingPage struct {
	PageID    uint32
	Revisions <-chan Revision
}

//Revision represents a revision of a page.
type Revision struct {
	ID, UserID uint32
	IsBot      bool
	Text, SHA1 string
	IsRevert   uint32
	Timestamp  time.Time
}

//Transform digest a wikipedia dump into the output stream. OutStream will not be closed by Transform.
func Transform(ctx context.Context, lang string, r io.Reader, isValid func(pageID uint32) bool, outStream chan<- EvolvingPage) (err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	ID2Bot, err := wikibots.New(ctx, lang)
	if err != nil {
		return
	}

	filename := ""
	if namer, ok := r.(interface{ Name() string }); ok {
		filename = namer.Name()
	}

	base := bBase{xml.NewDecoder(r), isValid, ID2Bot, outStream, &errorContext{0, filename}}
	b := base.New()

	defer func() { //Error handling
		b.End() //Close eventually open revision channel
		causer, errHasCause := err.(interface{ Cause() error })
		switch {
		case err == io.EOF:
			err = nil
		case errHasCause && causer.Cause() != nil:
			//do nothing
		default:
			err = b.Wrapf(err, "Unexpected error in outer event loop")
		}
	}()

	var t xml.Token
	for t, err = base.Decoder.Token(); err == nil; t, err = base.Decoder.Token() {
		switch xmlEvent(t) {
		case "page start":
			b, err = b.Start()
		case "id start":
			b, err = b.NewPage(ctx, t.(xml.StartElement))
		case "revision start":
			b, err = b.NewRevision(ctx, t.(xml.StartElement))
		case "page end":
			b, err = b.End()
		}
	}

	return
}

//AnonimousUserID is the UserID value assumed by revisions done by an anonimous user
const AnonimousUserID uint32 = 0

var errInvalidXML = errors.New("Invalid XML")

type builder interface {
	Start() (be builder, err error)
	NewPage(ctx context.Context, t xml.StartElement) (be builder, err error)
	NewRevision(ctx context.Context, t xml.StartElement) (be builder, err error)
	End() (be builder, err error)
	Wrapf(err error, format string, args ...interface{}) error
}

/////////////////////////////////////////////////////////////////////////////////////

//bBase is the base state builder

type bBase struct {
	Decoder      *xml.Decoder
	IsValid      func(pageID uint32) bool
	ID2Bot       func(userID uint32) (username string, ok bool)
	OutStream    chan<- EvolvingPage
	ErrorContext *errorContext
}

func (bs *bBase) New() builder {
	be := bBase(*bs)
	return &be
}

func (bs *bBase) Start() (be builder, err error) {
	be = &bStarted{*bs}
	return
}
func (bs *bBase) NewPage(ctx context.Context, t xml.StartElement) (be builder, err error) {
	err = bs.Wrapf(errInvalidXML, "Error invalid xml (not found obligatory element \"page\" before \"id\")")
	return
}
func (bs *bBase) NewRevision(ctx context.Context, t xml.StartElement) (be builder, err error) {
	err = bs.Wrapf(errInvalidXML, "Error invalid xml (not found obligatory element \"page\" before \"revision\")")
	return
}
func (bs *bBase) End() (be builder, err error) {
	err = bs.Wrapf(errInvalidXML, "Error invalid xml (not found obligatory element \"page\" start before end)")
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
	err = bs.Wrapf(errInvalidXML, "Error invalid xml (found nested element page)")
	return
}
func (bs *bStarted) NewPage(ctx context.Context, t xml.StartElement) (be builder, err error) {
	var pageID uint32
	if err = bs.Decoder.DecodeElement(&pageID, &t); err != nil {
		err = bs.Wrapf(err, "Error while decoding page ID")
		return
	}

	bs.ErrorContext.LastPageID = pageID //used for error reporting purposes

	if bs.IsValid(pageID) {
		revisions := make(chan Revision, 10)
		select {
		case <-ctx.Done():
			err = bs.Wrapf(ctx.Err(), "Context cancelled")
			return
		case bs.OutStream <- EvolvingPage{pageID, revisions}:
			be = &bSetted{
				bStarted:      *bs,
				Revisions:     revisions,
				SHA12SerialID: map[string]uint32{},
			}
			return
		}
	}

	if err = bs.Decoder.Skip(); err != nil {
		err = bs.Wrapf(err, "Error while skipping page")
		return
	}

	be = bs.New()
	return
}
func (bs *bStarted) NewRevision(ctx context.Context, t xml.StartElement) (be builder, err error) { //no obligatory element "id"
	err = bs.Wrapf(errInvalidXML, "Error invalid xml (found a page revision without finding previous page ID)")
	return
}
func (bs *bStarted) End() (be builder, err error) { //no obligatory element "id"
	err = bs.Wrapf(errInvalidXML, "Error invalid xml (found a page end without finding previous page ID)")
	return
}
func (bs *bStarted) Wrapf(err error, format string, args ...interface{}) error {
	return errorsOnSteroids.Wrapf(err, format+" - %v", append(args, bs.ErrorContext)...)
}

/////////////////////////////////////////////////////////////////////////////////////

//bSetted is the state of the builder in which has been set a page ID for the page
type bSetted struct {
	bStarted

	Revisions     chan Revision
	RevisionCount uint32
	SHA12SerialID map[string]uint32
}

func (bs *bSetted) Start() (be builder, err error) { //no page nesting
	close(bs.Revisions)
	err = bs.Wrapf(errInvalidXML, "Error invalid xml (found nested element page)")
	return
}
func (bs *bSetted) NewPage(ctx context.Context, t xml.StartElement) (be builder, err error) {
	close(bs.Revisions)
	err = bs.Wrapf(errInvalidXML, "Error invalid xml (found a page with two ids)")
	return
}
func (bs *bSetted) NewRevision(ctx context.Context, t xml.StartElement) (be builder, err error) {
	defer func() {
		if err != nil {
			close(bs.Revisions)
		}
	}()

	//parse revision
	var r revision
	if err = bs.Decoder.DecodeElement(&r, &t); err != nil {
		err = bs.Wrapf(err, "Error while decoding the %vth revision", bs.RevisionCount+1)
		return
	}

	//Calculate reverts
	serialID, IsRevert := bs.RevisionCount, uint32(0)
	oldSerialID, isRevert := bs.SHA12SerialID[r.SHA1]
	switch {
	case isRevert:
		IsRevert = serialID - (oldSerialID + 1)
		fallthrough
	case len(r.SHA1) == 31:
		bs.SHA12SerialID[r.SHA1] = serialID
	}

	//convert time
	const layout = "2006-01-02T15:04:05Z"
	timestamp, err := time.Parse(layout, r.Timestamp)
	if err != nil {
		err = bs.Wrapf(err, "Error while decoding the timestamp %s of %vth revision", r.Timestamp, bs.RevisionCount+1)
		return
	}
	r.Timestamp = ""

	//Check if userID represents bot
	_, isBot := bs.ID2Bot(r.UserID)

	bs.RevisionCount++

	select {
	case <-ctx.Done():
		err = bs.Wrapf(ctx.Err(), "Context cancelled")
	case bs.Revisions <- Revision{r.ID, r.UserID, isBot, r.Text, r.SHA1, IsRevert, timestamp}:
		be = bs
	}

	return
}
func (bs *bSetted) End() (be builder, err error) {
	close(bs.Revisions)
	be = bs.New()
	return
}

// A page revision.
type revision struct {
	ID        uint32 `xml:"id"`
	Timestamp string `xml:"timestamp"`
	UserID    uint32 `xml:"contributor>id"`
	Text      string `xml:"text"`
	SHA1      string `xml:"sha1"`
	//converted data
	timestamp time.Time
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
