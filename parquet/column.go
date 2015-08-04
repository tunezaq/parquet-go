package parquet

import (
	"fmt"
	"io"
	"io/ioutil"

	"github.com/kostya-sh/parquet-go/parquetformat"
)

// TODO: add other commons methods
type ColumnChunkReader interface {
	Next() bool
	NextPage() bool
	Err() error
	// TODO: use smaller type, maybe byte?
	Repetition() int
	// TODO: use smaller type, maybe byte?
	Definition() int
	Value() interface{}
}

type countingReader struct {
	rs io.ReadSeeker
	n  int64
}

func (r *countingReader) Read(p []byte) (n int, err error) {
	n, err = r.rs.Read(p)
	r.n += int64(n)
	return
}

// TODO: shorter name?
type BooleanColumnChunkReader struct {
	// TODO: consider using a separate reader for each column chunk (no seeking)
	r *countingReader

	// initialized once
	fixedRepetition int
	fixedDefinition int
	totalSize       int64

	// changing state
	err             error
	isAtStartOfPage bool
	isAtLastPage    bool
	valuesRead      int64
	dataPageOffset  int64
	header          *parquetformat.PageHeader

	// encoder
	encoder booleanPlainEncoder
}

func NewBooleanColumnChunkReader(r io.ReadSeeker, schema *parquetformat.SchemaElement, chunk *parquetformat.ColumnChunk) (*BooleanColumnChunkReader, error) {
	if chunk.FilePath != nil {
		return nil, fmt.Errorf("data in another file: '%s'", *chunk.FilePath)
	}

	// chunk.FileOffset is useless
	// see https://issues.apache.org/jira/browse/PARQUET-291
	meta := chunk.MetaData
	if meta == nil {
		return nil, fmt.Errorf("missing ColumnMetaData")
	}

	if meta.Type != parquetformat.Type_BOOLEAN {
		return nil, fmt.Errorf("wrong type, expected BOOLEAN was %s", meta.Type)
	}

	if schema.RepetitionType == nil {
		return nil, fmt.Errorf("nil RepetitionType (root SchemaElement?)")
	}
	if *schema.RepetitionType != parquetformat.FieldRepetitionType_REQUIRED {
		panic("nyi (non-required columns)")
	}
	if len(meta.PathInSchema) != 1 {
		panic("nyi (nested columns)")
	}

	// uncompress
	if meta.Codec != parquetformat.CompressionCodec_UNCOMPRESSED {
		return nil, fmt.Errorf("unsupported compression codec: %s", meta.Codec)
	}

	// only REQUIRED non-neseted columns are supported for now
	// so definitionLevel = 1 and repetitionLevel = 1
	cr := BooleanColumnChunkReader{
		r:               &countingReader{rs: r},
		fixedRepetition: 1,
		fixedDefinition: 1,
		totalSize:       meta.TotalCompressedSize,
		dataPageOffset:  meta.DataPageOffset,
	}

	return &cr, nil
}

// IsAtStartOfPage returns true if the reader is positioned at the first value of a page.
func (cr *BooleanColumnChunkReader) IsAtStartOfPage() bool {
	return cr.isAtStartOfPage
}

// PageHeader returns page header of the current page.
func (cr *BooleanColumnChunkReader) PageHeader() *parquetformat.PageHeader {
	return cr.header
}

func (cr *BooleanColumnChunkReader) readDataPage() ([]byte, error) {
	var err error

	if _, err := cr.r.rs.Seek(cr.dataPageOffset, 0); err != nil {
		return nil, err
	}
	ph := parquetformat.PageHeader{}
	if err := ph.Read(cr.r); err != nil {
		return nil, err
	}
	if ph.Type != parquetformat.PageType_DATA_PAGE {
		return nil, fmt.Errorf("DATA_PAGE type expected, but was %s", ph.Type)
	}
	if ph.DataPageHeader == nil {
		return nil, fmt.Errorf("null DataPageHeader in %+v", ph)
	}
	cr.header = &ph

	dph := ph.DataPageHeader

	if dph.Encoding != parquetformat.Encoding_PLAIN {
		return nil, fmt.Errorf("unsupported encoding %s for BOOLEAN type", dph.Encoding)
	}

	size := int64(ph.CompressedPageSize)
	data, err := ioutil.ReadAll(io.LimitReader(cr.r, size))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) != size {
		return nil, fmt.Errorf("unable to read page fully")
	}

	if cr.r.n > cr.totalSize {
		return nil, fmt.Errorf("Over-read")
	}
	if cr.r.n == cr.totalSize {
		cr.isAtLastPage = true
	}
	cr.dataPageOffset += size

	return data, nil
}

// Next advances the reader to the next value, which then will be available
// through Boolean method. It returns false when the reading stops, either by
// reaching the end of the input or an error.
func (cr *BooleanColumnChunkReader) Next() bool {
	if cr.err != nil {
		return false
	}

	n := cr.encoder.data != nil && cr.encoder.next()
	if cr.encoder.err != nil {
		cr.err = cr.encoder.err
		return false
	}

	if !n {
		if cr.isAtLastPage {
			// TODO: may be set error if trying to read past the end of the column chunk
			return false
		}

		data, err := cr.readDataPage()
		if err != nil {
			cr.err = err
			return false
		}
		cr.encoder.init(data, cr.header.DataPageHeader.NumValues)
		cr.isAtStartOfPage = true

		n = cr.encoder.next()
		if cr.encoder.err != nil {
			cr.err = cr.encoder.err
			return false
		}
	} else {
		cr.isAtStartOfPage = false
	}

	cr.valuesRead++
	return n
}

// Skip behaves like Next but it may skip decoding the current value.
// TODO: think about use case and implement if necessary
func (cr *BooleanColumnChunkReader) Skip() bool {
	panic("nyi")
	return false
}

// NextPage advances the reader to the first value of the next page. It behaves as Next.
// TODO: think about how this method can be used to skip unreadable/corrupted pages
func (cr *BooleanColumnChunkReader) NextPage() bool {
	if cr.err != nil {
		return false
	}
	cr.isAtStartOfPage = true
	// TODO: implement
	return false
}

// Repetition returns repetition level of the most recent value generated by a call to Next or NextPage.
func (cr *BooleanColumnChunkReader) Repetition() int {
	if cr.fixedRepetition >= 0 {
		return cr.fixedRepetition
	}
	panic("nyi")
}

// Definition returns definition level of the most recent value generated by a call to Next or NextPage.
func (cr *BooleanColumnChunkReader) Definition() int {
	if cr.fixedDefinition >= 0 {
		return cr.fixedDefinition
	}
	panic("nyi")
}

// Boolean returns the most recent value generated by a call to Next or NextPage
func (cr *BooleanColumnChunkReader) Boolean() bool {
	return cr.encoder.value
}

// Value() = Boolean()
func (cr *BooleanColumnChunkReader) Value() interface{} {
	return cr.Boolean()
}

// Err returns the first non-EOF error that was encountered by the reader.
func (cr *BooleanColumnChunkReader) Err() error {
	return cr.err
}
