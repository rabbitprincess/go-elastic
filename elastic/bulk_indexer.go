package elastic

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	es "github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
)

type BulkService struct {
	client *es.Client

	pretty     bool        // pretty format the returned JSON response
	human      bool        // return human readable values for statistics
	errorTrace bool        // include the stack trace of returned errors
	filterPath []string    // list of filters used to reduce the response
	headers    http.Header // custom request-level HTTP headers

	index               string
	requests            []BulkableRequest
	pipeline            string
	timeout             time.Duration
	refresh             string
	routing             string
	waitForActiveShards string

	// estimated bulk size in bytes, up to the request index sizeInBytesCursor
	sizeInBytes       int64
	sizeInBytesCursor int
}

// NewBulkService initializes a new BulkService.
func NewBulkService(client *es.Client) *BulkService {
	builder := &BulkService{
		client: client,
	}
	return builder
}

// Reset cleans up the request queue
func (s *BulkService) Reset() {
	s.requests = make([]BulkableRequest, 0)
	s.sizeInBytes = 0
	s.sizeInBytesCursor = 0
}

// Add adds bulkable requests, i.e. BulkIndexRequest, BulkUpdateRequest,
// and/or BulkDeleteRequest.
func (s *BulkService) Add(requests ...BulkableRequest) *BulkService {
	s.requests = append(s.requests, requests...)
	return s
}

// EstimatedSizeInBytes returns the estimated size of all bulkable
// requests added via Add.
func (s *BulkService) EstimatedSizeInBytes() int64 {
	if s.sizeInBytesCursor == len(s.requests) {
		return s.sizeInBytes
	}
	for _, r := range s.requests[s.sizeInBytesCursor:] {
		s.sizeInBytes += s.estimateSizeInBytes(r)
		s.sizeInBytesCursor++
	}
	return s.sizeInBytes
}

// estimateSizeInBytes returns the estimates size of the given
// bulkable request, i.e. BulkIndexRequest, BulkUpdateRequest, and
// BulkDeleteRequest.
func (s *BulkService) estimateSizeInBytes(r BulkableRequest) int64 {
	lines, _ := r.Source()
	size := 0
	for _, line := range lines {
		// +1 for the \n
		size += len(line) + 1
	}
	return int64(size)
}

// NumberOfActions returns the number of bulkable requests that need to
// be sent to Elasticsearch on the next batch.
func (s *BulkService) NumberOfActions() int {
	return len(s.requests)
}

func (s *BulkService) bodyAsString() (string, error) {
	// Pre-allocate to reduce allocs
	var buf strings.Builder
	buf.Grow(int(s.EstimatedSizeInBytes()))

	for _, req := range s.requests {
		source, err := req.Source()
		if err != nil {
			return "", err
		}
		for _, line := range source {
			buf.WriteString(line)
			buf.WriteByte('\n')
		}
	}

	return buf.String(), nil
}

// Do sends the batched requests to Elasticsearch. Note that, when successful,
// you can reuse the BulkService for the next batch as the list of bulk
// requests is cleared on success.
func (s *BulkService) Do(ctx context.Context) (*BulkResponse, error) {
	// No actions?
	if s.NumberOfActions() == 0 {
		return nil, errors.New("elastic: No bulk actions to commit")
	}

	// Get body
	body, err := s.bodyAsString()
	if err != nil {
		return nil, err
	}

	req := esapi.BulkRequest{
		Index: s.index,
		// DocumentType: s.typ,
		Body: bytes.NewReader([]byte(body)),

		Pipeline: s.pipeline,
		Refresh:  s.refresh,
		Routing:  s.routing,
		// Source:              s.Source,
		// SourceExcludes:      s.SourceExcludes,
		// SourceIncludes:      s.SourceIncludes,
		Timeout:             s.timeout,
		WaitForActiveShards: s.waitForActiveShards,

		Pretty:     s.pretty,
		Human:      s.human,
		ErrorTrace: s.errorTrace,
		FilterPath: s.filterPath,
		Header:     s.headers,
	}

	// Add Header and MetaHeader to config if not already set
	if req.Header == nil {
		req.Header = http.Header{}
	}
	req.Header.Set(elasticsearch.HeaderClientMeta, "h=bp")

	res, err := req.Do(ctx, s.client)
	resBulk := new(BulkResponse)
	err = HandleResp(res, err, resBulk)
	if err != nil {
		return nil, err
	}

	// Reset so the request can be reused
	s.Reset()

	return resBulk, nil
}
