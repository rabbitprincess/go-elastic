package elastic

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	doc "github.com/aergoio/aergo-indexer/indexer/documents"
	es "github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/elastic/go-elasticsearch/v8/esutil"
)

var (
	ELASTIC_USERNAME      = "elastic"
	ELASTIC_PASSWORD      = "-nCP=DZkYnlLYw3zXPRw"
	ELASTIC_CERT_FILEPATH = "./http_ca.crt"
)

func NewElasticClient(esURL string) (*es.Client, error) {
	cert, _ := ioutil.ReadFile(ELASTIC_CERT_FILEPATH)
	cfg := es.Config{
		Addresses: []string{
			esURL,
		},
		Username: ELASTIC_USERNAME,
		Password: ELASTIC_PASSWORD,
		CACert:   cert,
	}
	es, err := es.NewClient(cfg)
	if err != nil {
		return nil, err
	}
	return es, nil
}

func NewElasticsearchDbController(esURL string) (*ElasticsearchDbController2, error) {
	client, err := NewElasticClient(esURL)
	if err != nil {
		return nil, err
	}
	return &ElasticsearchDbController2{
		Client: client,
	}, nil
}

var _ DbController = &ElasticsearchDbController2{}

type ElasticsearchDbController2 struct {
	Client *es.Client
}

func (e *ElasticsearchDbController2) Info() (map[string]interface{}, error) {
	res, err := e.Client.Info()
	var info map[string]interface{}
	if err = HandleResp(res, err, &info); err != nil {
		return nil, err
	}
	return info, nil
}

func (e *ElasticsearchDbController2) IsConflict(err interface{}) bool {
	switch e := err.(type) {
	case *http.Response:
		return e.StatusCode == http.StatusConflict
	case *esapi.Response:
		return e.StatusCode == http.StatusConflict
	case esapi.Response:
		return e.StatusCode == http.StatusConflict
	case int:
		return e == http.StatusConflict
	}
	return false
}

func (e *ElasticsearchDbController2) CreateIndex(indexName string, documentType string) error {
	// 인덱스 추가 ( body - es mapping 과 함께 )
	res, err := e.Client.Indices.Create(
		indexName,
	)
	if err = HandleResp(res, err, nil); err != nil {
		return err
	}
	esBodyMapping := bytes.NewReader([]byte(EsMappings[documentType]))
	res, err = e.Client.Indices.PutMapping([]string{indexName}, esBodyMapping)
	if err = HandleResp(res, err, nil); err != nil {
		return err
	}

	return nil
}

// UpdateAlias updates an alias with a new index name and delete stale indices
func (e *ElasticsearchDbController2) UpdateAlias(aliasName string, indexName string) error {
	// 1. 모든 indices 조회
	res, err := e.Client.Indices.Get([]string{"_all"})
	aliacesRes := new(AliasesResult)
	if err = HandleResp(res, err, aliacesRes); err != nil {
		return err
	}
	indices := aliacesRes.IndicesByAlias(aliasName)

	// 2. indices 내 alias 제거
	if len(indices) > 0 {
		res, err := e.Client.Indices.DeleteAlias(indices, []string{aliasName})
		if err = HandleResp(res, err, nil); err != nil {
			return err
		}
	}

	// 3. alias 추가
	res, err = e.Client.Indices.PutAlias([]string{indexName}, aliasName)
	if err = HandleResp(res, err, nil); err != nil {
		return err
	}

	// 4. old indices 제거
	if len(indices) > 0 {
		res, err = e.Client.Indices.Delete(indices)
		if err = HandleResp(res, err, nil); err != nil {
			return err
		}
	}
	return nil
}

func (e *ElasticsearchDbController2) Insert(doc doc.DocType, params UpdateParams) (uint64, error) {
	body, err := json.Marshal(doc)
	if err != nil {
		return 0, err
	}

	var res *esapi.Response
	if params.Upsert == true {
		res, err = e.Client.Update(params.IndexName, doc.GetID(), bytes.NewReader(body))
	} else {
		res, err = e.Client.Create(params.IndexName, doc.GetID(), bytes.NewReader(body))
	}
	if err = HandleResp(res, err, nil); err != nil {
		return 0, err
	}
	return 1, nil
}

func (e *ElasticsearchDbController2) InsertBulk(documentChannel chan doc.DocType, params UpdateParams) (uint64, error) {
	ctx := context.Background()

	bulkIndexer, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Client: e.Client,
		Index:  params.IndexName,
		// NumWorkers: 10, // the number of worker goroutines ( default : number of CPUs )
		// FlushBytes: 5e+6, // The flush threshold in bytes (default: 5M)
	})
	if err != nil {
		return 0, err
	}

	var Action string
	if params.Upsert == true {
		Action = "update"
	} else {
		Action = "create"
	}

	for d := range documentChannel {
		body, err := json.Marshal(d)
		if err != nil {
			return 0, err
		}

		if err = bulkIndexer.Add(ctx, esutil.BulkIndexerItem{
			Action:     Action,                // Action field configures the operation to perform (index, create, delete, update)
			DocumentID: d.GetID(),             // DocumentID is the optional document ID
			Body:       bytes.NewReader(body), // Body is an `io.Reader` with the payload
			OnSuccess: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem) {
				fmt.Printf("[%d] %s test/%s", res.Status, res.Result, item.DocumentID)
			},
			OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
				log.Printf("ERROR: %s | %s: %s\n", err, res.Error.Type, res.Error.Reason)
			},
		}); err != nil {
			return 0, err
		}

		select {
		default:
		case <-ctx.Done():
			stats := bulkIndexer.Stats()
			if stats.NumFailed > 0 {
				return 0, fmt.Errorf("Indexed [%d] documents with [%d] errors", stats.NumFlushed, stats.NumFailed)
			}
			total := stats.NumAdded + stats.NumUpdated
			return total, ctx.Err()
		}
	}

	err = bulkIndexer.Close(ctx)
	if err != nil {
		return 0, err
	}

	// Report the indexer statistics
	stats := bulkIndexer.Stats()
	if stats.NumFailed > 0 {
		return 0, fmt.Errorf("Indexed [%d] documents with [%d] errors", stats.NumFlushed, stats.NumFailed)
	}
	total := stats.NumAdded + stats.NumUpdated
	return total, nil
}

func (e *ElasticsearchDbController2) Delete(params QueryParams) (uint64, error) {
	// make query where ( between )
	var query bytes.Buffer
	if params.IntegerRange != nil {
		queryTmpl := map[string]interface{}{
			"query": map[string]interface{}{
				"range": map[string]interface{}{
					params.IntegerRange.Field: map[string]interface{}{
						"gte": params.IntegerRange.Min,
						"lte": params.IntegerRange.Max,
					},
				},
			},
		}
		enc := json.NewEncoder(&query)
		enc.SetEscapeHTML(false)
		err := enc.Encode(queryTmpl)
		if err != nil {
			return 0, fmt.Errorf("invalid query json format | err : %v", err)
		}
	}

	res, err := e.Client.DeleteByQuery(
		[]string{params.IndexName},
		&query,
	)
	deleteRes := new(BulkIndexByScrollResponse)
	if err = HandleResp(res, err, deleteRes); err != nil {
		return 0, err
	}
	count := uint64(deleteRes.Deleted)
	return count, nil
}

func (e *ElasticsearchDbController2) Count(params QueryParams) (int64, error) {
	res, err := e.Client.Count(
		e.Client.Count.WithIndex(params.IndexName),
	)
	countRes := new(CountResponse)
	if err = HandleResp(res, err, countRes); err != nil {
		return 0, err
	}
	return countRes.Count, nil
}

func (e *ElasticsearchDbController2) SelectOne(params QueryParams, createDocument CreateDocFunction) (doc.DocType, error) {
	var sort string
	if params.SortField != "" {
		if params.SortAsc == true {
			sort = fmt.Sprintf("%s:%s", params.SortField, "asc")
		} else {
			sort = fmt.Sprintf("%s:%s", params.SortField, "desc")
		}
	}

	res, err := e.Client.Search(
		e.Client.Search.WithIndex(params.IndexName),
		e.Client.Search.WithFrom(params.From),
		e.Client.Search.WithSize(1),
		e.Client.Search.WithSort(sort),
	)
	searchRes := new(SearchResult)
	if err = HandleResp(res, err, searchRes); err != nil {
		return nil, err
	}
	if searchRes.TotalHits() == 0 || len(searchRes.Hits.Hits) == 0 {
		return nil, nil
	}

	// decode doc
	hit := searchRes.Hits.Hits[0]
	return HandleDoc(hit, createDocument)
}

func (e *ElasticsearchDbController2) GetExistingIndexPrefix(aliasName string, documentType string) (bool, string, error) {
	res, err := e.Client.Indices.GetAlias(
		e.Client.Indices.GetAlias.WithIndex("_all"),
	)
	aliasesRes := new(AliasesResult)
	if err = HandleResp(res, err, aliasesRes); err != nil {
		return false, "", err
	}

	indices := aliasesRes.IndicesByAlias(aliasName)
	if len(indices) > 0 {
		indexNamePrefix := strings.TrimRight(indices[0], documentType)
		return true, indexNamePrefix, nil
	}
	return false, "", nil
}

func (e *ElasticsearchDbController2) Scroll(params QueryParams, createDocument CreateDocFunction) ScrollInstance {
	return &EsScrollInstance{
		mtx:            sync.Mutex{},
		client:         e.Client,
		params:         &params,
		createDocument: createDocument,
		keepAlive:      time.Minute * 5, // default duration : 5 minute ( 기존과 동일 )
	}
}

// EsScrollInstance is an instance of a scroll for ES
type EsScrollInstance struct {
	mtx            sync.Mutex
	client         *es.Client
	params         *QueryParams
	createDocument CreateDocFunction
	keepAlive      time.Duration

	result        *SearchResult
	scrollId      string
	current       int
	currentLength int
	ctx           context.Context
}

// Next returns the next document of a scroll or io.EOF
func (s *EsScrollInstance) Next() (doc.DocType, error) {
	var err error
	if s.scrollId == "" { // Load first part ( use select method )
		err = s.first()
	} else if s.current >= s.currentLength { // Load next part ( use scroll method )
		err = s.next()
	}
	if err != nil {
		return nil, err
	}

	// Return next document
	if s.current < s.currentLength {
		hit := s.result.Hits.Hits[s.current]
		s.current++
		return HandleDoc(hit, s.createDocument)
	}

	return nil, io.EOF // returns io.EOF when scroll is done
}

func (s *EsScrollInstance) first() error {
	var sort string
	if s.params.SortField != "" {
		if s.params.SortAsc == true {
			sort = fmt.Sprintf("%s:%s", s.params.SortField, "asc")
		} else {
			sort = fmt.Sprintf("%s:%s", s.params.SortField, "desc")
		}
	}

	// indexName, size, sortfield, sortasc
	res, err := s.client.Search(
		s.client.Search.WithIndex(s.params.IndexName),
		s.client.Search.WithSize(s.params.Size),
		s.client.Search.WithSort(sort),
		s.client.Search.WithScroll(s.keepAlive),
	)
	searchRes := new(SearchResult)
	if err = HandleResp(res, err, searchRes); err != nil {
		return err
	}

	// 후처리
	s.result = searchRes
	s.current = 0
	s.currentLength = len(searchRes.Hits.Hits)
	s.scrollId = searchRes.ScrollId
	return nil
}

func (s *EsScrollInstance) next() error {
	res, err := s.client.Scroll(
		s.client.Scroll.WithScroll(s.keepAlive),
		s.client.Scroll.WithScrollID(s.scrollId),
	)
	searchRes := new(SearchResult)
	if err = HandleResp(res, err, searchRes); err != nil {
		return err
	}

	// 후처리
	s.result = searchRes
	s.current = 0
	s.currentLength = len(searchRes.Hits.Hits)
	return nil
}
