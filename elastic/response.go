package elastic

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
)

//------------------------------------------------------------------//
// index api

type AliasesResult struct {
	Indices map[string]indexResult
}

type indexResult struct {
	Aliases []aliasResult
}

type aliasResult struct {
	AliasName    string
	IsWriteIndex bool
}

// IndicesByAlias returns all indices given a specific alias name.
func (ar AliasesResult) IndicesByAlias(aliasName string) []string {
	var indices []string
	for indexName, indexInfo := range ar.Indices {
		for _, aliasInfo := range indexInfo.Aliases {
			if aliasInfo.AliasName == aliasName {
				indices = append(indices, indexName)
			}
		}
	}
	return indices
}

// HasAlias returns true if the index has a specific alias.
func (ir indexResult) HasAlias(aliasName string) bool {
	for _, alias := range ir.Aliases {
		if alias.AliasName == aliasName {
			return true
		}
	}
	return false
}

//------------------------------------------------------------------//
// count api

// CountResponse is the response of using the Count API.
type CountResponse struct {
	Count           int64       `json:"count"`
	TerminatedEarly bool        `json:"terminated_early,omitempty"`
	Shards          *ShardsInfo `json:"_shards,omitempty"`
}

// ShardsInfo represents information from a shard.
type ShardsInfo struct {
	Total      int             `json:"total"`
	Successful int             `json:"successful"`
	Failed     int             `json:"failed"`
	Failures   []*ShardFailure `json:"failures,omitempty"`
	Skipped    int             `json:"skipped,omitempty"`
}

// ShardFailure represents details about a failure.
type ShardFailure struct {
	Index   string                 `json:"_index,omitempty"`
	Shard   int                    `json:"_shard,omitempty"`
	Node    string                 `json:"_node,omitempty"`
	Reason  map[string]interface{} `json:"reason,omitempty"`
	Status  string                 `json:"status,omitempty"`
	Primary bool                   `json:"primary,omitempty"`
}

//------------------------------------------------------------------//
// search api

// SearchResult is the result of a search in Elasticsearch.
type SearchResult struct {
	Header          http.Header    `json:"-"`
	TookInMillis    int64          `json:"took,omitempty"`             // search time in milliseconds
	TerminatedEarly bool           `json:"terminated_early,omitempty"` // request terminated early
	NumReducePhases int            `json:"num_reduce_phases,omitempty"`
	ScrollId        string         `json:"_scroll_id,omitempty"`   // only used with Scroll and Scan operations
	Hits            *SearchHits    `json:"hits,omitempty"`         // the actual search hits
	Suggest         SearchSuggest  `json:"suggest,omitempty"`      // results from suggesters
	Aggregations    Aggregations   `json:"aggregations,omitempty"` // results from aggregations
	TimedOut        bool           `json:"timed_out,omitempty"`    // true if the search timed out
	Error           *ErrorDetails  `json:"error,omitempty"`        // only used in MultiGet
	Profile         *SearchProfile `json:"profile,omitempty"`      // profiling results, if optional Profile API was active for this search
	Shards          *ShardsInfo    `json:"_shards,omitempty"`      // shard information
	Status          int            `json:"status,omitempty"`       // used in MultiSearch
}

// Aggregations is a list of aggregations that are part of a search result.
type Aggregations map[string]*json.RawMessage

// ErrorDetails encapsulate error details from Elasticsearch.
// It is used in e.g. elastic.Error and elastic.BulkResponseItem.
type ErrorDetails struct {
	Type         string                   `json:"type"`
	Reason       string                   `json:"reason"`
	ResourceType string                   `json:"resource.type,omitempty"`
	ResourceId   string                   `json:"resource.id,omitempty"`
	Index        string                   `json:"index,omitempty"`
	Phase        string                   `json:"phase,omitempty"`
	Grouped      bool                     `json:"grouped,omitempty"`
	CausedBy     map[string]interface{}   `json:"caused_by,omitempty"`
	RootCause    []*ErrorDetails          `json:"root_cause,omitempty"`
	FailedShards []map[string]interface{} `json:"failed_shards,omitempty"`
}

// TotalHits is a convenience function to return the number of hits for
// a search result.
func (r *SearchResult) TotalHits() int64 {
	if r.Hits != nil {
		return r.Hits.TotalHits.Value
	}
	return 0
}

// Each is a utility function to iterate over all hits. It saves you from
// checking for nil values. Notice that Each will ignore errors in
// serializing JSON and hits with empty/nil _source will get an empty
// value
func (r *SearchResult) Each(typ reflect.Type) []interface{} {
	if r.Hits == nil || r.Hits.Hits == nil || len(r.Hits.Hits) == 0 {
		return nil
	}
	var slice []interface{}
	for _, hit := range r.Hits.Hits {
		v := reflect.New(typ).Elem()
		if hit.Source == nil {
			slice = append(slice, v.Interface())
			continue
		}
		if err := json.Unmarshal(*hit.Source, v.Addr().Interface()); err == nil {
			slice = append(slice, v.Interface())
		}
	}
	return slice
}

// SearchHits specifies the list of search hits.
type SearchHits struct {
	TotalHits *TotalHit    `json:"total"`               // total number of hits found
	MaxScore  *float64     `json:"max_score,omitempty"` // maximum score of all hits
	Hits      []*SearchHit `json:"hits,omitempty"`      // the actual hits returned
}

type TotalHit struct {
	Value    int64  `json:"value"`
	Relation string `json:"relation"`
}

// NestedHit is a nested innerhit
type NestedHit struct {
	Field  string     `json:"field"`
	Offset int        `json:"offset,omitempty"`
	Child  *NestedHit `json:"_nested,omitempty"`
}

// SearchHit is a single hit.
type SearchHit struct {
	Score          *float64                       `json:"_score,omitempty"`   // computed score
	Index          string                         `json:"_index,omitempty"`   // index name
	Type           string                         `json:"_type,omitempty"`    // type meta field
	Id             string                         `json:"_id,omitempty"`      // external or internal
	Uid            string                         `json:"_uid,omitempty"`     // uid meta field (see MapperService.java for all meta fields)
	Routing        string                         `json:"_routing,omitempty"` // routing meta field
	Parent         string                         `json:"_parent,omitempty"`  // parent meta field
	Version        *int64                         `json:"_version,omitempty"` // version number, when Version is set to true in SearchService
	SeqNo          *int64                         `json:"_seq_no"`
	PrimaryTerm    *int64                         `json:"_primary_term"`
	Sort           []interface{}                  `json:"sort,omitempty"`            // sort information
	Highlight      SearchHitHighlight             `json:"highlight,omitempty"`       // highlighter information
	Source         *json.RawMessage               `json:"_source,omitempty"`         // stored document source
	Fields         map[string]interface{}         `json:"fields,omitempty"`          // returned (stored) fields
	Explanation    *SearchExplanation             `json:"_explanation,omitempty"`    // explains how the score was computed
	MatchedQueries []string                       `json:"matched_queries,omitempty"` // matched queries
	InnerHits      map[string]*SearchHitInnerHits `json:"inner_hits,omitempty"`      // inner hits with ES >= 1.5.0
	Nested         *NestedHit                     `json:"_nested,omitempty"`         // for nested inner hits
	Shard          string                         `json:"_shard,omitempty"`          // used e.g. in Search Explain
	Node           string                         `json:"_node,omitempty"`           // used e.g. in Search Explain

	// HighlightFields
	// SortValues
	// MatchedFilters
}

// SearchHitInnerHits is used for inner hits.
type SearchHitInnerHits struct {
	Hits *SearchHits `json:"hits,omitempty"`
}

// SearchExplanation explains how the score for a hit was computed.
// See https://www.elastic.co/guide/en/elasticsearch/reference/6.8/search-request-explain.html.
type SearchExplanation struct {
	Value       float64             `json:"value"`             // e.g. 1.0
	Description string              `json:"description"`       // e.g. "boost" or "ConstantScore(*:*), product of:"
	Details     []SearchExplanation `json:"details,omitempty"` // recursive details
}

// Suggest

// SearchSuggest is a map of suggestions.
// See https://www.elastic.co/guide/en/elasticsearch/reference/6.8/search-suggesters.html.
type SearchSuggest map[string][]SearchSuggestion

// SearchSuggestion is a single search suggestion.
// See https://www.elastic.co/guide/en/elasticsearch/reference/6.8/search-suggesters.html.
type SearchSuggestion struct {
	Text    string                   `json:"text"`
	Offset  int                      `json:"offset"`
	Length  int                      `json:"length"`
	Options []SearchSuggestionOption `json:"options"`
}

// SearchSuggestionOption is an option of a SearchSuggestion.
// See https://www.elastic.co/guide/en/elasticsearch/reference/6.8/search-suggesters.html.
type SearchSuggestionOption struct {
	Text            string              `json:"text"`
	Index           string              `json:"_index"`
	Type            string              `json:"_type"`
	Id              string              `json:"_id"`
	Score           float64             `json:"score"`  // term and phrase suggesters uses "score" as of 6.2.4
	ScoreUnderscore float64             `json:"_score"` // completion and context suggesters uses "_score" as of 6.2.4
	Highlighted     string              `json:"highlighted"`
	CollateMatch    bool                `json:"collate_match"`
	Freq            int                 `json:"freq"` // from TermSuggestion.Option in Java API
	Source          *json.RawMessage    `json:"_source"`
	Contexts        map[string][]string `json:"contexts,omitempty"`
}

// SearchProfile is a list of shard profiling data collected during
// query execution in the "profile" section of a SearchResult
type SearchProfile struct {
	Shards []SearchProfileShardResult `json:"shards"`
}

// SearchProfileShardResult returns the profiling data for a single shard
// accessed during the search query or aggregation.
type SearchProfileShardResult struct {
	ID           string                    `json:"id"`
	Searches     []QueryProfileShardResult `json:"searches"`
	Aggregations []ProfileResult           `json:"aggregations"`
}

// QueryProfileShardResult is a container class to hold the profile results
// for a single shard in the request. It comtains a list of query profiles,
// a collector tree and a total rewrite tree.
type QueryProfileShardResult struct {
	Query       []ProfileResult `json:"query,omitempty"`
	RewriteTime int64           `json:"rewrite_time,omitempty"`
	Collector   []interface{}   `json:"collector,omitempty"`
}

// CollectorResult holds the profile timings of the collectors used in the
// search. Children's CollectorResults may be embedded inside of a parent
// CollectorResult.
type CollectorResult struct {
	Name      string            `json:"name,omitempty"`
	Reason    string            `json:"reason,omitempty"`
	Time      string            `json:"time,omitempty"`
	TimeNanos int64             `json:"time_in_nanos,omitempty"`
	Children  []CollectorResult `json:"children,omitempty"`
}

// ProfileResult is the internal representation of a profiled query,
// corresponding to a single node in the query tree.
type ProfileResult struct {
	Type          string           `json:"type"`
	Description   string           `json:"description,omitempty"`
	NodeTime      string           `json:"time,omitempty"`
	NodeTimeNanos int64            `json:"time_in_nanos,omitempty"`
	Breakdown     map[string]int64 `json:"breakdown,omitempty"`
	Children      []ProfileResult  `json:"children,omitempty"`
}

// Aggregations (see search_aggs.go)

// Highlighting

// SearchHitHighlight is the highlight information of a search hit.
// See https://www.elastic.co/guide/en/elasticsearch/reference/6.8/search-request-highlighting.html
// for a general discussion of highlighting.
type SearchHitHighlight map[string][]string

//------------------------------------------------------------------//
// delete api

// BulkIndexByScrollResponse is the outcome of executing Do with
// DeleteByQueryService and UpdateByQueryService.
type BulkIndexByScrollResponse struct {
	Header           http.Header `json:"-"`
	Took             int64       `json:"took"`
	SliceId          *int64      `json:"slice_id,omitempty"`
	TimedOut         bool        `json:"timed_out"`
	Total            int64       `json:"total"`
	Updated          int64       `json:"updated,omitempty"`
	Created          int64       `json:"created,omitempty"`
	Deleted          int64       `json:"deleted"`
	Batches          int64       `json:"batches"`
	VersionConflicts int64       `json:"version_conflicts"`
	Noops            int64       `json:"noops"`
	Retries          struct {
		Bulk   int64 `json:"bulk"`
		Search int64 `json:"search"`
	} `json:"retries,omitempty"`
	Throttled            string                             `json:"throttled"`
	ThrottledMillis      int64                              `json:"throttled_millis"`
	RequestsPerSecond    float64                            `json:"requests_per_second"`
	Canceled             string                             `json:"canceled,omitempty"`
	ThrottledUntil       string                             `json:"throttled_until"`
	ThrottledUntilMillis int64                              `json:"throttled_until_millis"`
	Failures             []bulkIndexByScrollResponseFailure `json:"failures"`
}

type bulkIndexByScrollResponseFailure struct {
	Index  string `json:"index,omitempty"`
	Type   string `json:"type,omitempty"`
	Id     string `json:"id,omitempty"`
	Status int    `json:"status,omitempty"`
	Shard  int    `json:"shard,omitempty"`
	Node   int    `json:"node,omitempty"`
	// TOOD "cause" contains exception details
	// TOOD "reason" contains exception details
}

//---------------------------------------------------------------------------------//
// bulk insert api

// BulkIndexerItem represents an indexer item.
type BulkIndexerItem struct {
	Index           string
	Action          string
	DocumentID      string
	Routing         string
	Version         *int64
	VersionType     string
	Body            io.ReadSeeker
	RetryOnConflict *int
}

// BulkUpdateRequest is a request to update a document in Elasticsearch.
//
// See https://www.elastic.co/guide/en/elasticsearch/reference/6.8/docs-bulk.html
// for details.
type BulkUpdateRequest struct {
	BulkableRequest
	index string
	typ   string
	id    string

	routing         string
	parent          string
	script          interface{}
	scriptedUpsert  *bool
	version         int64  // default is MATCH_ANY
	versionType     string // default is "internal"
	retryOnConflict *int
	upsert          interface{}
	docAsUpsert     *bool
	detectNoop      *bool
	doc             interface{}
	returnSource    *bool
	ifSeqNo         *int64
	ifPrimaryTerm   *int64

	source []string

	useEasyJSON bool
}

type bulkUpdateRequestCommand map[string]bulkUpdateRequestCommandOp

type bulkUpdateRequestCommandOp struct {
	Index  string `json:"_index,omitempty"`
	Id     string `json:"_id,omitempty"`
	Parent string `json:"parent,omitempty"`
	// RetryOnConflict is "_retry_on_conflict" for 6.0 and "retry_on_conflict" for 6.1+.
	RetryOnConflict *int   `json:"retry_on_conflict,omitempty"`
	Routing         string `json:"routing,omitempty"`
	Version         int64  `json:"version,omitempty"`
	VersionType     string `json:"version_type,omitempty"`
	IfSeqNo         *int64 `json:"if_seq_no,omitempty"`
	IfPrimaryTerm   *int64 `json:"if_primary_term,omitempty"`
}

type bulkUpdateRequestCommandData struct {
	DetectNoop     *bool       `json:"detect_noop,omitempty"`
	Doc            interface{} `json:"doc,omitempty"`
	DocAsUpsert    *bool       `json:"doc_as_upsert,omitempty"`
	Script         interface{} `json:"script,omitempty"`
	ScriptedUpsert *bool       `json:"scripted_upsert,omitempty"`
	Upsert         interface{} `json:"upsert,omitempty"`
	Source         *bool       `json:"_source,omitempty"`
}

// NewBulkUpdateRequest returns a new BulkUpdateRequest.
func NewBulkUpdateRequest() *BulkUpdateRequest {
	return &BulkUpdateRequest{}
}

// UseEasyJSON is an experimental setting that enables serialization
// with github.com/mailru/easyjson, which should in faster serialization
// time and less allocations, but removed compatibility with encoding/json,
// usage of unsafe etc. See https://github.com/mailru/easyjson#issues-notes-and-limitations
// for details. This setting is disabled by default.
func (r *BulkUpdateRequest) UseEasyJSON(enable bool) *BulkUpdateRequest {
	r.useEasyJSON = enable
	return r
}

// Index specifies the Elasticsearch index to use for this update request.
// If unspecified, the index set on the BulkService will be used.
func (r *BulkUpdateRequest) Index(index string) *BulkUpdateRequest {
	r.index = index
	r.source = nil
	return r
}

// Type specifies the Elasticsearch type to use for this update request.
// If unspecified, the type set on the BulkService will be used.
func (r *BulkUpdateRequest) Type(typ string) *BulkUpdateRequest {
	r.typ = typ
	r.source = nil
	return r
}

// Id specifies the identifier of the document to update.
func (r *BulkUpdateRequest) Id(id string) *BulkUpdateRequest {
	r.id = id
	r.source = nil
	return r
}

// Routing specifies a routing value for the request.
func (r *BulkUpdateRequest) Routing(routing string) *BulkUpdateRequest {
	r.routing = routing
	r.source = nil
	return r
}

// Parent specifies the identifier of the parent document (if available).
func (r *BulkUpdateRequest) Parent(parent string) *BulkUpdateRequest {
	r.parent = parent
	r.source = nil
	return r
}

// Script specifies an update script.
// See https://www.elastic.co/guide/en/elasticsearch/reference/6.8/docs-bulk.html#bulk-update
// and https://www.elastic.co/guide/en/elasticsearch/reference/6.8/modules-scripting.html
// for details.
func (r *BulkUpdateRequest) Script(script interface{}) *BulkUpdateRequest {
	r.script = script
	r.source = nil
	return r
}

// ScripedUpsert specifies if your script will run regardless of
// whether the document exists or not.
//
// See https://www.elastic.co/guide/en/elasticsearch/reference/6.8/docs-update.html#_literal_scripted_upsert_literal
func (r *BulkUpdateRequest) ScriptedUpsert(upsert bool) *BulkUpdateRequest {
	r.scriptedUpsert = &upsert
	r.source = nil
	return r
}

// RetryOnConflict specifies how often to retry in case of a version conflict.
func (r *BulkUpdateRequest) RetryOnConflict(retryOnConflict int) *BulkUpdateRequest {
	r.retryOnConflict = &retryOnConflict
	r.source = nil
	return r
}

// Version indicates the version of the document as part of an optimistic
// concurrency model.
func (r *BulkUpdateRequest) Version(version int64) *BulkUpdateRequest {
	r.version = version
	r.source = nil
	return r
}

// VersionType can be "internal" (default), "external", "external_gte",
// or "external_gt".
func (r *BulkUpdateRequest) VersionType(versionType string) *BulkUpdateRequest {
	r.versionType = versionType
	r.source = nil
	return r
}

// IfSeqNo indicates to only perform the index operation if the last
// operation that has changed the document has the specified sequence number.
func (r *BulkUpdateRequest) IfSeqNo(ifSeqNo int64) *BulkUpdateRequest {
	r.ifSeqNo = &ifSeqNo
	return r
}

// IfPrimaryTerm indicates to only perform the index operation if the
// last operation that has changed the document has the specified primary term.
func (r *BulkUpdateRequest) IfPrimaryTerm(ifPrimaryTerm int64) *BulkUpdateRequest {
	r.ifPrimaryTerm = &ifPrimaryTerm
	return r
}

// Doc specifies the updated document.
func (r *BulkUpdateRequest) Doc(doc interface{}) *BulkUpdateRequest {
	r.doc = doc
	r.source = nil
	return r
}

// DocAsUpsert indicates whether the contents of Doc should be used as
// the Upsert value.
//
// See https://www.elastic.co/guide/en/elasticsearch/reference/6.8/docs-update.html#_literal_doc_as_upsert_literal
// for details.
func (r *BulkUpdateRequest) DocAsUpsert(docAsUpsert bool) *BulkUpdateRequest {
	r.docAsUpsert = &docAsUpsert
	r.source = nil
	return r
}

// DetectNoop specifies whether changes that don't affect the document
// should be ignored (true) or unignored (false). This is enabled by default
// in Elasticsearch.
func (r *BulkUpdateRequest) DetectNoop(detectNoop bool) *BulkUpdateRequest {
	r.detectNoop = &detectNoop
	r.source = nil
	return r
}

// Upsert specifies the document to use for upserts. It will be used for
// create if the original document does not exist.
func (r *BulkUpdateRequest) Upsert(doc interface{}) *BulkUpdateRequest {
	r.upsert = doc
	r.source = nil
	return r
}

// ReturnSource specifies whether Elasticsearch should return the source
// after the update. In the request, this responds to the `_source` field.
// It is false by default.
func (r *BulkUpdateRequest) ReturnSource(source bool) *BulkUpdateRequest {
	r.returnSource = &source
	r.source = nil
	return r
}

// String returns the on-wire representation of the update request,
// concatenated as a single string.
func (r *BulkUpdateRequest) String() string {
	lines, err := r.Source()
	if err != nil {
		return fmt.Sprintf("error: %v", err)
	}
	return strings.Join(lines, "\n")
}

// Source returns the on-wire representation of the update request,
// split into an action-and-meta-data line and an (optional) source line.
// See https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html
// for details.
func (r *BulkUpdateRequest) Source() ([]string, error) {
	// { "update" : { "_index" : "test", "_id" : "1", ... } }
	// { "doc" : { "field1" : "value1", ... } }
	// or
	// { "update" : { "_index" : "test", "_id" : "1", ... } }
	// { "script" : { ... } }

	if r.source != nil {
		return r.source, nil
	}

	lines := make([]string, 2)

	// "update" ...
	updateCommand := bulkUpdateRequestCommandOp{
		Index:           r.index,
		Id:              r.id,
		Routing:         r.routing,
		Parent:          r.parent,
		Version:         r.version,
		VersionType:     r.versionType,
		RetryOnConflict: r.retryOnConflict,
		IfSeqNo:         r.ifSeqNo,
		IfPrimaryTerm:   r.ifPrimaryTerm,
	}
	command := bulkUpdateRequestCommand{
		"update": updateCommand,
	}

	body, err := json.Marshal(command)
	if err != nil {
		return nil, err
	}

	lines[0] = string(body)

	// 2nd line: {"doc" : { ... }} or {"script": {...}}
	var doc interface{}
	if r.doc != nil {
		// Automatically serialize strings as raw JSON
		switch t := r.doc.(type) {
		default:
			doc = r.doc
		case string:
			if len(t) > 0 {
				doc = json.RawMessage(t)
			}
		case *string:
			if t != nil && len(*t) > 0 {
				doc = json.RawMessage(*t)
			}
		}
	}
	data := bulkUpdateRequestCommandData{
		DocAsUpsert:    r.docAsUpsert,
		DetectNoop:     r.detectNoop,
		Upsert:         r.upsert,
		ScriptedUpsert: r.scriptedUpsert,
		Doc:            doc,
		Source:         r.returnSource,
	}
	if r.script != nil {
		data.Script = r.script
	}

	// encoding/json
	body, err = json.Marshal(data)
	if err != nil {
		return nil, err
	}

	lines[1] = string(body)

	r.source = lines
	return lines, nil
}

type BulkResponse struct {
	Took   int                            `json:"took,omitempty"`
	Errors bool                           `json:"errors,omitempty"`
	Items  []map[string]*BulkResponseItem `json:"items,omitempty"`
}

// BulkResponseItem is the result of a single bulk request.
type BulkResponseItem struct {
	Index         string        `json:"_index,omitempty"`
	Type          string        `json:"_type,omitempty"`
	Id            string        `json:"_id,omitempty"`
	Version       int64         `json:"_version,omitempty"`
	Result        string        `json:"result,omitempty"`
	Shards        *ShardsInfo   `json:"_shards,omitempty"`
	SeqNo         int64         `json:"_seq_no,omitempty"`
	PrimaryTerm   int64         `json:"_primary_term,omitempty"`
	Status        int           `json:"status,omitempty"`
	ForcedRefresh bool          `json:"forced_refresh,omitempty"`
	Error         *ErrorDetails `json:"error,omitempty"`
	GetResult     *GetResult    `json:"get,omitempty"`
}

// Indexed returns all bulk request results of "index" actions.
func (r *BulkResponse) Indexed() []*BulkResponseItem {
	return r.ByAction("index")
}

// Created returns all bulk request results of "create" actions.
func (r *BulkResponse) Created() []*BulkResponseItem {
	return r.ByAction("create")
}

// Updated returns all bulk request results of "update" actions.
func (r *BulkResponse) Updated() []*BulkResponseItem {
	return r.ByAction("update")
}

// Deleted returns all bulk request results of "delete" actions.
func (r *BulkResponse) Deleted() []*BulkResponseItem {
	return r.ByAction("delete")
}

// ByAction returns all bulk request results of a certain action,
// e.g. "index" or "delete".
func (r *BulkResponse) ByAction(action string) []*BulkResponseItem {
	if r.Items == nil {
		return nil
	}
	var items []*BulkResponseItem
	for _, item := range r.Items {
		if result, found := item[action]; found {
			items = append(items, result)
		}
	}
	return items
}

// ById returns all bulk request results of a given document id,
// regardless of the action ("index", "delete" etc.).
func (r *BulkResponse) ById(id string) []*BulkResponseItem {
	if r.Items == nil {
		return nil
	}
	var items []*BulkResponseItem
	for _, item := range r.Items {
		for _, result := range item {
			if result.Id == id {
				items = append(items, result)
			}
		}
	}
	return items
}

// Failed returns those items of a bulk response that have errors,
// i.e. those that don't have a status code between 200 and 299.
func (r *BulkResponse) Failed() []*BulkResponseItem {
	if r.Items == nil {
		return nil
	}
	var errors []*BulkResponseItem
	for _, item := range r.Items {
		for _, result := range item {
			if !(result.Status >= 200 && result.Status <= 299) {
				errors = append(errors, result)
			}
		}
	}
	return errors
}

// Succeeded returns those items of a bulk response that have no errors,
// i.e. those have a status code between 200 and 299.
func (r *BulkResponse) Succeeded() []*BulkResponseItem {
	if r.Items == nil {
		return nil
	}
	var succeeded []*BulkResponseItem
	for _, item := range r.Items {
		for _, result := range item {
			if result.Status >= 200 && result.Status <= 299 {
				succeeded = append(succeeded, result)
			}
		}
	}
	return succeeded
}

// -- Bulkable request (index/update/delete) --

// BulkableRequest is a generic interface to bulkable requests.
type BulkableRequest interface {
	fmt.Stringer
	Source() ([]string, error)
}

// GetResult is the outcome of GetService.Do.
type GetResult struct {
	Index       string                 `json:"_index"`   // index meta field
	Type        string                 `json:"_type"`    // type meta field
	Id          string                 `json:"_id"`      // id meta field
	Uid         string                 `json:"_uid"`     // uid meta field (see MapperService.java for all meta fields)
	Routing     string                 `json:"_routing"` // routing meta field
	Parent      string                 `json:"_parent"`  // parent meta field
	Version     *int64                 `json:"_version"` // version number, when Version is set to true in SearchService
	SeqNo       *int64                 `json:"_seq_no"`
	PrimaryTerm *int64                 `json:"_primary_term"`
	Source      *json.RawMessage       `json:"_source,omitempty"`
	Found       bool                   `json:"found,omitempty"`
	Fields      map[string]interface{} `json:"fields,omitempty"`
	//Error     string                 `json:"error,omitempty"` // used only in MultiGet
	// TODO double-check that MultiGet now returns details error information
	Error *ErrorDetails `json:"error,omitempty"` // only used in MultiGet
}
