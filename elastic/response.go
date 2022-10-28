package elastic

import (
	"encoding/json"
	"net/http"
	"reflect"
)

//------------------------------------------------------------------//
// index api

type AliasesResult map[string]indexResult

type indexResult struct {
	Aliases map[string]interface{} `json:"aliases"`
}

// IndicesByAlias returns all indices given a specific alias name.
func (ar AliasesResult) IndicesByAlias(aliasName string) []string {
	var indices []string
	for indexName, indexInfo := range ar {
		for aliasInfo := range indexInfo.Aliases {
			if aliasInfo == aliasName {
				indices = append(indices, indexName)
			}
		}
	}
	return indices
}

// HasAlias returns true if the index has a specific alias.
func (ir indexResult) HasAlias(aliasName string) bool {
	for alias := range ir.Aliases {
		if alias == aliasName {
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
