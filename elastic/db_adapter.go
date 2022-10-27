package elastic

import (
	"fmt"

	doc "github.com/aergoio/aergo-indexer/indexer/documents"
	"github.com/aergoio/aergo-lib/log"
)

var (
	logger = log.NewLogger("db")
)

type UpdateParams struct {
	IndexName string
	TypeName  string
	Upsert    bool
	Size      int
}

type QueryParams struct {
	IndexName    string
	TypeName     string
	From         int
	Size         int
	SortField    string
	SortAsc      bool
	SelectFields []string
	IntegerRange *IntegerRangeQuery
	StringMatch  *StringMatchQuery
}

type IntegerRangeQuery struct {
	Field string
	Min   uint64
	Max   uint64
}

type StringMatchQuery struct {
	Field string
	Value string
}

type DbController interface {
	Insert(document doc.DocType, params UpdateParams) (uint64, error)
	InsertBulk(documentChannel chan doc.DocType, params UpdateParams) (uint64, error)
	Delete(params QueryParams) (uint64, error)
	Count(params QueryParams) (int64, error)
	SelectOne(params QueryParams, createDocument CreateDocFunction) (doc.DocType, error)
	Scroll(params QueryParams, createDocument CreateDocFunction) ScrollInstance
	GetExistingIndexPrefix(aliasName string, documentType string) (bool, string, error)
	CreateIndex(indexName string, documentType string) error
	UpdateAlias(aliasName string, indexName string) error
	IsConflict(err interface{}) bool
}

type CreateDocFunction = func() doc.DocType

type ScrollInstance interface {
	/*
		params QueryParams
		current    int
	*/
	Next() (doc.DocType, error)
}

type IndexConflictError struct {
	WrappedError error
}

func (i *IndexConflictError) Error() string {
	return fmt.Sprintf("conflict: %s", i.WrappedError.Error())
}
