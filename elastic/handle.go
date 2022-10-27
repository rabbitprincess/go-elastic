package elastic

import (
	"encoding/json"
	"fmt"
	"io/ioutil"

	doc "github.com/aergoio/aergo-indexer/indexer/documents"
	"github.com/elastic/go-elasticsearch/v8/esapi"
)

//------------------------------------------------------------------//
// handle function

// 1. v interface{} == nil -> check error only
// 2. v interface{} != nil -> check error and decode response
func HandleResp(res *esapi.Response, err error, v interface{}) error {
	if err != nil {
		return err
	}
	// check error
	if res == nil {
		return fmt.Errorf("empty response")
	}
	if res.IsError() == true {
		return fmt.Errorf("err on resp | %s", res.String())
	}

	// decode resp body
	if v != nil {
		body, err := ioutil.ReadAll(res.Body)
		if err != nil {
			return err
		}
		fmt.Println(string(body))
		err = json.Unmarshal(body, v)
		if err != nil {
			return err
		}
	}
	res.Body.Close()

	return nil
}

func HandleDoc(hit *SearchHit, createDocument CreateDocFunction) (doc.DocType, error) {
	unmarshalled := createDocument()
	if err := json.Unmarshal(*hit.Source, unmarshalled); err != nil {
		return nil, err
	}
	unmarshalled.SetID(hit.Id)
	return unmarshalled, nil
}
