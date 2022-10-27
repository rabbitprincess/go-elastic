package elastic_test

import (
	"fmt"
	"go-elasticsearch-example/elastic"
	"log"
	"testing"
	"time"

	doc "github.com/aergoio/aergo-indexer/indexer/documents"
)

func InitClient() (*elastic.ElasticsearchDbController2, error) {
	client, err := elastic.NewElasticsearchDbController("https://localhost:9200")
	if err != nil {
		return nil, err
	}
	return client, nil
}

func Test_elastic_init(t *testing.T) {
	client, err := InitClient()
	if err != nil {
		t.Fatal(err)
	}

	// info
	info, err := client.Info()
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range info {
		fmt.Printf("%v : %v\n", k, v)
	}
}

func Test_elastic_createIndex(t *testing.T) {
	client, err := InitClient()
	if err != nil {
		t.Fatal(err)
	}

	// create index ( tx )
	err = client.CreateIndex("chain_2022-10-19_06-44-43_tx", "tx")
	if err != nil {
		t.Fatal(err)
	}

	// create index ( block )
	err = client.CreateIndex("block", "block")
	if err != nil {
		t.Fatal(err)
	}

	// create index ( name )
	err = client.CreateIndex("name", "name")
	if err != nil {
		t.Fatal(err)
	}

	// create index ( token_transfer )
	err = client.CreateIndex("token_transfer", "token_transfer")
	if err != nil {
		t.Fatal(err)
	}

	// create index ( token )
	err = client.CreateIndex("token", "token")
	if err != nil {
		t.Fatal(err)
	}
}

func Test_elastic_insert(t *testing.T) {
	client, err := InitClient()
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i < 20; i++ {
		blockDoc := &doc.EsBlock{
			BaseEsType:    &doc.BaseEsType{Id: RandStringRunes(50)},
			Timestamp:     time.Now(),
			BlockNo:       uint64(i),
			TxCount:       uint(i + 10),
			Size:          int64(1000000 - i*234),
			RewardAccount: RandStringRunes(20),
			RewardAmount:  "123434567890000000",
		}

		params := elastic.UpdateParams{
			IndexName: "block",
			TypeName:  "block",
		}

		_, err = client.Insert(blockDoc, params)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func Test_elastic_insertBulk(t *testing.T) {
	client, err := InitClient()
	if err != nil {
		t.Fatal(err)
	}

	chanDoc := make(chan doc.DocType)
	params := elastic.UpdateParams{
		IndexName: "block",
		TypeName:  "block",
		Upsert:    false,
		Size:      10,
	}
	go func() {
		insert, err := client.InsertBulk(chanDoc, params)
		if err != nil {
			log.Fatal(err)
		}

		fmt.Printf("insert ( cnt : %d )\n", insert)
	}()

	for i := 1; i < 100; i++ {
		chanDoc <- &doc.EsBlock{
			BaseEsType:    &doc.BaseEsType{Id: RandStringRunes(50)},
			Timestamp:     time.Now(),
			BlockNo:       uint64(i),
			TxCount:       uint(i + 10),
			Size:          int64(1000000 - i*234),
			RewardAccount: RandStringRunes(20),
			RewardAmount:  "123434567890000000",
		}
	}
	close(chanDoc)
	time.Sleep(time.Second * 10)
}

func Test_elastic_count(t *testing.T) {
	client, err := InitClient()
	if err != nil {
		t.Fatal(err)
	}

	params := elastic.QueryParams{
		IndexName: "block",
	}
	count, err := client.Count(params)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("count in index ( block ) : %d\n", count)
}

func Test_elastic_selectOne(t *testing.T) {
	client, err := InitClient()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		res, err := client.SelectOne(
			elastic.QueryParams{
				IndexName: "block",
				SortField: "no",
				SortAsc:   true,
				From:      i,
			},
			func() doc.DocType {
				block := new(doc.EsBlock)
				block.BaseEsType = new(doc.BaseEsType)
				return block
			},
		)
		if err != nil {
			t.Fatal(err)
		}
		fmt.Println(res)
	}
}

func Test_elastic_scroll(t *testing.T) {
	client, err := InitClient()
	if err != nil {
		t.Fatal(err)
	}
	scroll := client.Scroll(
		elastic.QueryParams{
			IndexName: "block",
			SortField: "no",
			SortAsc:   true,
			Size:      6,
		},
		func() doc.DocType {
			block := new(doc.EsBlock)
			block.BaseEsType = new(doc.BaseEsType)
			return block
		},
	)
	var doc doc.DocType
	var errScroll error
	for {
		doc, errScroll = scroll.Next()
		if errScroll != nil {
			break
		}
		fmt.Printf("%+v\n", doc)
	}
	fmt.Printf("err : %v\n", errScroll)
}

func Test_elastic_delete(t *testing.T) {
	client, err := InitClient()
	if err != nil {
		t.Fatal(err)
	}
	cnt, err := client.Delete(elastic.QueryParams{
		IndexName: "block",
		IntegerRange: &elastic.IntegerRangeQuery{
			Field: "no",
			Min:   0,
			Max:   30,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("delete cnt : %d\n", cnt)
}
