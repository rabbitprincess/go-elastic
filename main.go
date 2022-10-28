package main

import (
	"log"

	"github.com/gokch/go-elastic/elastic"
)

func main() {
	client, err := elastic.NewElasticsearchDbController("https://localhost:9200")
	if err != nil {
		log.Fatalf("Error creating the client: %s", err)
	}
	_, err = client.Info()
	if err != nil {
		log.Fatalf("Error info the client: %s", err)
	}
}
