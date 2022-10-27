package main

import (
	"go-elasticsearch-example/elastic"
	"log"
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
