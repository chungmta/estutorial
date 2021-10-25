package main

import (
	"context"
	"encoding/json"
	"estutorial/modal"
	"fmt"
	"github.com/olivere/elastic/v7"
	"log"
	"reflect"
	"strings"
)

const mapping = `
{
	"settings":{
		"number_of_shards":1,
		"number_of_replicas":0
	},
	"mappings":{
		"doc":{
			"properties":{
				"user":{
					"type":"keyword"
				},
				"message":{
					"type":"text",
					"store": true,
					"fielddata": true
				},
                "retweets":{
                    "type":"long"
                },
				"tags":{
					"type":"keyword"
				},
				"location":{
					"type":"geo_point"
				},
				"suggest_field":{
					"type":"completion"
				}
			}
		}
	}
}
`

func main() {
	client, err := elastic.NewClient(
		elastic.SetSniff(false),
		elastic.SetURL("http://127.0.0.1:9200"),
	)
	if err != nil {
		// Handle error
		panic(err)
	}
	tweetExample(client)

	fmt.Println(strings.Repeat("-", 37))
	fmt.Println(strings.Repeat("-", 37))

	/*
		TEST SEARCH
	*/
	//Match query
	//matchQuery := elastic.NewMatchQuery("account_number", 20)

	//must
	//Sử đụng điều kiện must kết quả trả về khi tất cả các truy vấn là đúng, ví dụ sau tìm tất cả các địa chỉ có chứa từ mill VÀ lane
	//boolMustQuery := elastic.NewBoolQuery().
	//	Must(elastic.NewMatchQuery("address", "mill")).
	//	Must(elastic.NewMatchQuery("address", "lane"))

	////Kết hợp nhiều điều kiện must, must_not, should vào truy vấn
	//boolCombineQuery := elastic.NewBoolQuery().
	//	Must(elastic.NewMatchQuery("age", "40")).
	//	MustNot(elastic.NewMatchQuery("state", "40"))

	//Để lọc dùng đến filter, ví dụ sử dụng loại range để lọc lấy lấy dữ liệu balance trong khoảng nào đó.
	boolFilter := elastic.NewBoolQuery().
		Must(elastic.NewMatchAllQuery()).
		Filter(elastic.NewRangeQuery("balance").Gte(20000).Lte(30000))

	//Wildcard Query
	// state bat dau = d
	_ = elastic.NewWildcardQuery("state", "d*")

	r1, err := client.Search().
		Index("bank").
		Query(boolFilter).
		//Sort("user", true).
		//From(0).Size(10).
		Pretty(true).
		Do(context.Background()) // execute
	if err != nil {
		// Handle error
		panic(err)
	}

	// r1 is of type SearchResult and returns hits, suggestions,
	// and all kinds of other information from Elasticsearch.
	fmt.Printf("Query took %d milliseconds\n", r1.TookInMillis)
	fmt.Printf("Found a total of %d account\n", r1.TotalHits())
	fmt.Println(strings.Repeat("-", 37))
}

func tweetExample(client *elastic.Client) {
	log.Println(strings.Repeat("-", 37))

	// Starting with elastic.v5, you must pass a context to execute each service
	ctx := context.Background()

	// Ping the Elasticsearch server to get e.g. the version number
	info, code, err := client.Ping("http://127.0.0.1:9200").Do(ctx)
	if err != nil {
		// Handle error
		panic(err)
	}
	fmt.Printf("Elasticsearch returned with code %d and version %s\n", code, info.Version.Number)

	// Getting the ES version number is quite common, so there's a shortcut
	esversion, err := client.ElasticsearchVersion("http://127.0.0.1:9200")
	if err != nil {
		// Handle error
		panic(err)
	}
	fmt.Printf("Elasticsearch version %s\n", esversion)

	// Use the IndexExists service to check if a specified index exists.
	exists, err := client.IndexExists("twitter").Do(ctx)
	if err != nil {
		// Handle error
		panic(err)
	}
	if !exists {
		// Create a new index.
		fmt.Printf("Create new index\n")
		createIndex, err := client.CreateIndex("twitter").Do(ctx)
		if err != nil {
			// Handle error
			panic(err)
		}
		if !createIndex.Acknowledged {
			// Not acknowledged
		}
	}

	// Index a tweet (using JSON serialization)
	tweet1 := modal.Tweet{User: "olivere", Message: "Take Five", Retweets: 0}
	put1, err := client.Index().
		Index("twitter").
		Type("doc").
		Id("1").
		BodyJson(tweet1).
		Do(context.Background())
	if err != nil {
		// Handle error
		panic(err)
	}
	fmt.Printf("Indexed tweet %s to index %s, type %s\n", put1.Id, put1.Index, put1.Type)

	// Index a second tweet (by string)
	tweet2 := `{"user" : "olivere", "message" : "It's a Raggy Waltz"}`
	put2, err := client.Index().
		Index("twitter").
		Type("doc").
		Id("2").
		BodyString(tweet2).
		Do(context.Background())
	if err != nil {
		// Handle error
		panic(err)
	}
	fmt.Printf("Indexed tweet %s to index %s, type %s\n", put2.Id, put2.Index, put2.Type)

	// Get tweet with specified ID
	get1, err := client.Get().
		Index("twitter").
		Type("doc").
		Id("1").
		Do(context.Background())
	if err != nil {
		switch {
		case elastic.IsNotFound(err):
			panic(fmt.Sprintf("Document not found: %v", err))
		case elastic.IsTimeout(err):
			panic(fmt.Sprintf("Timeout retrieving document: %v", err))
		case elastic.IsConnErr(err):
			panic(fmt.Sprintf("Connection problem: %v", err))
		default:
			// Some other kind of error
			panic(err)
		}
	}
	fmt.Printf("Got document %s in version %d from index %s, type %s\n", get1.Id, get1.Version, get1.Index, get1.Type)

	//Search with a term query
	termQuery := elastic.NewTermQuery("user", "olivere")
	searchResult, err := client.Search().
		Index("twitter"). // search in index "twitter"
		Query(termQuery). // specify the query
		//Sort("user", true).      // sort by "user" field, ascending
		//From(0).Size(10).        // take documents 0-9
		Pretty(true). // pretty print request and response JSON
		Do(context.Background()) // execute
	if err != nil {
		// Handle error
		panic(err)
	}

	// searchResult is of type SearchResult and returns hits, suggestions,
	// and all kinds of other information from Elasticsearch.
	fmt.Printf("Query took %d milliseconds\n", searchResult.TookInMillis)

	// Each is a convenience function that iterates over hits in a search result.
	// It makes sure you don't need to check for nil values in the response.
	// However, it ignores errors in serialization. If you want full control
	// over iterating the hits, see below.
	var ttyp modal.Tweet
	for _, item := range searchResult.Each(reflect.TypeOf(ttyp)) {
		t := item.(modal.Tweet)
		fmt.Printf("Tweet by %s: %s\n", t.User, t.Message)
	}
	// TotalHits is another convenience function that works even when something goes wrong.
	fmt.Printf("Found a total of %d tweets %v\n", searchResult.TotalHits(), *searchResult.Hits.TotalHits)

	// Here's how you iterate through results with full control over each step.
	if searchResult.TotalHits() > 0 {
		fmt.Printf("Found a total of %d tweets\n", searchResult.Hits.TotalHits.Value)

		// Iterate through results
		for _, hit := range searchResult.Hits.Hits {
			// hit.Index contains the name of the index

			// Deserialize hit.Source into a Tweet (could also be just a map[string]interface{}).
			var t modal.Tweet
			err := json.Unmarshal(hit.Source, &t)
			if err != nil {
				// Deserialization failed
			}

			// Work with tweet
			fmt.Printf("Tweet by %s: %s\n", t.User, t.Message)
		}
	} else {
		// No hits
		fmt.Print("Found no tweets\n")
	}

	// Update a tweet by the update API of Elasticsearch.
	// We just increment the number of retweets.
	script := elastic.NewScript("ctx._source.retweets += params.num").Param("num", 1)
	update, err := client.Update().Index("twitter").Type("doc").Id("1").
		Script(script).
		Upsert(map[string]interface{}{"retweets": 0}).
		Do(context.Background())
	if err != nil {
		// Handle error
		panic(err)
	}
	fmt.Printf("New version of tweet %q is now %d", update.Id, update.Version)

	// Delete an index.
	//deleteIndex, err := client.DeleteIndex("twitter").Do(context.Background())
	//if err != nil {
	//	// Handle error
	//	panic(err)
	//}
	//if !deleteIndex.Acknowledged {
	//	// Not acknowledged
	//}
}
