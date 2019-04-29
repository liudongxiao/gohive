# go client for hive with session connection pool 
## usage

```
package main

import (
	"fmt"
	"log"
	"sunteng/commons/db/hive"
)

func main() {
	const host = "127.0.0.1:10000"
	cfg := &hive.Config{
		Host:      host,
		BenchSize: 1000,
		UserName:  "hdfs",
		Password:  "",
	}
	client, err := hive.NewClient(cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	sql := "select id from dsp.dim_ostype limit 1500"
	result, err := client.ExecuteEx(sql, true)
	if err != nil {
		log.Fatal(err)
	}

	var id string
	var ids []string

	for result.NextPage() {

		for result.NextInPage() {
			result.Scan(&id)
			ids = append(ids, id)
		}
	}
	if err := result.Err(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("=======", len(ids))

}

```
## more examples please read the hive client test file
