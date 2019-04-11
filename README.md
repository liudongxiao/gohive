# go client for hive with session connection pool 
#usage

```
package main

import "github.com/liudongxiao/gohive/hive"

func main() {
	cfg := &hive.Config{}
	hcli, err := hive.NewClient(cfg)
	if err != nil {
		panic(err)
	}
	hcli.Execute("select* from table")

}
```
