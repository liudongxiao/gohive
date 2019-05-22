# Hive go client 说明

# hive go client  带连接池支持

## 代码

Hive client 其实是使用 Hive thrift 接口，用 TCLIService.thrift 

tcliservice 目录下面是 TCLIService.thrift 生成的 golang 代码，千万不要手工修改，除非你非常清楚结果

## 使用

```go
cfg := &Config{
    Host:      host,
    UserName:  "root",
}

client, err := NewClient(cfg)
if err != nil {
    t.Fatal(err)
}

defer client.Close()

// 执行 HQL
// ExecuteEx(statement, aync)
result, err := client.ExecuteEx("select country_name from test.Table limit 1500", true)
if err != nil {
    t.Fatal(err)
}

// 获取返回结果
for result.NextPage() {

    for result.NextInPage() {
        result.Scan(&id)
        ids = append(ids, id)
    }
}
```

