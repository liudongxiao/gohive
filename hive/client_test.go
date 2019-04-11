package hive

import (
	"context"
	//"math/rand"
	"sync"
	"testing"
	"time"
)

func TestClient2(t *testing.T) {

	cfg := &Config{
		Host:      host,
		BenchSize: 1000,
		UserName:  "hdfs",
		Password:  "",
	}
	client, err := NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	result, err := client.ExecuteEx("select country_name from dsp.dim_ostype limit 1500", true)
	if err != nil {
		t.Fatal(err)
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
		t.Fatal(err)
	}
	t.Log("=======", len(ids))
}

func TestClient(t *testing.T) {

	cfg := &Config{
		Host:      host,
		BenchSize: 1,
		UserName:  "hdfs",
		Password:  "",
	}
	client, err := NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	result, err := client.ExecuteEx(" SELECT count(1) as cnt FROM dsp.dim_ostype", true)
	if err != nil {
		t.Fatal(err)
	}

	var number int64
	if result.Next() {
		result.Scan(&number)
	}

	t.Log("==== TestClient: ", number)
}

func TestClient3(t *testing.T) {
	cfg := &Config{
		Host:      host,
		BenchSize: 1,
		UserName:  "hdfs",
		Password:  "",
	}
	client, err := NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Second)
	defer cancel()

	//time.Sleep(time.Duration(2) * time.Second)
	result, err := client.ExecuteSyncCtx(ctx, "SELECT count(1) FROM dsp.dim_ostype")

	if err != nil {
		t.Log("timeoout")
	} else if result != nil {
		var number int64
		if result.Next() {
			result.Scan(&number)
		}

		t.Log("==== TestClient: ", number)
	} else {
		t.Log("result is nil")
	}
}

func TestClient4(t *testing.T) {
	cfg := &Config{
		Host:      host,
		BenchSize: 1,
		UserName:  "hdfs",
		Password:  "",
	}
	client, err := NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Second)
	defer cancel()

	for i := 0; i < 5; i++ {
		//time.Sleep(time.Duration(2) * time.Second)
		result, err := client.ExecuteAsyncCtx(ctx, "SELECT count(1) FROM dsp.dim_ostype")

		if err != nil {
			t.Log("timeoout")
		} else if result != nil {
			var number int64
			if result.Next() {
				result.Scan(&number)
			}

			t.Log("==== TestClient: ", number)
		} else {
			t.Log("result is nil")
		}
	}
}

func TestClient5(t *testing.T) {
	cfg := &Config{
		Host:      host,
		BenchSize: 1,
		UserName:  "hdfs",
		Password:  "",
	}
	client, err := NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	num := 30
	var wg sync.WaitGroup
	wg.Add(num)

	for i := 0; i < num; i++ {
		go func(i int) {
			defer wg.Done()
			result, err := client.ExecuteSyncCtx(ctx, "SELECT count(1) FROM dsp.dim_ostype")
			if err != nil {
				t.Log("timeout")
			} else if result != nil {
				var number int64
				if result.Next() {
					result.Scan(&number)
				} else {
					t.Log("result is nil ")

				}
			}
		}(i)
	}
	wg.Wait()

	select {
	case <-ctx.Done():
		t.Log("timeout , cancel excute Sql")
	default:
		t.Log("success get the Sql result")
	}
}

func TestClient6(t *testing.T) {
	cfg := &Config{
		Host:      host,
		BenchSize: 1,
		UserName:  "hdfs",
		Password:  "",
	}
	client, err := NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	num := 30
	var wg sync.WaitGroup
	wg.Add(num)

	for i := 0; i < num; i++ {
		go func(i int) {
			defer wg.Done()
			result, err := client.ExecuteAsyncCtx(ctx, "SELECT count(1) FROM dsp.dim_ostype")
			if err != nil {
				t.Log("timtout")
			} else if result != nil {
				var number int64
				if result.NextCtx(ctx) {
					result.Scan(&number)
				} else {
					t.Log("result is nil ")

				}
			}
		}(i)
	}
	wg.Wait()

	select {
	case <-ctx.Done():
		t.Log("timeout , cancel excute Sql")
	default:
		t.Log("success get the Sql result")
	}
}

func TestLogHive(t *testing.T) {

	cfg := &Config{
		Host:        host,
		BenchSize:   1,
		UserName:    "root",
		Password:    "root",
		LogDetail:   true,                      //需要打开log开关
		LogInterval: Duration(1 * time.Second), //异步轮询拉取日志的频率，默认为5s
	}
	client, err := NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	//异步提交才能支持实时打印日志
	result, err := client.ExecuteEx("SELECT count(1) FROM dsp.dim_ostype", true)
	//同步提交任务日志会在job结束后打印，但是要注意hive连接的维持
	//result, err := client.ExecuteEx("SELECT count(1) FROM dsp.dim_ostype", false)
	if err != nil {
		t.Fatal(err)
	}

	var number int64
	if result.Next() {
		result.Scan(&number)
	}

	t.Log("==== TestClient: ", number)
}

func TestClient_callback(t *testing.T) {
	cfg := &Config{
		Host:      host,
		BenchSize: 1,
		UserName:  "hdfs",
		Password:  "",
	}
	client, err := NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Second)
	defer cancel()

	for i := 0; i < 5; i++ {
		//time.Sleep(time.Duration(2) * time.Second)
		err := client.AddAsyncCtx(ctx, NewContext(), "SELECT count(1) FROM dsp.dim_ostype", func(result *ExecuteResult) {
			var number int64
			if result.Next() {
				result.Scan(&number)
			}
			t.Log("==== TestClient: ", number)
		})
		if err != nil {
			t.Error(err)
		}

	}
}
