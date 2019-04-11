package hive

import (
	"math/rand"
	"sync"
	"time"
)

var (
	instance       *Client
	instanceConfig *Config
	instanceInit   sync.Once
)

var SessionID = rand.New(rand.NewSource(time.Now().UnixNano())).Int31()

func Init(cfg *Config) {
	instanceConfig = cfg
}

func Hive() *Client {
	instanceInit.Do(func() {
		if instance == nil {
			var err error
			instance, err = NewClient(instanceConfig)
			if err != nil {
				panic(err)
			}
		}
	})
	return instance
}

func SubmitAsync(statement string) (*ExecuteResult, error) {
	return Hive().ExecuteEx(statement, true)
}

func Submit(statement string) (*ExecuteResult, error) {
	return Hive().Execute(statement)
}
