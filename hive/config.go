package hive

import (
	"strconv"
	"time"
)

type Config struct {
	Host            string
	MinSessionCount int
	MaxSessionCount int
	UserName        string
	Password        string
	DBName          string
	PollingThread   int
	BackoffTime     Duration
	BenchSize       int
	Concurrent      int
	LogDetail       bool

	//hive日志拉取间隔时间
	LogInterval Duration
}

func (c *Config) init() {
	if c.MinSessionCount == 0 {
		c.MinSessionCount = 8
	}
	if c.MaxSessionCount == 0 {
		c.MaxSessionCount = 1024
	}
	if c.BackoffTime == 0 {
		c.BackoffTime = Duration(time.Minute)
	}
	if c.BenchSize == 0 {
		c.BenchSize = 1000
	}
	if c.PollingThread == 0 {
		c.PollingThread = 4
	}
	if c.Concurrent == 0 {
		c.Concurrent = 50
	}
	if c.LogInterval == 0 {
		c.LogInterval = Duration(5 * time.Second)
	}
}

type Duration time.Duration

func (d Duration) Unwrap() time.Duration {
	return time.Duration(d)
}

func (d Duration) String() string {
	return time.Duration(d).String()
}

func (d Duration) MarshalJSON() ([]byte, error) {
	return []byte(time.Duration(d).String()), nil
}

func (d *Duration) UnmarshalJSON(b []byte) error {
	val, err := strconv.Unquote(string(b))
	if err != nil {
		return err
	}
	duration, err := time.ParseDuration(val)
	if err != nil {
		return err
	}
	*d = Duration(duration)
	return nil
}
