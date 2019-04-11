package hive

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"crypto/md5"
	"dmp_web/go/commons/db/hive/tcliservice"
	"dmp_web/go/commons/log"
)

var (
	ErrClosing = fmt.Errorf("I'm closing, please try it later")
)

type Cli interface {
	AddPollingWithCallback(op *Operation, f func(*PollingResult))
	AddPolling(op *Operation) chan *PollingResult
	Fetch(op *Operation, o Orientation) (*SegmentResult, error)
	Execute(statement string) (*ExecuteResult, error)
	ExecuteAsyncCtx(ctx context.Context, statement string) (*ExecuteResult, error)
	ExecuteSyncCtx(ctx context.Context, statement string) (*ExecuteResult, error)
	SubmitAsync(statement string) (*ExecuteResult, error)
	SubmitAsyncCtx(ctx context.Context, statement string) (*ExecuteResult, error)
	AddAsync(dimension *HiveContext, statement string, callback func(*ExecuteResult)) error
	AddAsyncCtx(ctx context.Context, dimension *HiveContext, statement string, callback func(*ExecuteResult)) error
	StageSize() int
	ExecuteEx(statement string, async bool) (*ExecuteResult, error)
	Close()
}

type Client struct {
	cfg               *Config
	pool              *Pool
	stopChan          chan struct{}
	notifyPollingChan chan *PollItem
	polling           *Polling
	closed            int32
	logDetail         bool
	logInterval       Duration

	working  int32
	waitChan chan struct{}
}

func NewClient(cfg *Config) (*Client, error) {
	if cfg == nil {
		cfg = new(Config)
	}
	cfg.init()

	pool, err := NewPool(cfg.Host, cfg.UserName, cfg.Password, cfg.DBName, cfg.MinSessionCount, cfg.MaxSessionCount)
	if err != nil {
		return nil, err
	}
	cli := &Client{
		cfg:       cfg,
		pool:      pool,
		polling:   NewPolling(),
		waitChan:  make(chan struct{}),
		logDetail: cfg.LogDetail,
	}
	for i := 0; i < cfg.PollingThread; i++ {
		go cli.pollingLoop()
	}
	return cli, nil
}

func (c *Client) AddPollingWithCallback(op *Operation, f func(*PollingResult)) {
	c.polling.AddCallback(op, f)
}

func (c *Client) AddPolling(op *Operation) chan *PollingResult {
	return c.polling.Add(op)
}

func (c *Client) pollingLoop() {
	timeout := time.Second
	timer := time.NewTimer(timeout)

	// just make the exit quickly. Don't care about the processing tasks.
loop:
	for atomic.LoadInt32(&c.closed) == 0 {
		poll := c.polling.Poll()
		if poll == nil {
			timer.Reset(c.cfg.BackoffTime.Unwrap())
			select {
			case <-c.polling.NewPollingChan:
			case <-c.stopChan:
				break loop
			case <-timer.C:
			}
			continue
		}
		session, ok := c.pool.Require()
		if !ok {
			break loop
		}

		state, err := session.GetState(poll.Op)
		c.polling.SubStaging()
		if err != nil {
			log.Debug("session getState error : ", err)
			// invalid handle ? ignore
			c.pool.Release(session)
			continue
		}

		if !state.IsCompleted() {
			poll.MarkBackoff(c.cfg.BackoffTime.Unwrap())
			c.polling.AddItem(poll)
			c.pool.Release(session)
			continue
		}

		var result PollingResult
		if state.IsSuccess() {
			// success, schema is not required
			schema, _ := session.GetSchema(poll.Op)
			if schema == nil {
				schema = new(Schema)
			}
			result.Schema = schema
		} else {
			result.Error = state.GetError()
			// error
		}
		c.pool.Release(session)

		// wake up one block SubmitAsync if someone is waiting
		select {
		case c.waitChan <- struct{}{}:
		default:
		}

		if poll.Callback != nil {
			go poll.Callback(&result)
		}

		if poll.Chan != nil {
			poll.Chan <- &result
		}

	}
}

func (c *Client) Fetch(op *Operation, o Orientation) (*SegmentResult, error) {
	session, ok := c.pool.Require()
	if !ok {
		return nil, ErrClosing
	}

	result, err := session.FetchResult(op, o, c.cfg.BenchSize)
	c.pool.Release(session)
	if err != nil {
		return nil, err
	}
	return result, err
}
func (c *Client) ResultSlice(sql string) ([]string, error) {

	result, err := c.Execute(sql)
	if err != nil {
		return nil, err
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
		return nil, err
	}
	return ids, nil
}

func (c *Client) ResultCount(sql string) (int64, error) {

	result, err := c.Execute(sql)
	if err != nil {
		return -1, err
	}

	var count int64
	if result.Next() {
		result.Scan(&count)
	}
	if err := result.Err(); err != nil {
		return -1, err
	}
	return count, nil
}

func (c *Client) Execute(statement string) (*ExecuteResult, error) {
	return c.ExecuteEx(statement, false)
}

func (c *Client) ExecuteAsyncCtx(ctx context.Context, statement string) (*ExecuteResult, error) {

	type result struct {
		hiveResult *ExecuteResult
		err        error
	}
	retChan := make(chan *result, 1)
	go func() {
		if ret, err := c.ExecuteEx(statement, true); err != nil {
			retChan <- &result{nil, err}
		} else {
			retChan <- &result{ret, nil}
		}
	}()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case ret := <-retChan:
		if ret.err != nil {
			return nil, ret.err
		} else {
			return ret.hiveResult, nil
		}
	}
}

func (c *Client) ExecuteSyncCtx(ctx context.Context, statement string) (*ExecuteResult, error) {

	type result struct {
		hiveResult *ExecuteResult
		err        error
	}
	retChan := make(chan *result, 1)
	go func() {
		if ret, err := c.ExecuteEx(statement, false); err != nil {
			retChan <- &result{nil, err}
		} else {
			retChan <- &result{ret, nil}
		}
	}()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case ret := <-retChan:
		if ret.err != nil {
			return nil, ret.err
		} else {
			return ret.hiveResult, nil
		}
	}
}

func (c *Client) SubmitAsync(statement string) (*ExecuteResult, error) {
	return c.ExecuteEx(statement, true)
}

func (c *Client) SubmitAsyncCtx(ctx context.Context, statement string) (*ExecuteResult, error) {
	return c.ExecuteAsyncCtx(ctx, statement)
}

func (c *Client) AddAsync(dimension *HiveContext, statement string, callback func(*ExecuteResult)) error {
	if dimension.HasError() {
		log.Error("hive context has error,please check")
	}
	dimension.Add()

	ret, err := c.SubmitAsync(statement)
	if err != nil {
		err = fmt.Errorf("%v: %v", err, statement)
		dimension.Error(err)
		return err
	}

	ret.RunOnFinish(func() {
		if dimension.HasError() {
			log.Error("hive context has error,please check")
		}
		callback(ret)
		dimension.Done()
	})

	return nil
}

func (c *Client) AddAsyncCtx(ctx context.Context, dimension *HiveContext, statement string, callback func(*ExecuteResult)) error {
	if dimension.HasError() {
		log.Error("hive context has error,please check")
	}
	dimension.Add()

	ret, err := c.SubmitAsyncCtx(ctx, statement)
	if err != nil {
		err = fmt.Errorf("%v: %v", err, statement)
		dimension.Error(err)
		return err
	}

	ret.RunOnFinish(func() {
		if dimension.HasError() {
			log.Error("hive context has error,please check")
		}
		callback(ret)
		dimension.Done()
	})

	return nil
}

func (c *Client) StageSize() int {
	return int(atomic.LoadInt32(&c.working))
}

func (c *Client) checkConcurrentLimit() {
	if c.polling.Size() > c.cfg.Concurrent {
		<-c.waitChan
	}
}

func (c *Client) ExecuteEx(statement string, async bool) (*ExecuteResult, error) {
	// exceed the max of concurrent size
	c.checkConcurrentLimit()

	session, ok := c.pool.Require()
	if !ok {
		return nil, ErrClosing
	}
	defer c.pool.Release(session)

	if err, _ := session.SubmitEx("SET hive.execution.engine=spark;", false); err != nil {
		log.Error(err)
	}

	log.Debug("Execute HQL: ", statement)
	op, err := session.SubmitEx(statement, async)

	if err != nil {
		log.Errorf("ExecuteEx HQL error: %s, %s", statement, err.Error())

		return nil, err
	}
	if c.logDetail {
		go c.logResponseLog(op.Handle)
	}

	//启动了hive日志打印开关

	return newResult(c, op, statement), nil
}

func (c *Client) Close() {
	if !atomic.CompareAndSwapInt32(&c.closed, 0, 1) {
		return
	}
	c.pool.Close()
}

//打印hive日志。注意本方法会一直执行直到hive结束(成功or失败)，建议单起一个goroutine来执行本方法
func (c *Client) logResponseLog(handle *tcliservice.TOperationHandle) {

	start := time.Now()

	//每次拉取日志行数
	maxRows := 20
	req := tcliservice.NewTFetchResultsReq()
	req.OperationHandle = handle
	req.Orientation = tcliservice.TFetchOrientation(0)
	req.MaxRows = int64(maxRows)
	// The type of a fetch results request. 0 represents Query output. 1 represents Log
	req.FetchType = 1

	//如果log中定义了单独的hive-logger，则把日志打到该logger上；否则日志打到最近继承的logger上
	logger := log.Get("dmp_web/go/commons/db/hive")
	//唯一性前缀，来区分日志所属的job
	prefix := md5.Sum(handle.OperationId.GetGUID())

	session, ok := c.pool.Require()
	if !ok {
		logger.Errorf("%x # failed to require session", prefix)
		return
	}

	defer c.pool.Release(session)
	//任务结束后等待次数,防止任务结束但日志未打完的情况
	retryAfterFinished := 10
	ctx := context.Background()
	for {
		resp, err := session.thrift.FetchResults(ctx, req)
		if err := session.HasError(resp, err); err != nil {
			logger.Errorf("%x # error while fetch hive-response: %+v", prefix, err)
			return
		} else {
			len := len(resp.GetResults().GetRows())
			if resp.GetResults().IsSetBinaryColumns() {
				for _, row := range resp.GetResults().GetColumns() {
					str := row.StringVal.String()
					logger.Logf("%x # %s", prefix, str)
				}
			} else if resp.GetResults().GetRows() != nil {
				for _, row := range resp.GetResults().GetRows() {
					str := row.GetColVals()[0].StringVal.GetValue()
					logger.Logf("%x # %s", prefix, str)
				}
			}
			//一次获取的数量大于等于maxRows，则继续执行读取，防止因为等待间隔内查询结束而少打日志
			if len >= maxRows {
				continue
			}
		}
		if running, err := session.IsRunning(handle); err != nil {
			logger.Errorf("%x # failed to check running %+v", prefix, err)
			return
		} else if !running {
			if retryAfterFinished > 0 {
				retryAfterFinished -= 1
				time.Sleep(c.logInterval.Unwrap())
				continue
			}
			end := time.Now()
			//执行时间
			dur := end.Sub(start)
			logger.Logf("%x # finished. duration:%v", prefix, dur)
			return
		}
		time.Sleep(c.logInterval.Unwrap())
	}
}
