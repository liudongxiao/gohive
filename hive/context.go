package hive

import (
	"fmt"
	"sync"
	"sync/atomic"
)

type Context interface {
	HasError() bool
	Add()
	Error(err error)
	kickWait()
	Progress() string
	Done()
	Wait() error
	Get() (int, int, int)
	GetProcessRate() int32
}

type HiveContext struct {
	total    int32
	ref      int32
	hasError int32
	err      error
	doneChan chan struct{}
	guard    sync.RWMutex
}

func NewContext() *HiveContext {
	return &HiveContext{
		doneChan: make(chan struct{}, 1),
	}
}

func (c *HiveContext) HasError() bool {
	//检查整个任务是否有出错，出错的话只是打印日志，不取消任务的执行

	return atomic.LoadInt32(&c.hasError) == 1

}

func (c *HiveContext) Add() {
	c.guard.Lock()

	c.ref++
	c.total++
	c.guard.Unlock()

}

func (c *HiveContext) Error(err error) {
	c.guard.Lock()
	if !atomic.CompareAndSwapInt32(&c.hasError, 0, 1) {
		c.guard.Unlock()
		return
	}
	c.ref--
	c.err = err
	c.guard.Unlock()
	c.kickWait()
}

func (c *HiveContext) kickWait() {
	select {
	case c.doneChan <- struct{}{}:
	default:
	}
}

func (c *HiveContext) Progress() string {
	c.guard.RLock()
	total := c.total
	ref := c.ref
	c.guard.RUnlock()
	return fmt.Sprintf("[%v/%v]",
		total-ref,
		total,
	)
}

func (c *HiveContext) Done() {
	c.guard.Lock()
	c.ref--
	if c.ref == 0 {
		c.kickWait()
	}
	c.guard.Unlock()
}

func (c *HiveContext) Wait() error {
	c.guard.RLock()
	if c.ref == 0 {
		return nil
	}
	c.guard.RUnlock()

	select {
	case <-c.doneChan:
		var err error
		c.guard.Lock()
		if c.err != nil {
			err = c.err
		}
		c.guard.Unlock()
		return err
	}
}

func (c *HiveContext) Get() (total, undone, done int32) {
	c.guard.RLock()
	total = c.total
	undone = c.ref
	c.guard.RUnlock()
	done = total - undone
	return
}

func (c *HiveContext) GetProcessRate() int32 {
	c.guard.RLock()
	defer c.guard.RUnlock()
	return c.ref / c.total

}
