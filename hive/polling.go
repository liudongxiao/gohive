package hive

import (
	"container/list"
	"sync"
	"time"
)

type Polling struct {
	list *list.List
	sync.Mutex
	NewPollingChan chan struct{}
	staging        int
}

type PollItem struct {
	Op       *Operation
	backoff  time.Time
	Callback func(*PollingResult)
	Chan     chan *PollingResult
}

func (p *PollItem) MarkBackoff(d time.Duration) {
	p.backoff = time.Now().Add(d)
}

func NewPolling() *Polling {
	return &Polling{
		list:           list.New(),
		NewPollingChan: make(chan struct{}, 1),
	}
}

func (p *Polling) Size() int {
	p.Lock()
	size := p.list.Len() + p.staging
	p.Unlock()
	return size
}

func (p *Polling) SubStaging() {
	p.Lock()
	p.staging--
	p.Unlock()
}

func (p *Polling) Poll() (item *PollItem) {
	now := time.Now()
	p.Lock()
	for elem := p.list.Front(); elem != nil; elem = elem.Next() {
		if now.After(elem.Value.(*PollItem).backoff) {
			item = elem.Value.(*PollItem)
			p.list.Remove(elem)
			break
		}
	}
	if item != nil {
		p.staging++
	}
	p.Unlock()
	return item
}

func (p *Polling) AddItem(item *PollItem) {
	p.Lock()
	p.list.PushBack(item)
	p.Unlock()

	select {
	case p.NewPollingChan <- struct{}{}:
	default:
	}
}

func (p *Polling) AddCallback(op *Operation, cb func(*PollingResult)) {
	item := &PollItem{
		Op:       op,
		Callback: cb,
	}
	p.AddItem(item)
	return
}

func (p *Polling) Add(op *Operation) chan *PollingResult {
	item := &PollItem{
		Op:   op,
		Chan: make(chan *PollingResult, 1),
	}
	p.AddItem(item)
	return item.Chan
}
