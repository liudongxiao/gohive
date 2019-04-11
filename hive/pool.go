package hive

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"dmp_web/go/commons/log"
)

var (
	ErrPoolIsFull   = fmt.Errorf("pool is full")
	ErrPoolIsClosed = fmt.Errorf("pool is closed")
)

type Pool struct {
	host        string
	username    string
	password    string
	dbName      string
	PoolSize    int32
	Chan        chan *Session
	sessionSize int32
	closed      int32
	wg          sync.WaitGroup
}

func NewPool(host, username, password, dbname string, minSize, size int) (*Pool, error) {
	p := &Pool{
		host:     host,
		username: username,
		password: password,
		dbName:   dbname,
		PoolSize: int32(size),
		Chan:     make(chan *Session, size),
	}
	for i := 0; i < minSize; i++ {
		if err := p.addSession(); err != nil {
			return nil, err
		}
	}
	return p, nil
}

func (p *Pool) Close() {
	if !atomic.CompareAndSwapInt32(&p.closed, 0, 1) {
		return
	}
	close(p.Chan)
	for session := range p.Chan {
		p.closeSession(session)
	}

	// 检查退出状态
	checkChan := make(chan bool, 1)
	go func() {
		for {
			select {
			case <-checkChan:
				return
			default:
			}
			time.Sleep(time.Second * 1)
			log.Debugf("pool wait for %d session to be close", atomic.LoadInt32(&p.sessionSize))
		}
	}()
	// waiting sessions which is already required out to close by `Release`
	p.wg.Wait()
	checkChan <- true
}

func (p *Pool) CreateSession() (*Session, error) {
	session, err := NewSession(p.host, p.username, p.password, p.dbName)
	return session, err
}

func (p *Pool) addSession() error {
	if atomic.LoadInt32(&p.closed) == 1 {
		return ErrPoolIsClosed
	}

	if atomic.AddInt32(&p.sessionSize, 1) > p.PoolSize {
		atomic.AddInt32(&p.sessionSize, -1)
		return ErrPoolIsFull
	}

	session, err := NewSession(p.host, p.username, p.password, p.dbName)
	if err != nil {
		atomic.AddInt32(&p.sessionSize, -1)
		return err
	}
	p.wg.Add(1)
	p.Chan <- session
	return nil
}

func (p *Pool) closeSession(s *Session) {
	s.Close()
	atomic.AddInt32(&p.sessionSize, -1)
	p.wg.Done()
}

func (p *Pool) Available() int {
	return len(p.Chan)
}

// ok will be false when Pool is closing (p.Chan is closed)
func (p *Pool) Require() (session *Session, ok bool) {
	var tries int
	for {
		tries++
		if atomic.LoadInt32(&p.closed) == 1 {
			return nil, false
		}

		select {
		case session, ok = <-p.Chan:
			if !ok {
				log.Errorf("add session fail : closed pool")
				return nil, false
			} else if session.IsClosed() || session.Ping() != nil {
				p.closeSession(session)
				if err := p.addSession(); err != nil {
					log.Warnf("add session fail :%v", err)
					return nil, false
				}
			} else {
				return
			}
		case <-time.After(time.Duration(5+tries) * time.Second):
		}

	}
	return
}

func (p *Pool) TryRequire() *Session {
	select {
	case s := <-p.Chan:
		return s
	default:
		return nil
	}
}

func (p *Pool) Release(s *Session) {
	if atomic.LoadInt32(&p.closed) == 1 {
		log.Debugf("pool is closed ,just close session")
		p.closeSession(s)
		return
	}
	p.Chan <- s
}
