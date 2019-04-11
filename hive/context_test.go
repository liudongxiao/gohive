package hive

import (
	"context"
	"errors"
	"testing"
)

type Job func(context.Context) error

var ErrTest = errors.New("test err")

func TestContextNoErr(t *testing.T) {
	s := NewContext()
	ctx := context.Background()

	jobCreator := func(i int) Job {
		return func(ctx context.Context) error {
			return nil
		}
	}

	jobErrCreator := func(i int) Job {
		return func(ctx context.Context) error {
			return ErrTest
		}
	}

	jobs := make([]Job, 0, 200)
	for i := 0; i < 100; i++ {
		jobs = append(jobs, jobCreator(i))
	}
	for i := 0; i < 100; i++ {
		jobs = append(jobs, jobErrCreator(i))

	}
	for _, job := range jobs {
		s.Add()
		if err := job(ctx); err != nil {
			s.Error(err)
		}
		s.Done()
	}

	s.Wait()
	if total, undone, done := s.Get(); total == 200 && undone == 0 && done != 200 {
		t.Errorf("total = %d, undone %d, done %d ", total, undone, done)
	}
	if s.HasError() != true {
		t.Error("should have error")
	}
}

func TestContextWithGorutine(t *testing.T) {
	s := NewContext()
	ctx := context.Background()

	jobCreator := func(i int) Job {
		errC := make(chan error, 1)
		return func(ctx context.Context) error {
			go func() {
				errC <- nil
			}()
			return <-errC
		}
	}

	jobErrCreator := func(i int) Job {
		errC := make(chan error, 1)
		return func(ctx context.Context) error {
			go func() {
				errC <- ErrTest
			}()
			return <-errC
		}
	}

	jobs := make([]Job, 0, 20)
	for i := 0; i < 10; i++ {
		jobs = append(jobs, jobCreator(i))

	}
	for i := 0; i < 10; i++ {
		jobs = append(jobs, jobErrCreator(i))
	}
	for _, job := range jobs {
		s.Add()
		if err := job(ctx); err != nil {
			s.Error(err)
		}
		s.Done()
	}

	s.Wait()
	if total, undone, done := s.Get(); total == 20 && undone == 0 && done != 20 {
		t.Errorf("total = %d, undone %d, done %d ", total, undone, done)
	}
	if s.HasError() != true {
		t.Error("should have error")

	}
}
