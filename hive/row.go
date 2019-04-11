package hive

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"dmp_web/go/commons/db/hive/tcliservice"
	"dmp_web/go/commons/log"
	"runtime/debug"
)

type ExecuteResult struct {
	Op      *Operation
	cli     *Client
	Schema  *Schema
	offset  int
	rows    []*tcliservice.TRow
	hasNext bool
	err     error
	guard   sync.Mutex
	sql     string

	done int32
}

func newResult(cli *Client, op *Operation, sql string) *ExecuteResult {
	return &ExecuteResult{
		Op:      op,
		cli:     cli,
		offset:  -1,
		hasNext: true,
		sql:     sql,
	}
}

func (r *ExecuteResult) RunOnFinish(f func()) {
	r.cli.AddPollingWithCallback(r.Op, func(s *PollingResult) {
		r.err = s.Error
		r.Schema = s.Schema
		r.setDone()
		f()
	})
}

func (r *ExecuteResult) IsDone() bool {
	return atomic.LoadInt32(&r.done) == 1
}

func (r *ExecuteResult) setDone() {
	atomic.StoreInt32(&r.done, 1)
}

func (r *ExecuteResult) GetSql() string {
	r.guard.Lock()
	defer r.guard.Unlock()
	return r.sql
}

func (r *ExecuteResult) setSql(sql string) {
	r.guard.Lock()
	defer r.guard.Unlock()
	r.sql = sql
}

func (r *ExecuteResult) Wait() bool {
	r.guard.Lock()
	defer r.guard.Unlock()

	if r.IsDone() {
		return r.err == nil
	}

	ch := r.cli.AddPolling(r.Op)
	result := <-ch
	r.err = result.Error
	r.Schema = result.Schema
	r.setDone()
	return r.err == nil
}

func (r *ExecuteResult) WaitCtx(ctx context.Context) bool {
	r.guard.Lock()
	defer r.guard.Unlock()

	if r.IsDone() {
		return r.err == nil
	}

	ch := r.cli.AddPolling(r.Op)
	select {
	case result := <-ch:
		r.err = result.Error
		r.Schema = result.Schema
		r.setDone()
		return r.err == nil
	case <-ctx.Done():
		return false
	}

}

func (r *ExecuteResult) Err() error {
	return r.err
}

func (r *ExecuteResult) NextPage() bool {
	if r.offset < 0 {
		if !r.IsDone() {
			if !r.Wait() {
				return false
			}
		}
	}

	if r.nextPage() {
		r.offset = -1
		return true
	}
	return false
}

func (r *ExecuteResult) NextPageCtx(ctx context.Context) bool {
	if r.offset < 0 {
		if !r.IsDone() {
			if !r.WaitCtx(ctx) {
				return false
			}
		}
	}

	if r.nextPage() {
		r.offset = -1
		return true
	}
	return false
}
func (r *ExecuteResult) nextPage() bool {
	if !r.hasNext {
		r.err = nil
		return false
	}
	rows, hasNext, err := r.Fetch(r.offset == -1)
	if err != nil {
		r.err = err
		return false
	}
	r.rows = rows
	r.hasNext = hasNext
	return len(r.rows) > 0
}

func (r *ExecuteResult) PageLength() int {
	return len(r.rows)
}

func (r *ExecuteResult) NextInPage() bool {
	if r.offset+1 >= len(r.rows) {
		return false
	}
	r.offset++
	r.err = nil
	return true
}

func (r *ExecuteResult) Next() bool {

	if r.offset < 0 {
		if !r.IsDone() {
			if !r.Wait() {
				return false
			}
		}
		if !r.nextPage() {
			return false
		}
	}

	if r.offset+1 >= len(r.rows) {
		if !r.nextPage() {
			return false
		}
		r.offset = -1
	}

	r.offset++
	r.err = nil
	return true
}
func (r *ExecuteResult) NextCtx(ctx context.Context) bool {

	if r.offset < 0 {
		if !r.IsDone() {
			if !r.WaitCtx(ctx) {
				return false
			}
		}
		if !r.nextPage() {
			return false
		}
	}

	if r.offset+1 >= len(r.rows) {
		if !r.nextPage() {
			return false
		}
		r.offset = -1
	}

	r.offset++
	r.err = nil
	return true
}
func (r *ExecuteResult) Scan(obj ...interface{}) {
	r.ScanAll(obj)
}

func (r *ExecuteResult) Print(buf *bytes.Buffer) {
	current := r.rows[r.offset]
	for idx, val := range current.ColVals {
		buf.WriteString(r.print(val))
		if idx >= len(current.ColVals)-1 {
			break
		}
		buf.WriteString("\t")
	}
}

func (r *ExecuteResult) print(v *tcliservice.TColumnValue) string {
	switch {
	case v.IsSetBoolVal():
		return fmt.Sprint(v.GetBoolVal().GetValue())
	case v.IsSetI32Val():
		return fmt.Sprint(v.GetI32Val().GetValue())
	case v.IsSetI64Val():
		return fmt.Sprint(v.GetI64Val().GetValue())
	case v.IsSetStringVal():
		return fmt.Sprint(v.GetStringVal().GetValue())
	case v.IsSetByteVal():
		return fmt.Sprint(v.GetByteVal().GetValue())
	case v.IsSetDoubleVal():
		return fmt.Sprint(v.GetDoubleVal().GetValue())
	default:
		return ""
	}
}

func (r *ExecuteResult) ScanAll(obj []interface{}) {
	current := r.rows[r.offset]
	decodeRow(r.Schema.Columns, current, obj, r.sql)

}

func (r *ExecuteResult) Fetch(init bool) ([]*tcliservice.TRow, bool, error) {
	orientation := OrientationNext

	if init {
		orientation = OrientationFirst
	}
	result, err := r.cli.Fetch(r.Op, orientation)
	if err != nil {
		return nil, false, err
	}

	// why not result.HasMoreRows? It always return false
	// hasNext := len(result.Results.Columns) != 0
	hasNext := result.HasMoreRows

	return result.Results.Rows, hasNext, nil
}

func decodeColumnValue(typ tcliservice.TTypeId, row *tcliservice.TColumnValue, obj interface{}) (err error) {
	defer func() {
		if r := recover(); r != nil {
			var ok bool
			var stack string
			var buf bytes.Buffer
			buf.Write(debug.Stack())
			stack = buf.String()
			log.Error(stack)
			err1, ok := r.(error)
			if !ok {
				err = fmt.Errorf("panic in decodeColumnValue: %v", r)
			} else {
				err = err1
			}
		}
	}()
	switch typ {
	case tcliservice.TTypeId_STRING_TYPE:
		*(obj.(*string)) = row.StringVal.GetValue()
	case tcliservice.TTypeId_BOOLEAN_TYPE:
		*(obj.(*bool)) = row.BoolVal.GetValue()
	case tcliservice.TTypeId_TINYINT_TYPE:
		*(obj.(*int64)) = int64(row.ByteVal.GetValue())
	case tcliservice.TTypeId_SMALLINT_TYPE:
		*(obj.(*int64)) = int64(row.I16Val.GetValue())
	case tcliservice.TTypeId_INT_TYPE:
		*(obj.(*int64)) = int64(row.I32Val.GetValue())
	case tcliservice.TTypeId_BIGINT_TYPE:
		*(obj.(*int64)) = row.I64Val.GetValue()
	case tcliservice.TTypeId_FLOAT_TYPE:
		*(obj.(*float64)) = row.DoubleVal.GetValue()
	case tcliservice.TTypeId_DOUBLE_TYPE:
		*(obj.(*float64)) = row.DoubleVal.GetValue()
	default:
	}
	return
}

func decodeRow(schema []*tcliservice.TColumnDesc, row *tcliservice.TRow, obj []interface{}, sql string) {
	// prevent panic
	colval := row.ColVals[:len(obj)]

	for idx, val := range colval {
		typ := schema[idx].TypeDesc.Types[0].PrimitiveEntry.Type
		if err := decodeColumnValue(typ, val, obj[idx]); err != nil {
			log.Error(err, "\n", sql)
		}
	}
	return
}

func handlePanic() error {
	if r := recover(); r != nil {
		var ok bool
		err, ok := r.(error)
		if !ok {
			err = fmt.Errorf("pkg: %v", r)
		}
		return err
	}
	return nil
}
