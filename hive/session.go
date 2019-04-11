package hive

import (
	"context"
	"fmt"
	"net"
	"runtime"
	"strings"
	"sync/atomic"

	"git.apache.org/thrift.git/lib/go/thrift"

	"dmp_web/go/commons/db/hive/tcliservice"
	"dmp_web/go/commons/log"
)

type Session struct {
	thrift  *tcliservice.TCLIServiceClient
	session *tcliservice.TSessionHandle
	closed  int32
}

// Operationa 封装了一个 tcliservice.TOperationHandle， 包含 4 个对象
// OperationId struct 里面有一个本次查询的 ID
// OperationType struct
// HasResultSet bool 是否有返回结果
// ModifiedRowCount float64 有 BUG 好像一直是 nil
type Operation struct {
	Handle *tcliservice.TOperationHandle
}

type Statuser interface {
	GetStatus() *tcliservice.TStatus
}

type OperationState tcliservice.TGetOperationStatusResp

func NewSession(host, username, password, dbname string) (*Session, error) {
	transport, err := thrift.NewTSocket(host)
	if err != nil {
		return nil, err
	}
	if err := transport.Open(); err != nil {
		return nil, err
	}

	protocol := thrift.NewTBinaryProtocolFactoryDefault()
	client := tcliservice.NewTCLIServiceClientFactory(transport, protocol)

	req := tcliservice.NewTOpenSessionReq()
	if username != "" {
		req.Username = &username
		req.Password = &password
	}
	req.ClientProtocol = tcliservice.TProtocolVersion_HIVE_CLI_SERVICE_PROTOCOL_V5
	if dbname != "" {
		if req.Configuration == nil {
			req.Configuration = make(map[string]string)
		}
		// ref:
		// hive/service/src/java/org/apache/hive/service/cli/session/HiveSessionImpl.java
		req.Configuration["use:"] = dbname
	}
	ctx := context.Background()
	session, err := client.OpenSession(ctx, req)
	if err := (*Session).HasError(nil, session, err); err != nil {
		return nil, err
	}

	return &Session{
		thrift:  client,
		session: session.SessionHandle,
	}, nil
}

func (o OperationState) IsCompleted() bool {
	state := (*tcliservice.TGetOperationStatusResp)(&o).GetOperationState()
	switch state {
	case tcliservice.TOperationState_CANCELED_STATE:
	case tcliservice.TOperationState_CLOSED_STATE:
	case tcliservice.TOperationState_FINISHED_STATE:
	case tcliservice.TOperationState_ERROR_STATE:
	default:
		return false
	}
	return true
}

func (o OperationState) GetError() error {
	if o.ErrorMessage == nil {
		return nil
	}
	return fmt.Errorf(*o.ErrorMessage)
}

func (o OperationState) IsSuccess() bool {
	state := (*tcliservice.TGetOperationStatusResp)(&o).GetOperationState()
	switch state {
	case tcliservice.TOperationState_FINISHED_STATE:
		return true
	}
	return false
}

type Schema tcliservice.TTableSchema

type PollingResult struct {
	Schema *Schema
	Error  error
}

type Orientation tcliservice.TFetchOrientation

const (
	OrientationNext     = Orientation(tcliservice.TFetchOrientation_FETCH_NEXT)
	OrientationPrior    = Orientation(tcliservice.TFetchOrientation_FETCH_PRIOR)
	OrientationRelative = Orientation(tcliservice.TFetchOrientation_FETCH_RELATIVE)
	OrientationAbsolute = Orientation(tcliservice.TFetchOrientation_FETCH_ABSOLUTE)
	OrientationFirst    = Orientation(tcliservice.TFetchOrientation_FETCH_FIRST)
	OrientationLast     = Orientation(tcliservice.TFetchOrientation_FETCH_LAST)
)

type SegmentResult struct {
	Results     *tcliservice.TRowSet
	HasMoreRows bool
}

func (s *Session) FetchResult(op *Operation, o Orientation, size int) (*SegmentResult, error) {
	ctx := context.Background()
	req := tcliservice.NewTFetchResultsReq()
	req.OperationHandle = op.Handle
	req.Orientation = tcliservice.TFetchOrientation(o)
	req.MaxRows = int64(size)
	resp, err := s.thrift.FetchResults(ctx, req)
	if err := s.HasError(resp, err); err != nil {
		return nil, err
	}

	// WHT resp.HasMoreRows 有问题，不管什么情况最后返回的结果都是 false
	// 现在只能通过一个返回结果的行数和最大返回数据 size 做对比
	// 如果比 size 小，说明肯定已经没有结果了，这次 fetch 就是最后的结果了
	// 如果大于等于 size 我就只能假设还有数据，再尝试获取一次
	var hasMoreRows bool = true

	if len(resp.Results.GetRows()) < size {
		hasMoreRows = false
	} else {
		hasMoreRows = true
	}

	return &SegmentResult{
		Results:     resp.Results,
		HasMoreRows: hasMoreRows,
	}, nil
}

func (s *Session) GetSchema(op *Operation) (*Schema, error) {
	ctx := context.Background()
	req := tcliservice.NewTGetResultSetMetadataReq()
	req.OperationHandle = op.Handle
	resp, err := s.thrift.GetResultSetMetadata(ctx, req)
	if err := s.HasError(resp, err); err != nil {
		return nil, err
	}
	return (*Schema)(resp.Schema), nil
}

func (s *Session) GetState(o *Operation) (*OperationState, error) {
	ctx := context.Background()
	req := tcliservice.NewTGetOperationStatusReq()
	req.OperationHandle = o.Handle
	resp, err := s.thrift.GetOperationStatus(ctx, req)
	if err := s.HasError(resp, err); err != nil {
		return nil, err
	}

	return (*OperationState)(resp), nil
}

func (s *Session) Submit(statement string) (*Operation, error) {
	return s.SubmitEx(statement, false)
}

func (s *Session) SubmitEx(statement string, async bool) (*Operation, error) {
	ctx := context.Background()
	req := tcliservice.NewTExecuteStatementReq()
	req.SessionHandle = s.session
	req.Statement = statement
	req.RunAsync = async
	resp, err := s.thrift.ExecuteStatement(ctx, req)
	if err := s.HasError(resp, err); err != nil {
		return nil, err
	}

	return &Operation{
		Handle: resp.OperationHandle,
	}, nil
}

func (s *Session) CloseOperation(o *Operation) error {
	ctx := context.Background()
	req := tcliservice.NewTCloseOperationReq()
	req.OperationHandle = o.Handle
	resp, err := s.thrift.CloseOperation(ctx, req)
	if err := s.HasError(resp, err); err != nil {
		return err
	}

	return nil
}

func (s *Session) IsClosed() bool {
	return atomic.LoadInt32(&s.closed) == 1
}

func (s *Session) Close() error {
	if !atomic.CompareAndSwapInt32(&s.closed, 0, 1) {
		return nil
	}

	ctx := context.Background()
	req := tcliservice.NewTCloseSessionReq()
	req.SessionHandle = s.session
	resp, err := s.thrift.CloseSession(ctx, req)

	// 直接处理 error ，因为 HasError 中会调用 Close，防止递归调用
	if err != nil {
		if err1, ok := err.(thrift.TTransportException); ok {
			if err, ok := err1.Err().(*net.OpError); ok {
				if !err.Temporary() {
					return err
				}
			}
		} else {
			errStr := err.Error()
			if strings.Contains(errStr, "broken pipe") {
				return nil
			}
			if strings.Contains(errStr, "EOF") {
				return nil
			}
		}
		if err := GetRespError(resp); err != nil {
			return err
		}
	}
	return nil
}

func (s *Session) Ping() error {
	ctx := context.Background()
	resp, err := s.thrift.GetInfo(ctx, &tcliservice.TGetInfoReq{
		s.session, tcliservice.TGetInfoType_CLI_DBMS_NAME,
	})
	if err := s.HasError(resp, err); err != nil {
		return err
	}
	return nil
}

func (s *Session) HasError(p Statuser, err error) error {
	if err != nil {
		if isUnrecoveredErr(err) {
			stackInfo := make([]byte, 10240)
			n := runtime.Stack(stackInfo, false)
			log.Errorf("hive session will be closed because it have a unrecovered error: %s\nstack:\n%s", err.Error(), string(stackInfo[:n]))
			if s != nil {
				s.Close()
			}
		}
		return err
	}
	if err := GetRespError(p); err != nil {
		return err
	}
	return nil
}

func isUnrecoveredErr(err error) bool {
	if err1, ok := err.(thrift.TTransportException); ok {
		if err, ok := err1.Err().(*net.OpError); ok {
			if !err.Temporary() {
				return true
			}
		}
	} else {
		errStr := err.Error()
		if strings.Contains(errStr, "broken pipe") {
			return true
		}
		if strings.Contains(errStr, "EOF") {
			return true
		}
	}
	return false
}

func getRawError(cerr error) error {
	if err, ok := cerr.(thrift.TTransportException); ok {
		cerr = err.Err()
	}
	return cerr
}

func GetRespError(p Statuser) error {
	status := p.GetStatus()
	if isSuccess(status) {
		return nil
	}
	info := ""
	if status.ErrorMessage != nil {
		info = *status.ErrorMessage
	}
	return fmt.Errorf(info)
}

func isSuccess(p *tcliservice.TStatus) bool {
	status := p.GetStatusCode()
	return status == tcliservice.TStatusCode_SUCCESS_STATUS || status == tcliservice.TStatusCode_SUCCESS_WITH_INFO_STATUS
}

//判断指定handle是否仍在执行状态;true=正在执行
func (s *Session) IsRunning(handle *tcliservice.TOperationHandle) (bool, error) {
	ctx := context.Background()
	req := tcliservice.NewTGetOperationStatusReq()
	req.OperationHandle = handle
	// The type of a fetch results request. 0 represents Query output. 1 represents Log
	if resp, err := s.thrift.GetOperationStatus(ctx, req); err != nil {
		return false, err
	} else {
		//返回状态
		return resp.GetOperationState() == tcliservice.TOperationState_RUNNING_STATE, nil
	}
}
