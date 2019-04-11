// Automatically generated by MockGen. DO NOT EDIT!
// Source: dmp_web/go/commons/db/hive (interfaces: Cli)

package mock_hive

import (
	context "context"
	hive "dmp_web/go/commons/db/hive"

	gomock "github.com/golang/mock/gomock"
)

// Mock of Cli interface
type MockCli struct {
	ctrl     *gomock.Controller
	recorder *_MockCliRecorder
}

// Recorder for MockCli (not exported)
type _MockCliRecorder struct {
	mock *MockCli
}

func NewMockCli(ctrl *gomock.Controller) *MockCli {
	mock := &MockCli{ctrl: ctrl}
	mock.recorder = &_MockCliRecorder{mock}
	return mock
}

func (_m *MockCli) EXPECT() *_MockCliRecorder {
	return _m.recorder
}

func (_m *MockCli) AddAsync(_param0 *hive.HiveContext, _param1 string, _param2 func(*hive.ExecuteResult)) error {
	ret := _m.ctrl.Call(_m, "AddAsync", _param0, _param1, _param2)
	ret0, _ := ret[0].(error)
	return ret0
}

func (_mr *_MockCliRecorder) AddAsync(arg0, arg1, arg2 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "AddAsync", arg0, arg1, arg2)
}

func (_m *MockCli) AddAsyncCtx(_param0 context.Context, _param1 *hive.HiveContext, _param2 string, _param3 func(*hive.ExecuteResult)) error {
	ret := _m.ctrl.Call(_m, "AddAsyncCtx", _param0, _param1, _param2, _param3)
	ret0, _ := ret[0].(error)
	return ret0
}

func (_mr *_MockCliRecorder) AddAsyncCtx(arg0, arg1, arg2, arg3 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "AddAsyncCtx", arg0, arg1, arg2, arg3)
}

func (_m *MockCli) AddPolling(_param0 *hive.Operation) chan *hive.PollingResult {
	ret := _m.ctrl.Call(_m, "AddPolling", _param0)
	ret0, _ := ret[0].(chan *hive.PollingResult)
	return ret0
}

func (_mr *_MockCliRecorder) AddPolling(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "AddPolling", arg0)
}

func (_m *MockCli) AddPollingWithCallback(_param0 *hive.Operation, _param1 func(*hive.PollingResult)) {
	_m.ctrl.Call(_m, "AddPollingWithCallback", _param0, _param1)
}

func (_mr *_MockCliRecorder) AddPollingWithCallback(arg0, arg1 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "AddPollingWithCallback", arg0, arg1)
}

func (_m *MockCli) Close() {
	_m.ctrl.Call(_m, "Close")
}

func (_mr *_MockCliRecorder) Close() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Close")
}

func (_m *MockCli) Execute(_param0 string) (*hive.ExecuteResult, error) {
	ret := _m.ctrl.Call(_m, "Execute", _param0)
	ret0, _ := ret[0].(*hive.ExecuteResult)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (_mr *_MockCliRecorder) Execute(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Execute", arg0)
}

func (_m *MockCli) ExecuteAsyncCtx(_param0 context.Context, _param1 string) (*hive.ExecuteResult, error) {
	ret := _m.ctrl.Call(_m, "ExecuteAsyncCtx", _param0, _param1)
	ret0, _ := ret[0].(*hive.ExecuteResult)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (_mr *_MockCliRecorder) ExecuteAsyncCtx(arg0, arg1 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "ExecuteAsyncCtx", arg0, arg1)
}

func (_m *MockCli) ExecuteEx(_param0 string, _param1 bool) (*hive.ExecuteResult, error) {
	ret := _m.ctrl.Call(_m, "ExecuteEx", _param0, _param1)
	ret0, _ := ret[0].(*hive.ExecuteResult)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (_mr *_MockCliRecorder) ExecuteEx(arg0, arg1 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "ExecuteEx", arg0, arg1)
}

func (_m *MockCli) ExecuteSyncCtx(_param0 context.Context, _param1 string) (*hive.ExecuteResult, error) {
	ret := _m.ctrl.Call(_m, "ExecuteSyncCtx", _param0, _param1)
	ret0, _ := ret[0].(*hive.ExecuteResult)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (_mr *_MockCliRecorder) ExecuteSyncCtx(arg0, arg1 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "ExecuteSyncCtx", arg0, arg1)
}

func (_m *MockCli) Fetch(_param0 *hive.Operation, _param1 hive.Orientation) (*hive.SegmentResult, error) {
	ret := _m.ctrl.Call(_m, "Fetch", _param0, _param1)
	ret0, _ := ret[0].(*hive.SegmentResult)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (_mr *_MockCliRecorder) Fetch(arg0, arg1 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Fetch", arg0, arg1)
}

func (_m *MockCli) StageSize() int {
	ret := _m.ctrl.Call(_m, "StageSize")
	ret0, _ := ret[0].(int)
	return ret0
}

func (_mr *_MockCliRecorder) StageSize() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "StageSize")
}

func (_m *MockCli) SubmitAsync(_param0 string) (*hive.ExecuteResult, error) {
	ret := _m.ctrl.Call(_m, "SubmitAsync", _param0)
	ret0, _ := ret[0].(*hive.ExecuteResult)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (_mr *_MockCliRecorder) SubmitAsync(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "SubmitAsync", arg0)
}

func (_m *MockCli) SubmitAsyncCtx(_param0 context.Context, _param1 string) (*hive.ExecuteResult, error) {
	ret := _m.ctrl.Call(_m, "SubmitAsyncCtx", _param0, _param1)
	ret0, _ := ret[0].(*hive.ExecuteResult)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (_mr *_MockCliRecorder) SubmitAsyncCtx(arg0, arg1 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "SubmitAsyncCtx", arg0, arg1)
}