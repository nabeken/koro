// Code generated by MockGen. DO NOT EDIT.
// Source: shard.go

// Package koro is a generated GoMock package.
package koro

import (
	reflect "reflect"

	dynamodbstreams "github.com/aws/aws-sdk-go/service/dynamodbstreams"
	gomock "github.com/golang/mock/gomock"
)

// MockDynamoDBStreamer is a mock of DynamoDBStreamer interface.
type MockDynamoDBStreamer struct {
	ctrl     *gomock.Controller
	recorder *MockDynamoDBStreamerMockRecorder
}

// MockDynamoDBStreamerMockRecorder is the mock recorder for MockDynamoDBStreamer.
type MockDynamoDBStreamerMockRecorder struct {
	mock *MockDynamoDBStreamer
}

// NewMockDynamoDBStreamer creates a new mock instance.
func NewMockDynamoDBStreamer(ctrl *gomock.Controller) *MockDynamoDBStreamer {
	mock := &MockDynamoDBStreamer{ctrl: ctrl}
	mock.recorder = &MockDynamoDBStreamerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockDynamoDBStreamer) EXPECT() *MockDynamoDBStreamerMockRecorder {
	return m.recorder
}

// DescribeStream mocks base method.
func (m *MockDynamoDBStreamer) DescribeStream(arg0 *dynamodbstreams.DescribeStreamInput) (*dynamodbstreams.DescribeStreamOutput, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DescribeStream", arg0)
	ret0, _ := ret[0].(*dynamodbstreams.DescribeStreamOutput)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// DescribeStream indicates an expected call of DescribeStream.
func (mr *MockDynamoDBStreamerMockRecorder) DescribeStream(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DescribeStream", reflect.TypeOf((*MockDynamoDBStreamer)(nil).DescribeStream), arg0)
}

// GetRecords mocks base method.
func (m *MockDynamoDBStreamer) GetRecords(arg0 *dynamodbstreams.GetRecordsInput) (*dynamodbstreams.GetRecordsOutput, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetRecords", arg0)
	ret0, _ := ret[0].(*dynamodbstreams.GetRecordsOutput)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetRecords indicates an expected call of GetRecords.
func (mr *MockDynamoDBStreamerMockRecorder) GetRecords(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetRecords", reflect.TypeOf((*MockDynamoDBStreamer)(nil).GetRecords), arg0)
}

// GetShardIterator mocks base method.
func (m *MockDynamoDBStreamer) GetShardIterator(arg0 *dynamodbstreams.GetShardIteratorInput) (*dynamodbstreams.GetShardIteratorOutput, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetShardIterator", arg0)
	ret0, _ := ret[0].(*dynamodbstreams.GetShardIteratorOutput)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetShardIterator indicates an expected call of GetShardIterator.
func (mr *MockDynamoDBStreamerMockRecorder) GetShardIterator(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetShardIterator", reflect.TypeOf((*MockDynamoDBStreamer)(nil).GetShardIterator), arg0)
}

// ListStreams mocks base method.
func (m *MockDynamoDBStreamer) ListStreams(arg0 *dynamodbstreams.ListStreamsInput) (*dynamodbstreams.ListStreamsOutput, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ListStreams", arg0)
	ret0, _ := ret[0].(*dynamodbstreams.ListStreamsOutput)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ListStreams indicates an expected call of ListStreams.
func (mr *MockDynamoDBStreamerMockRecorder) ListStreams(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListStreams", reflect.TypeOf((*MockDynamoDBStreamer)(nil).ListStreams), arg0)
}
