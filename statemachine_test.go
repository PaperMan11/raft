package raft

import "github.com/stretchr/testify/mock"

type mockStateMachine struct {
	mock.Mock
}

func (m *mockStateMachine) Save() ([]byte, error) {
	// Called告诉模拟对象已经调用了一个方法，并获得要返回的参数数组
	args := m.Called()
	return args.Get(0).([]byte), args.Error(1)
}

func (m *mockStateMachine) Recovery(b []byte) error {
	args := m.Called(b)
	return args.Error(0)
}
