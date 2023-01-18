package raft

const (
	StateChangeEventType  = "stateChange"
	LeaderChangeEventType = "leaderChange"
	TermChangeEventType   = "termChange"
	CommitEventType       = "commit"
	AddPeerEventType      = "addPeer"
	RemovePeerEventType   = "removePeer"

	HeartbeatIntervalEventType        = "heartbeatInterval"
	ElectionTimeoutThresholdEventType = "electionTimeoutThreshold"

	HeartbeatEventType = "heartbeat"
)

// Event 表示在Raft库中发生的操作。
// 监听器可以使用Server.AddEventListener()函数订阅事件类型。
type Event interface {
	Type() string
	Source() interface{}
	Value() interface{}
	PrevValue() interface{}
}

type event struct {
	typ       string
	source    interface{} // 当前 server
	value     interface{} // 当前 state
	prevValue interface{} // 之前 state
}

var _ Event = (*event)(nil)

func newEvent(typ string, value interface{}, prevValue interface{}) *event {
	return &event{
		typ:       typ,
		value:     value,
		prevValue: prevValue,
	}
}

// Type 事件类型
func (e *event) Type() string {
	return e.typ
}

// Source 分派事件对象
func (e *event) Source() interface{} {
	return e.source
}

// Value 与事件关联的值
func (e *event) Value() interface{} {
	return e.value
}

// PrevValue 返回与事件相关的前一个值。
func (e *event) PrevValue() interface{} {
	return e.prevValue
}
