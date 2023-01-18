package raft

// context 表示 server 的当前状态。
// 它在应用命令时被传递到命令中，因为服务器方法是锁定的。
type Context interface {
	Server() Server
	CurrentTerm() uint64
	CurrentIndex() uint64
	CommitIndex() uint64
}

type context struct {
	server       Server
	currentIndex uint64
	currentTerm  uint64
	commitIndex  uint64
}

var _ Context = (*context)(nil)

func (c *context) Server() Server {
	return c.server
}

func (c *context) CurrentTerm() uint64 {
	return c.currentTerm
}

func (c *context) CurrentIndex() uint64 {
	return c.currentIndex
}

func (c *context) CommitIndex() uint64 {
	return c.commitIndex
}
