package raft

// 记录其他节点 LogEntries 的状态（保存到文件中）
type Config struct {
	CommitIndex uint64  `json:"commitIndex"`
	Peers       []*Peer `json:"peers"`
}
