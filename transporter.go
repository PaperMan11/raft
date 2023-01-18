package raft

// Transporter是允许主机应用程序将请求传输到其他节点的接口
type Transporter interface {
	SendVoteRequest(server Server, peer *Peer, req *RequestVoteRequest) *RequestVoteResponse
	SendAppendEntriesRequest(server Server, peer *Peer, req *AppendEntriesRequest) *AppendEntriesResponse
	SendSnapshotRequest(server Server, peer *Peer, req *SnapshotRequest) *SnapshotResponse
	SendSnapshotRecoveryRequest(server Server, peer *Peer, req *SnapshotRecoveryRequest) *SnapshotRecoveryResponse
}
