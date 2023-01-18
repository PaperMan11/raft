package raft

import (
	"sync"
	"time"
)

type Peer struct {
	// peer中的某些方法会依赖server的状态，
	// 如peer内的appendEntries方法需要获取server的currentTerm
	server            *server
	Name              string `json:"name"`
	ConnectionString  string `json:"connectionString"` //peer的ip地址，形式为"ip:port"
	prevLogIndex      uint64
	stopChan          chan bool
	heartbeatInterval time.Duration
	lastActivity      time.Time // 记录peer的上次活跃时间
	sync.RWMutex
}

//------------------------------------------------------------------------------
//
// Constructor
//
//------------------------------------------------------------------------------

// Creates a new peer.
func newPeer(server *server, name string,
	connectionString string, heartbeatInterval time.Duration) *Peer {
	return &Peer{
		server:            server,
		Name:              name,
		ConnectionString:  connectionString,
		heartbeatInterval: heartbeatInterval,
	}
}

//------------------------------------------------------------------------------
//
// Accessors
//
//------------------------------------------------------------------------------

// Sets the heartbeat timeout
func (p *Peer) setHeartbeatInterval(duration time.Duration) {
	p.heartbeatInterval = duration
}

// --------------------------------------
// Prev log index
// --------------------------------------

// Retrieves the previous log index.
func (p *Peer) getPrevLogIndex() uint64 {
	p.RLock()
	defer p.RUnlock()
	return p.prevLogIndex
}

// Sets the previous log index.
func (p *Peer) setPrevLogIndex(value uint64) {
	p.Lock()
	defer p.Unlock()
	p.prevLogIndex = value
}

func (p *Peer) setLastActivity(now time.Time) {
	p.Lock()
	defer p.Unlock()
	p.lastActivity = now
}

//------------------------------------------------------------------------------
//
// Methods
//
//------------------------------------------------------------------------------

//--------------------------------------
// Heartbeat
//--------------------------------------

// Starts the peer heartbeat.
func (p *Peer) startHeartbeat() {
	p.stopChan = make(chan bool)
	c := make(chan bool)

	p.setLastActivity(time.Now())

	p.server.routineGroup.Add(1)
	go func() {
		defer p.server.routineGroup.Done()
		p.heartbeat(c)
	}()
	<-c
}

func (p *Peer) stopHeartbeat(flush bool) {
	p.setLastActivity(time.Time{})
	p.stopChan <- flush
}

// Listens to the heartbeat timeout and flushes an AppendEntries RPC.
func (p *Peer) heartbeat(c chan bool) {
	stopChan := p.stopChan

	c <- true
	ticker := time.NewTicker(p.heartbeatInterval)
	debugln("peer.heartbeat: ", p.Name, p.heartbeatInterval)

	for {
		select {
		case flush := <-stopChan:
			if flush {
				// before we can safely remove a node
				// we must flush the remove command to the node first
				p.flush()
				debugln("peer.heartbeat.stop.with.flush: ", p.Name)
				return
			} else {
				debugln("peer.heartbeat.stop: ", p.Name)
				return
			}
		case <-ticker.C:
			start := time.Now()
			p.flush()
			duration := time.Since(start)
			p.server.DispatchEvent(newEvent(HeartbeatEventType, duration, nil))
		}
	}
}

func (p *Peer) flush() {
	debugln("peer.heartbeat.flush: ", p.Name)
	prevLogIndex := p.getPrevLogIndex()
	term := p.server.currentTerm

	entries, prevLogTerm := p.server.log.getEntriesAfter(prevLogIndex, p.server.maxLogEntriesPerRequest)

	if entries != nil {
		p.sendAppendEntriesRequest(
			newAppendEntriesRequest(
				term, prevLogIndex, prevLogTerm,
				p.server.log.CommitIndex(), p.server.name, entries))
	} else {
		p.sendSnapshotRequest(
			newSnapshotRequest(p.server.name, p.server.snapshot))
	}
}

// LastActivity returns the last time any response was received from the peer.
func (p *Peer) LastActivity() time.Time {
	p.RLock()
	defer p.RUnlock()
	return p.lastActivity
}

//--------------------------------------
// Copying
//--------------------------------------

// Clones the state of the peer. The clone is not attached to a server and
// the heartbeat timer will not exist.
func (p *Peer) clone() *Peer {
	p.Lock()
	defer p.Unlock()
	return &Peer{
		Name:             p.Name,
		ConnectionString: p.ConnectionString,
		prevLogIndex:     p.prevLogIndex,
		lastActivity:     p.lastActivity,
	}
}

// --------------------------------------
// Append Entries
// --------------------------------------

// Sends an AppendEntries request to the peer through the transport.
func (p *Peer) sendAppendEntriesRequest(req *AppendEntriesRequest) {
	tracef("peer.append.send: %s->%s [prevLog:%v length: %v]\n",
		p.server.Name(), p.Name, req.PrevLogIndex, len(req.Entries))

	resp := p.server.Transporter().SendAppendEntriesRequest(p.server, p, req)
	if resp == nil {
		p.server.DispatchEvent(newEvent(HeartbeatIntervalEventType, p, nil))
		debugln("peer.append.timeout: ", p.server.Name(), "->", p.Name)
		return
	}
	traceln("peer.append.resp: ", p.server.Name(), "<-", p.Name)

	p.setLastActivity(time.Now())
	// If successful then update the previous log index.
	p.Lock()
	if resp.Success() {
		if len(req.Entries) > 0 {
			p.prevLogIndex = req.Entries[len(req.Entries)-1].GetIndex()

			// if peer append a log entry from the current term
			// we set append to true
			if req.Entries[len(req.Entries)-1].GetTerm() == p.server.currentTerm {
				resp.append = true
			}
		}
		traceln("peer.append.resp.success: ", p.Name, "; idx =", p.prevLogIndex)
		// If it was unsuccessful then decrement the previous log index and
		// we'll try again next time.
	} else {
		if resp.Term() > p.server.currentTerm {
			// this happens when there is a new leader comes up that this *leader* has not
			// known yet.
			// this server can know until the new leader send a ae with higher term
			// or this server finish processing this response.
			debugln("peer.append.resp.not.update: new.leader.found")
		} else if resp.Term() == req.Term && resp.CommitIndex() >= p.prevLogIndex {
			// we may miss a response from peer
			// so maybe the peer has committed the logs we just sent
			// but we did not receive the successful reply and did not increase
			// the prevLogIndex

			// peer failed to truncate the log and sent a fail reply at this time
			// we just need to update peer's prevLog index to commitIndex
			p.prevLogIndex = resp.CommitIndex()
			debugln("peer.append.resp.update: ", p.Name, "; idx =", p.prevLogIndex)
		} else if p.prevLogIndex > 0 {
			// 减少之前的日志索引，直到找到匹配的
			// 不让它低于peer的提交索引
			p.prevLogIndex--
			// 如果它不够，我们直接减少到的索引
			if p.prevLogIndex > resp.Index() {
				p.prevLogIndex = resp.Index()
			}
			debugln("peer.append.resp.decrement: ", p.Name, "; idx =", p.prevLogIndex)
		}
	}
	p.Unlock()

	// 将 peer 附加到 resp，这样 server 就可以知道它来自哪里
	resp.peer = p.Name
	p.server.sendAsync(resp)
}

// 通过 Transporter 向对端发送Snapshot请求
func (p *Peer) sendSnapshotRequest(req *SnapshotRequest) {
	debugln("peer.snap.send: ", p.Name)

	resp := p.server.Transporter().SendSnapshotRequest(p.server, p, req)
	if resp == nil {
		debugln("peer.snap.timeout: ", p.Name)
		return
	}

	debugln("peer.snap.recv: ", p.Name)
	// If successful, the peer should have been to snapshot state
	// Send it the snapshot!
	p.setLastActivity(time.Now())

	if resp.Success {
		p.sendSnapshotRecoveryRequest()
	} else {
		debugln("peer.snap.failed: ", p.Name)
		return
	}
}

// 通过 Transporter 向对端发送快照恢复请求
func (p *Peer) sendSnapshotRecoveryRequest() {
	req := newSnapshotRecoveryRequest(p.server.name, p.server.snapshot)
	debugln("peer.snap.recover.send: ", p.Name)
	resp := p.server.Transporter().SendSnapshotRecoveryRequest(p.server, p, req)

	if resp == nil {
		debugln("peer.snap.recovery.timeout: ", p.Name)
		return
	}

	p.setLastActivity(time.Now())
	if resp.Success {
		p.prevLogIndex = req.LastIndex
	} else {
		debugln("peer.snap.recovery.failed: ", p.Name)
		return
	}

	p.server.sendAsync(resp)
}

// --------------------------------------
// Vote Requests
// --------------------------------------

// send VoteRequest Request
func (p *Peer) sendVoteRequest(req *RequestVoteRequest, c chan *RequestVoteResponse) {
	debugln("peer.vote: ", p.server.Name(), "->", p.Name)
	req.peer = p
	if resp := p.server.Transporter().SendVoteRequest(p.server, p, req); resp != nil {
		debugln("peer.vote.recv: ", p.server.Name(), "<-", p.Name)
		p.setLastActivity(time.Now())
		resp.peer = p
		c <- resp
	} else {
		debugln("peer.vote.failed: ", p.server.Name(), "<-", p.Name)
	}
}
