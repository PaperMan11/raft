package raft

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path"
	"sort"
	"sync"
	"time"
)

const (
	// state
	Stopped      = "stopped"
	Initialized  = "initialized"
	Follower     = "follower"
	Candidate    = "candidate"
	Leader       = "leader"
	Snapshotting = "snapshotting"
)

const (
	MaxLogEntriesPerRequest         = 2000 // 一次 AppendEntries 最多的 log entry
	NumberOfLogEntriesAfterSnapshot = 200  // Snapshot 后的条目
)

const (
	// DefaultHeartbeatInterval is the interval that the leader will send
	// AppendEntriesRequests to followers to maintain leadership.
	DefaultHeartbeatInterval = 50 * time.Millisecond

	DefaultElectionTimeout = 150 * time.Millisecond
)

// ElectionTimeoutThresholdPercent
// 指定服务器将发送心跳RTT太接近选举超时的警告事件的阈值。
const ElectionTimeoutThresholdPercent = 0.8

var (
	NotLeaderError      = errors.New("raft.Server: Not current leader")
	DuplicatePeerError  = errors.New("raft.Server: Duplicate peer")
	CommandTimeoutError = errors.New("raft: Command timeout")
	StopError           = errors.New("raft: Has been stopped")
)

// 服务器参与共识协议，可以作为追随者、候选人或领导者。
type Server interface {
	Name() string
	Context() interface{}
	StateMachine() StateMachine
	Leader() string
	State() string
	Path() string
	LogPath() string
	SnapshotPath(lastIndex uint64, lastTerm uint64) string
	Term() uint64
	CommitIndex() uint64
	VotedFor() string
	MemberCount() int
	QuorumSize() int
	IsLogEmpty() bool
	LogEntries() []*LogEntry
	LastCommandName() string
	GetState() string
	ElectionTimeout() time.Duration
	SetElectionTimeout(duration time.Duration)
	HeartbeatInterval() time.Duration
	SetHeartbeatInterval(duration time.Duration)
	Transporter() Transporter
	SetTransporter(t Transporter)
	AppendEntries(req *AppendEntriesRequest) *AppendEntriesResponse
	RequestVote(req *RequestVoteRequest) *RequestVoteResponse
	RequestSnapshot(req *SnapshotRequest) *SnapshotResponse
	SnapshotRecoveryRequest(req *SnapshotRecoveryRequest) *SnapshotRecoveryResponse
	AddPeer(name string, connectiongString string) error
	RemovePeer(name string) error
	Peers() map[string]*Peer
	Init() error
	Start() error
	Stop()
	Running() bool
	Do(command Command) (interface{}, error)
	TakeSnapshot() error
	LoadSnapshot() error
	AddEventListener(string, EventListener)
	FlushCommitIndex()
}

type server struct {
	*eventDispatcher // 事件侦听器

	name        string
	path        string // LogEntries/snapshot basepath
	state       string // follower/candidate/leader/stopped/initialized
	transporter Transporter
	context     interface{}
	currentTerm uint64

	votedFor   string
	log        *Log
	leader     string
	peers      map[string]*Peer // 集群中的其他节点
	mutex      sync.RWMutex
	syncedPeer map[string]bool // 对于leader来说，该成员记录了日志已经被sync到了哪些follower

	stopped           chan bool
	c                 chan *ev // 当前节点的命令通道，所有的命令都通过该channel来传递
	electionTimeout   time.Duration
	heartbeatInterval time.Duration

	snapshot *Snapshot
	// PendingSnapshot is an unfinished snapshot.
	// After the pendingSnapshot is saved to disk,
	// it will be set to snapshot and also will be
	// set to nil.
	//
	// PendingSnapshot 是未完成的快照。
	// pendingSnapshot 保存到磁盘后，
	// 它将被设置为快照，并设置为nil。
	pendingSnapshot *Snapshot

	stateMachine            StateMachine // 用于压缩解压 LogEntry
	maxLogEntriesPerRequest uint64       // 一次 AppendEntries 最多的 log entry
	connectionString        string       // ip:port
	routineGroup            sync.WaitGroup
}

// ev server 循环处理的内部事件
type ev struct {
	target      interface{} // request/command
	returnValue interface{} // 执行命令后的返回值
	c           chan error
}

//------------------------------------------------------------------------------
//
// Constructor
//
//------------------------------------------------------------------------------

// Creates a new server with a log at the given path. transporter must
// not be nil. stateMachine can be nil if snapshotting and log
// compaction is to be disabled. context can be anything (including nil)
// and is not used by the raft package except returned by
// Server.Context(). connectionString can be anything.
//
//	@path: log path
//	@connectionString: ip:port
func NewServer(name string, path string, transporter Transporter,
	stateMachine StateMachine, ctx interface{}, connectionString string) (Server, error) {
	if name == "" {
		return nil, errors.New("raft.Server: Name cannot be blank")
	}
	if transporter == nil {
		panic("raft: Transport required")
	}

	s := &server{
		name:                    name,
		path:                    path,
		transporter:             transporter,
		stateMachine:            stateMachine,
		context:                 ctx,
		state:                   Stopped,
		peers:                   make(map[string]*Peer),
		log:                     newLog(),
		c:                       make(chan *ev, 256),
		electionTimeout:         DefaultElectionTimeout,
		heartbeatInterval:       DefaultHeartbeatInterval,
		maxLogEntriesPerRequest: MaxLogEntriesPerRequest,
		connectionString:        connectionString,
	}
	s.eventDispatcher = newEventDispatcher(s)

	// setup apply function
	s.log.ApplyFunc = func(le *LogEntry, c Command) (interface{}, error) {
		// Dispatch commit event
		s.DispatchEvent(newEvent(CommitEventType, le, nil))

		// Apply command to the state machine.
		switch c := c.(type) {
		case CommandApply:
			return c.Apply(&context{
				server:       s,
				currentTerm:  s.currentTerm,
				currentIndex: s.log.internalCurrentIndex(),
				commitIndex:  s.log.commitIndex,
			})
		case deprecatedCommandApply:
			return c.Apply(s)
		default:
			return nil, fmt.Errorf("Command does not implement Apply()")
		}
	}

	return s, nil
}

//------------------------------------------------------------------------------
//
// Accessors
//
//------------------------------------------------------------------------------

// --------------------------------------
// General
// --------------------------------------

func (s *server) Name() string {
	return s.name
}

func (s *server) Path() string {
	return s.path
}

// The name of the current leader.
func (s *server) Leader() string {
	return s.leader
}

// Retrieves a copy of the peer data.
func (s *server) Peers() map[string]*Peer {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	peers := make(map[string]*Peer)
	for name, peer := range s.peers {
		peers[name] = peer
	}
	return peers
}

// Retrieves the object that transports requests.
func (s *server) Transporter() Transporter {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.transporter
}

func (s *server) SetTransporter(t Transporter) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.transporter = t
}

// Retrieves the context passed into the constructor.
func (s *server) Context() interface{} {
	return s.context
}

// Retrieves the state machine passed into the constructor.
func (s *server) StateMachine() StateMachine {
	return s.stateMachine
}

// Retrieves the log path for the server.
func (s *server) LogPath() string {
	return path.Join(s.path, "log")
}

// Retrieves the current state of the server.
func (s *server) State() string {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.state
}

// Sets the state of the server.
func (s *server) setState(state string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// Temporarily store previous values.
	prevState := s.state
	prevLeader := s.leader

	// Update state and leader.
	s.state = state
	if state == Leader {
		s.leader = s.Name()
		s.syncedPeer = make(map[string]bool)
	}

	// Dispatch state and leader change events
	s.DispatchEvent(newEvent(StateChangeEventType, s.state, prevState))

	if prevLeader != s.leader {
		s.DispatchEvent(newEvent(LeaderChangeEventType, s.leader, prevLeader))
	}
}

// Get the state of the server for debugging
func (s *server) GetState() string {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return fmt.Sprintf("Name: %s, State: %s, Term: %v, CommitedIndex: %v ", s.name, s.state, s.currentTerm, s.log.commitIndex)
}

// Retrieves the current term of the server.
func (s *server) Term() uint64 {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.currentTerm
}

// Retrieves the current commit index of the server.
func (s *server) CommitIndex() uint64 {
	s.log.mutex.RLock()
	defer s.log.mutex.RUnlock()
	return s.log.commitIndex
}

// Retrieves the name of the candidate this server voted for in this term.
func (s *server) VotedFor() string {
	return s.votedFor
}

// Retrieves whether the server's log has no entries.
func (s *server) IsLogEmpty() bool {
	return s.log.isEmpty()
}

// A list of all the log entries. This should only be used for debugging purposes.
func (s *server) LogEntries() []*LogEntry {
	s.log.mutex.RLock()
	defer s.log.mutex.RUnlock()
	return s.log.entries
}

func (s *server) LastCommandName() string {
	return s.log.lastCommandName()
}

// 检查服务器是否可提升
func (s *server) promotable() bool {
	return s.log.currentIndex() > 0
}

// --------------------------------------
// Membership
// --------------------------------------

// 获取共识中的成员服务器数量
func (s *server) MemberCount() int {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return len(s.peers) + 1
}

// 获取仲裁所需的服务器数量
func (s *server) QuorumSize() int {
	return (s.MemberCount() / 2) + 1
}

// --------------------------------------
// Election timeout
// --------------------------------------

// Retrieves the election timeout.
func (s *server) ElectionTimeout() time.Duration {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.electionTimeout
}

// Sets the election timeout.
func (s *server) SetElectionTimeout(duration time.Duration) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.electionTimeout = duration
}

// --------------------------------------
// Heartbeat timeout
// --------------------------------------

// Retrieves the heartbeat timeout.
func (s *server) HeartbeatInterval() time.Duration {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.heartbeatInterval
}

// Sets the heartbeat timeout.
func (s *server) SetHeartbeatInterval(duration time.Duration) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.heartbeatInterval = duration
	for _, peer := range s.peers {
		peer.setHeartbeatInterval(duration)
	}
}

//------------------------------------------------------------------------------
//
// Methods
//
//------------------------------------------------------------------------------

// --------------------------------------
// Initialization
// --------------------------------------

func init() {
	// Register Command
	RegisterCommand(&NOPCommand{})
	RegisterCommand(&DefaultJoinCommand{})
	RegisterCommand(&DefaultLeaveCommand{})
}

// Init初始化raft服务器。
// 如果给定路径下没有之前的日志文件，Init()将创建一个空日志文件。
// 否则，Init()将从日志文件中加载日志条目。
func (s *server) Init() error {
	if s.Running() {
		return fmt.Errorf("raft.Server: Server already running[%v]", s.state)
	}

	// Server has been initialized or server was stopped after initialized
	// If log has been initialized, we know that the server was stopped after
	// running.
	if s.state == Initialized || s.log.initialized {
		s.state = Initialized
		return nil
	}

	// Create snapshot directory if it does not exist
	err := os.Mkdir(path.Join(s.path, "snapshot"), 0700)
	if err != nil && !os.IsExist(err) {
		s.debugln("raft: Snapshot dir error: ", err)
		return fmt.Errorf("raft: Initialization error: %s", err)
	}

	if err := s.readConf(); err != nil {
		s.debugln("raft: Conf file error: ", err)
		return fmt.Errorf("raft: Initialization error: %s", err)
	}

	// Initialize the log and load it up.
	if err := s.log.open(s.LogPath()); err != nil {
		s.debugln("raft: Log error: ", err)
		return fmt.Errorf("raft: Initialization error: %s", err)
	}

	// Update the term to the last term in the log.
	_, s.currentTerm = s.log.lastInfo()
	s.state = Initialized
	return nil
}

// 启动 raft 服务器如果日志条目存在，那么如果没有接收到AEs，则允许提升到候选。
// 如果不存在日志条目，则等待来自其他节点的AEs。
// 如果不存在日志条目，并且发出了自连接命令，则立即成为leader并提交条目。
func (s *server) Start() error {
	// Exit if the server is already running.
	if s.Running() {
		return fmt.Errorf("raft.Server: Server already running[%v]", s.state)
	}

	if err := s.Init(); err != nil {
		fmt.Println(111)
		return err
	}

	s.stopped = make(chan bool)
	s.setState(Follower)

	// If no log entries exist then
	// 1. wait for AEs from another node
	// 2. wait for self-join command
	// to set itself promotable
	if !s.promotable() {
		s.debugln("start as a new raft server")
		// If log entries exist then allow promotion to candidate
		// if no AEs received.
	} else {
		s.debugln("start from previous saved state")
	}

	debugln(s.GetState())

	s.routineGroup.Add(1)
	go func() {
		defer s.routineGroup.Done()
		s.loop()
	}()

	return nil
}

func (s *server) Stop() {
	if s.State() == Stopped {
		return
	}

	close(s.stopped)

	// make sure all goroutines have stopped before we close the log
	s.routineGroup.Wait()

	s.log.close()
	s.setState(Stopped)
}

func (s *server) Running() bool {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return (s.state != Stopped && s.state != Initialized)
}

//--------------------------------------
// Term
//--------------------------------------

// updates the current term for the server.
// This is only used when a larger external term is found.
func (s *server) updateCurrentTerm(term uint64, leaderName string) {
	_assert(term > s.currentTerm,
		"upadteCurrentTerm: update is called when term is not larger than currentTerm")

	// Store previous values temporarily.
	prevTerm := s.currentTerm
	prevLeader := s.leader

	// set currentTerm = T, convert to follower (§5.1)
	// stop heartbeats before step-down
	if s.state == Leader {
		for _, peer := range s.peers {
			peer.stopHeartbeat(false)
		}
	}
	// update the term an clear vote for
	if s.state != Follower {
		s.setState(Follower)
	}

	s.mutex.Lock()
	s.currentTerm = term
	s.leader = leaderName
	s.votedFor = ""
	s.mutex.Unlock()

	// Dispatch change events
	s.DispatchEvent(newEvent(TermChangeEventType, s.currentTerm, prevTerm))

	if prevLeader != s.leader {
		s.DispatchEvent(newEvent(LeaderChangeEventType, s.leader, prevLeader))
	}
}

//--------------------------------------
// Event Loop
//--------------------------------------

//               ________
//            --|Snapshot|                 timeout
//            |  --------                  ______
// recover    |       ^                   |      |
// snapshot / |       |snapshot           |      |
// higher     |       |                   v      |     recv majority votes
// term       |    --------    timeout    -----------                        -----------
//            |-> |Follower| ----------> | Candidate |--------------------> |  Leader   |
//                 --------               -----------                        -----------
//                    ^          higher term/ |                         higher term |
//                    |            new leader |                                     |
//                    |_______________________|____________________________________ |

// The main event loop for the server
func (s *server) loop() {
	defer s.debugln("server.loop.end")

	state := s.State()

	for state != Stopped {
		s.debugln("server.loop.run", state)
		switch state {
		case Follower:
			s.followLoop()
		case Candidate:
			s.candidateLoop()
		case Leader:
			s.leaderLoop()
		case Snapshotting:
			s.snapshotLoop()
		}
		state = s.State()
	}
}

// The event loop that is run when the server is in a Follower state.
// Responds to RPCs from candidates and leaders.
// Converts to candidate if election timeout elapses without either:
//
//	1.Receiving valid AppendEntries RPC, or
//	2.Granting vote to candidate
func (s *server) followLoop() {
	since := time.Now()
	electionTimeout := s.ElectionTimeout()
	timeoutChan := afterBetween(s.ElectionTimeout(), s.ElectionTimeout()*2)

	for s.State() == Follower {
		var err error
		update := false
		select {
		case <-s.stopped:
			s.setState(Stopped)
			return
		case e := <-s.c:
			switch req := e.target.(type) {
			case JoinCommand:
				// 如果没有日志条目，并且发出了self-join命令，
				// 那么立即成为leader并提交条目。
				if s.log.currentIndex() == 0 && req.NodeName() == s.Name() {
					s.debugln("selfjoin and promote to leader")
					s.setState(Leader)
					s.processCommand(req, e)
				} else {
					err = NotLeaderError
				}
			case *AppendEntriesRequest:
				// 如果心跳太接近选举超时，则发送一个事件。
				elapsedTime := time.Since(since) // Since返回自t以来所经过的时间
				if elapsedTime > time.Duration(float64(electionTimeout)*ElectionTimeoutThresholdPercent) {
					s.DispatchEvent(newEvent(ElectionTimeoutThresholdEventType, elapsedTime, nil))
				}
				e.returnValue, update = s.processAppendEntriesRequest(req)
			case *RequestVoteRequest:
				e.returnValue, update = s.processRequestVoteRequest(req)
			case *SnapshotRequest:
				e.returnValue = s.processSnapshotRequest(req)
			default:
				err = NotLeaderError
			}
			// Callback to event.
			e.c <- err
		case <-timeoutChan:
			// only allow synced follower to promote to candidate
			if s.promotable() {
				s.setState(Candidate)
			} else {
				update = true
			}
		}

		// Converts to candidate if election timeout elapses without either:
		//   1.Receiving valid AppendEntries RPC, or
		//   2.Granting vote to candidate
		if update {
			since = time.Now()
			timeoutChan = afterBetween(s.ElectionTimeout(), s.ElectionTimeout()*2)
		}
	}
}

// The event loop that is run when the server is in a Candidate state.
func (s *server) candidateLoop() {
	// Clear leader value.
	prevLeader := s.leader
	s.leader = ""
	if prevLeader != s.leader {
		s.DispatchEvent(newEvent(LeaderChangeEventType, s.leader, prevLeader))
	}

	lastLogIndex, lastLogTerm := s.log.lastInfo()
	doVote := true
	votesGranted := 0
	var timeoutChan <-chan time.Time
	var respChan chan *RequestVoteResponse

	for s.State() == Candidate {
		if doVote {
			// Increment current term, vote for self.
			s.currentTerm++
			s.votedFor = s.name

			// Send RequestVote RPCs to all other servers.
			respChan = make(chan *RequestVoteResponse, len(s.peers))
			for _, peer := range s.peers {
				s.routineGroup.Add(1)
				go func(peer *Peer) {
					defer s.routineGroup.Done()
					peer.sendVoteRequest(newRequestVoteRequest(
						s.currentTerm, s.name, lastLogIndex, lastLogTerm),
						respChan)
				}(peer)
			}

			// Wait for either:
			//   * Votes received from majority of servers: become leader
			//   * AppendEntries RPC received from new leader: step down.
			//   * Election timeout elapses without election resolution: increment term, start new election
			//   * Discover higher term: step down (§5.1)
			votesGranted = 1
			timeoutChan = afterBetween(s.ElectionTimeout(), s.ElectionTimeout()*2)
			doVote = false
		}

		// If we received enough votes then stop waiting for more votes.
		// And return from the candidate loop
		if votesGranted == s.QuorumSize() {
			s.debugln("server.candidate.recv.enough.votes")
			s.setState(Leader)
			return
		}

		// Collect votes from peers.
		select {
		case <-s.stopped:
			s.setState(Stopped)
			return
		case resp := <-respChan:
			if success := s.processVoteResponse(resp); success {
				s.debugln("server.candidate.vote.granted: ", votesGranted)
				votesGranted++
			}
		case e := <-s.c:
			var err error
			switch req := e.target.(type) {
			case Command:
				err = NotLeaderError
			case *AppendEntriesRequest:
				e.returnValue, _ = s.processAppendEntriesRequest(req)
			case *RequestVoteRequest:
				e.returnValue, _ = s.processRequestVoteRequest(req)
			}
			// Callback to event.
			e.c <- err

		case <-timeoutChan:
			doVote = true
		}
	}
}

// The event loop that is run when the server is in a Leader state.
func (s *server) leaderLoop() {
	logIndex, _ := s.log.lastInfo()

	// Update the peers prevLogIndex to leader's lastLogIndex and start heartbeat.
	s.debugln("leaderLoop.set.PrevIndex to ", logIndex)
	for _, peer := range s.peers {
		peer.setPrevLogIndex(logIndex)
		peer.startHeartbeat()
	}

	// 当服务器成为leader后提交一个NOP 摘自Raft论文:
	// "在选举时:发送初始空AppendEntries RPCs (heartbeat)到
	// 每个服务器; 在空闲期间重复此步骤，以防止选举超时
	// (§5.2)"。心跳开始以上做“空闲”期间的工作。
	s.routineGroup.Add(1)
	go func() {
		defer s.routineGroup.Done()
		s.Do(NOPCommand{})
	}()

	// Begin to collect response from followers
	for s.State() == Leader {
		var err error
		select {
		case <-s.stopped:
			// Stop all peers before stop
			for _, peer := range s.peers {
				peer.stopHeartbeat(false)
			}
			s.setState(Stopped)
			return
		case e := <-s.c:
			switch req := e.target.(type) {
			case Command:
				s.processCommand(req, e)
				continue
			case *AppendEntriesRequest:
				e.returnValue, _ = s.processAppendEntriesRequest(req)
			case *AppendEntriesResponse:
				s.processAppendEntriesResponse(req)
			case *RequestVoteRequest:
				e.returnValue, _ = s.processRequestVoteRequest(req)
			}
			// Callback to event.
			e.c <- err
		}
	}
	s.syncedPeer = nil
}

func (s *server) snapshotLoop() {
	for s.State() == Snapshotting {
		var err error
		select {
		case <-s.stopped:
			s.setState(Stopped)
			return
		case e := <-s.c:
			switch req := e.target.(type) {
			case Command:
				err = NotLeaderError
			case *AppendEntriesRequest:
				e.returnValue, _ = s.processAppendEntriesRequest(req)
			case *RequestVoteRequest:
				e.returnValue, _ = s.processRequestVoteRequest(req)
			case *SnapshotRecoveryRequest:
				e.returnValue = s.processSnapshotRecoveryRequest(req)
			}
			// Callback to event.
			e.c <- err
		}
	}
}

// 将事件发送到事件循环中进行处理
// 该函数将等待事件实际处理后才返回
func (s *server) send(value interface{}) (interface{}, error) {
	if !s.Running() {
		return nil, StopError
	}

	event := &ev{target: value, c: make(chan error, 1)}
	select {
	case s.c <- event: // 传递给事件处理器
	case <-s.stopped:
		return nil, StopError
	}

	select {
	case <-s.stopped:
		return nil, StopError
	case err := <-event.c:
		return event.returnValue, err
	}
}

// 非阻塞发送
func (s *server) sendAsync(value interface{}) {
	if !s.Running() {
		return
	}

	event := &ev{target: value, c: make(chan error, 1)}
	// 先尝试非阻塞发送
	// 在大多数情况下，这个不应该被阻塞
	// 避免创建不必要的go协程
	select {
	case s.c <- event:
		return
	default:
	}

	s.routineGroup.Add(1)
	go func() {
		defer s.routineGroup.Done()
		select {
		case s.c <- event:
		case <-s.stopped:
		}
	}()
}

// --------------------------------------
// Membership
// --------------------------------------

func (s *server) AddPeer(name string, connectiongString string) error {
	s.debugln("server.peer.add: ", name, len(s.peers))

	// Do not allow peers to be added twice.
	if s.peers[name] != nil {
		return nil
	}

	// Skip the Peer if it has the same name as the Server
	if s.name != name {
		peer := newPeer(s, name, connectiongString, s.heartbeatInterval)

		if s.State() == Leader {
			peer.startHeartbeat()
		}

		s.peers[peer.Name] = peer

		s.DispatchEvent(newEvent(AddPeerEventType, name, nil))
	}

	// Write the configuration to file.
	s.writeConf()
	return nil
}

func (s *server) RemovePeer(name string) error {
	s.debugln("server.peer.remove: ", name, len(s.peers))

	// Skip the Peer if it has the same name as the Server
	if name != s.Name() {
		// Return error if peer doesn't exist.
		peer := s.peers[name]
		if peer == nil {
			return fmt.Errorf("raft: Peer not found: %s", name)
		}

		// Stop peer and remove it.
		if s.State() == Leader {
			// We create a go routine here to avoid potential deadlock.
			// We are holding log write lock when reach this line of code.
			// Peer.stopHeartbeat can be blocked without go routine, if the
			// target go routine (which we want to stop) is calling
			// log.getEntriesAfter and waiting for log read lock.
			// So we might be holding log lock and waiting for log lock,
			// which lead to a deadlock.
			// TODO(xiangli) refactor log lock
			s.routineGroup.Add(1)
			go func() {
				defer s.routineGroup.Done()
				peer.stopHeartbeat(true)
			}()
		}

		delete(s.peers, name)

		s.DispatchEvent(newEvent(RemovePeerEventType, name, nil))
	}

	// Write the configuration to file.
	s.writeConf()
	return nil
}

//--------------------------------------
// Append Entries
//--------------------------------------

// Appends zero or more log entry from the leader to this server.
func (s *server) AppendEntries(req *AppendEntriesRequest) *AppendEntriesResponse {
	ret, _ := s.send(req)
	resp, _ := ret.(*AppendEntriesResponse)
	return resp
}

// Processes the "append entries" request.
func (s *server) processAppendEntriesRequest(req *AppendEntriesRequest) (*AppendEntriesResponse, bool) {
	s.traceln("server.ae.process")

	if req.Term < s.currentTerm { // reject
		s.debugln("server.ae.error: stale term")
		return newAppendEntriesResponse(s.currentTerm, false,
			s.log.currentIndex(), s.log.CommitIndex()), false
	}

	if req.Term == s.currentTerm {
		_assert(s.State() != Leader, "leader.elected.at.same.term.%d\n", s.currentTerm)

		// step-down to follower when it is a candidate
		if s.state == Candidate {
			// change state to follower
			s.setState(Follower)
		}

		// discover new leader when candidate
		// save leader name when follower
		s.leader = req.LeaderName
	} else {
		// Update term and leader.
		s.updateCurrentTerm(req.Term, req.LeaderName)
	}

	// Reject if log doesn't contain a matching previous entry.
	if err := s.log.truncate(req.PrevLogIndex, req.PrevLogTerm); err != nil {
		s.debugln("server.ae.truncate.error: ", err)
		return newAppendEntriesResponse(s.currentTerm, false,
			s.log.currentIndex(), s.log.CommitIndex()), true
	}

	// Append entries to the log.
	if err := s.log.appendEntries(req.Entries); err != nil {
		s.debugln("server.ae.append.error: ", err)
		return newAppendEntriesResponse(s.currentTerm, false,
			s.log.currentIndex(), s.log.CommitIndex()), true
	}

	// Commit up to the commit index.
	if err := s.log.setCommitIndex(req.CommitIndex); err != nil {
		s.debugln("server.ae.commit.error: ", err)
		return newAppendEntriesResponse(s.currentTerm, false,
			s.log.currentIndex(), s.log.CommitIndex()), true
	}

	// once the server appended and committed all the log entries from the leader
	return newAppendEntriesResponse(s.currentTerm, true,
		s.log.currentIndex(), s.log.CommitIndex()), true
}

// Processes the "append entries" response from the peer. This is only
// processed when the server is a leader. Responses received during other
// states are dropped.
func (s *server) processAppendEntriesResponse(resp *AppendEntriesResponse) {
	// If we find a higher term then change to a follower and exit.
	if resp.Term() > s.Term() {
		s.updateCurrentTerm(resp.Term(), "")
		return
	}

	// panic response if it's not successful.
	if !resp.Success() {
		return
	}

	// if one peer successfully append a log from the leader term,
	// we add it to the synced list
	if resp.append {
		s.syncedPeer[resp.peer] = true
	}

	// Increment the commit count to make sure we have a quorum before committing.
	if len(s.syncedPeer) < s.QuorumSize() {
		return
	}

	// 确定大多数的提交索引
	var indices []uint64
	indices = append(indices, s.log.currentIndex())
	for _, peer := range s.peers {
		indices = append(indices, peer.getPrevLogIndex())
	}
	sort.Sort(sort.Reverse(uint64Slice(indices)))

	// 我们可以提交到大多数成员所追加的索引
	commitIndex := indices[s.QuorumSize()-1]
	committedIndex := s.log.commitIndex
	if commitIndex > committedIndex {
		// leader needs to do a fsync before committing log entries
		s.log.sync()
		s.log.setCommitIndex(commitIndex)
		s.debugln("commit index ", commitIndex)
	}
}

// processVoteReponse processes a vote request:
//  1. if the vote is granted for the current term of the candidate, return true
//  2. if the vote is denied due to smaller term, update the term of this server
//     which will also cause the candidate to step-down, and return false.
//  3. if the vote is for a smaller term, ignore it and return false.
func (s *server) processVoteResponse(resp *RequestVoteResponse) bool {
	if resp.VoteGranted && resp.Term == s.currentTerm {
		return true
	}

	if resp.Term > s.currentTerm {
		s.debugln("server.candidate.vote.failed")
		s.updateCurrentTerm(resp.Term, "")
	} else {
		// ignore
		s.debugln("server.candidate.vote: denied")
	}
	return false
}

//--------------------------------------
// Request Vote
//--------------------------------------

func (s *server) RequestVote(req *RequestVoteRequest) *RequestVoteResponse {
	ret, _ := s.send(req)
	resp, _ := ret.(*RequestVoteResponse)
	return resp
}

// Processes a "request vote" request.
func (s *server) processRequestVoteRequest(req *RequestVoteRequest) (*RequestVoteResponse, bool) {
	// If the request is coming from an old term then reject it.
	if req.Term < s.Term() {
		s.debugln("server.rv.deny.vote: cause stale term")
		return newRequestVoteResponse(s.currentTerm, false), false
	}

	// If the term of the request peer is larger than this node, update the term
	// If the term is equal and we've already voted for a different candidate then
	// don't vote for this candidate.
	if req.Term > s.Term() {
		s.updateCurrentTerm(req.Term, "")
	} else if s.votedFor != "" && s.votedFor != req.CandidateName {
		s.debugln("server.deny.vote: cause duplicate vote: ",
			req.CandidateName, " already vote for ", s.votedFor)
		return newRequestVoteResponse(s.currentTerm, false), false
	}

	// If the candidate's log is not at least as up-to-date as our last log then don't vote.
	lastIndex, lastTerm := s.log.lastInfo()
	if lastIndex > req.LastLogIndex || lastTerm > req.LastLogTerm {
		s.debugln("server.deny.vote: cause out of date log: ", req.CandidateName,
			"Index :[", lastIndex, "]", " [", req.LastLogIndex, "]",
			"Term :[", lastTerm, "]", " [", req.LastLogTerm, "]")

		return newRequestVoteResponse(s.currentTerm, false), false
	}
	// 如果我们做到了这一步，那么就投票并重置我们的选举时间。
	s.debugln("server.rv.vote: ", s.name, " votes for", req.CandidateName, "at term", req.Term)
	s.votedFor = req.CandidateName
	return newRequestVoteResponse(s.currentTerm, true), true
}

//--------------------------------------
// Commands
//--------------------------------------

func (s *server) Do(command Command) (interface{}, error) {
	return s.send(command)
}

// Processes a command.
// 执行命令
func (s *server) processCommand(command Command, e *ev) {
	s.debugln("server.command.process")

	// Create an entry for the command in the log.
	entry, err := s.log.createEntry(s.currentTerm, command, e)
	if err != nil {
		s.debugln("server.command.log.entry.error:", err)
		e.c <- err
		return
	}

	if err := s.log.appendEntry(entry); err != nil {
		s.debugln("server.command.log.error:", err)
		e.c <- err
		return
	}

	s.syncedPeer[s.Name()] = true
	if len(s.peers) == 0 {
		commitIndex := s.log.currentIndex()
		s.log.setCommitIndex(commitIndex)
		s.debugln("commit index", commitIndex)
	}
}

//--------------------------------------
// Log compaction
//--------------------------------------

func (s *server) TakeSnapshot() error {
	if s.stateMachine == nil {
		return errors.New("Snapshot: Cannot create snapshot. Missing state machine")
	}

	// Shortcut without lock
	// Exit if the server is currently creating a snapshot.
	if s.pendingSnapshot != nil {
		return errors.New("Snapshot: Last snapshot is not finished")
	}

	// TODO: 获取锁，不允许再提交
	// 这将在完成重构heartbeat后执行
	s.debugln("take.snapshot")

	lastIndex, lastTerm := s.log.commitInfo()

	// check if there is log has been committed since the
	// last snapshot.
	if lastIndex == s.log.startIndex {
		return nil
	}

	path := s.SnapshotPath(lastIndex, lastTerm)
	// Attach snapshot to pending snapshot and save it to disk.
	s.pendingSnapshot = &Snapshot{lastIndex, lastTerm, nil, nil, path}

	// 保存状态机当前状态
	state, err := s.stateMachine.Save()
	if err != nil {
		return err
	}

	// Clone the list of peers.
	peers := make([]*Peer, 0, len(s.peers)+1)
	for _, peer := range s.peers {
		peers = append(peers, peer.clone())
	}
	peers = append(peers, &Peer{Name: s.Name(), ConnectionString: s.connectionString})
	// Attach snapshot to pending snapshot and save it to disk.
	s.pendingSnapshot.Peers = peers
	s.pendingSnapshot.State = state
	s.saveSnapshot()

	// 我们在快照之后保留一些日志条目
	// 我们不希望将整个快照发送到稍慢的机器
	if lastIndex-s.log.startIndex > NumberOfLogEntriesAfterSnapshot {
		compactIndex := lastIndex - NumberOfLogEntriesAfterSnapshot
		compactTerm := s.log.getEntry(compactIndex).Term()
		s.log.compact(compactIndex, compactTerm)
	}
	return nil
}

// 检索服务器的日志路径
func (s *server) saveSnapshot() error {
	if s.pendingSnapshot == nil {
		return errors.New("pendingSnapshot.is.nil")
	}

	// Write snapshot to disk.
	if err := s.pendingSnapshot.save(); err != nil {
		return err
	}

	// Swap the current and last snapshots.
	tmp := s.snapshot
	s.snapshot = s.pendingSnapshot

	// 如果前一个快照有变化，则删除
	if tmp != nil && !(tmp.LastIndex == s.snapshot.LastIndex && tmp.LastTerm == s.snapshot.LastTerm) {
		tmp.remove()
	}
	s.pendingSnapshot = nil

	return nil
}

func (s *server) SnapshotPath(lastIndex uint64, lastTerm uint64) string {
	return path.Join(s.path, "snapshot", fmt.Sprintf("%v_%v.ss", lastTerm, lastIndex))
}

func (s *server) RequestSnapshot(req *SnapshotRequest) *SnapshotResponse {
	ret, _ := s.send(req)
	resp, _ := ret.(*SnapshotResponse)
	return resp
}

func (s *server) processSnapshotRequest(req *SnapshotRequest) *SnapshotResponse {
	// If the follower’s log contains an entry at the snapshot’s last index with a term
	// that matches the snapshot’s last term, then the follower already has all the
	// information found in the snapshot and can reply false.
	entry := s.log.getEntry(req.LastIndex)
	if entry != nil && entry.Term() == req.LastTerm {
		return newSnapshotResponse(false) // 本节点不需要 snapshot
	}

	s.setState(Snapshotting)
	return newSnapshotResponse(true)
}

func (s *server) SnapshotRecoveryRequest(req *SnapshotRecoveryRequest) *SnapshotRecoveryResponse {
	ret, _ := s.send(req)
	resp, _ := ret.(*SnapshotRecoveryResponse)
	return resp
}

func (s *server) processSnapshotRecoveryRequest(req *SnapshotRecoveryRequest) *SnapshotRecoveryResponse {
	// Recover state sent from request.
	if err := s.stateMachine.Recovery(req.State); err != nil {
		panic("cannot recover from previous state")
	}

	// Recover the cluster configuration.
	s.peers = make(map[string]*Peer)
	for _, peer := range req.Peers {
		s.AddPeer(peer.Name, peer.ConnectionString)
	}

	// Update log state.
	s.currentTerm = req.LastTerm
	s.log.updateCommitIndex(req.LastIndex)

	// Create local snapshot.
	s.pendingSnapshot = &Snapshot{req.LastIndex, req.LastTerm, req.Peers, req.State, s.SnapshotPath(req.LastIndex, req.LastTerm)}
	s.saveSnapshot()

	// Clear the previous log entries.
	s.log.compact(req.LastIndex, req.LastTerm)
	return newSnapshotRecoveryResponse(req.LastTerm, true, req.LastIndex)
}

// Load a snapshot at restart
func (s *server) LoadSnapshot() error {
	// Open snapshot/ directory.
	dir, err := os.OpenFile(path.Join(s.path, "snapshot"), os.O_RDONLY, 0)
	if err != nil {
		s.debugln("cannot.open.snapshot: ", err)
		return err
	}

	// 获取所有快照的列表
	filenames, err := dir.Readdirnames(-1)
	if err != nil {
		dir.Close()
		panic(err)
	}
	dir.Close()

	if len(filenames) == 0 {
		s.debugln("no.snapshot.to.load")
		return nil
	}

	// 获取最新的快照
	sort.Strings(filenames)
	snapshotPath := path.Join(s.path, "snapshot", filenames[len(filenames)-1])

	// Read snapshot data.
	file, err := os.OpenFile(snapshotPath, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	defer file.Close()

	// Check checksum.
	var checksum uint32
	n, err := fmt.Fscanf(file, "%08x\n", &checksum)
	if err != nil {
		return err
	} else if n != 1 {
		return errors.New("checksum.err: bad.snapshot.file")
	}

	// Load remaining snapshot contents.
	b, err := io.ReadAll(file)
	if err != nil {
		return err
	}

	// Generate checksum.
	byteChecksum := crc32.ChecksumIEEE(b)
	if uint32(checksum) != byteChecksum {
		s.debugln(checksum, " ", byteChecksum)
		return errors.New("bad snapshot file")
	}

	// Decode snapshot.
	if err = json.Unmarshal(b, &s.snapshot); err != nil {
		s.debugln("unmarshal.snapshot.error: ", err)
		return err
	}

	// Recover snapshot into state machine.
	if err = s.stateMachine.Recovery(s.snapshot.State); err != nil {
		s.debugln("recovery.snapshot.error: ", err)
		return err
	}

	// Recover cluster configuration.
	for _, peer := range s.snapshot.Peers {
		s.AddPeer(peer.Name, peer.ConnectionString)
	}

	// Update log state.
	s.log.startTerm = s.snapshot.LastTerm
	s.log.startIndex = s.snapshot.LastIndex
	s.log.updateCommitIndex(s.snapshot.LastIndex)

	return err
}

//--------------------------------------
// Config File
//--------------------------------------

// Flushes commit index to the disk.
// So when the raft server restarts, it will commit upto the flushed commitIndex.
func (s *server) FlushCommitIndex() {
	s.debugln("server.conf.update")
	s.writeConf()
}

func (s *server) writeConf() {
	peers := make([]*Peer, len(s.peers))

	i := 0
	for _, peer := range s.peers {
		peers[i] = peer.clone()
		i++
	}

	r := &Config{
		CommitIndex: s.log.commitIndex,
		Peers:       peers,
	}

	b, _ := json.Marshal(r)

	confPath := path.Join(s.path, "conf")
	tmpConfPath := path.Join(s.path, "conf.tmp")

	err := writeFileSynced(tmpConfPath, b, 0600)
	if err != nil {
		panic(err)
	}

	os.Rename(tmpConfPath, confPath)
}

func (s *server) readConf() error {
	confPath := path.Join(s.path, "conf")
	s.debugln("readConf.open", confPath)

	// open conf file
	b, err := os.ReadFile(confPath)
	if err != nil { // 初始化时，并没有该目录
		return nil
	}

	conf := &Config{}

	if err = json.Unmarshal(b, conf); err != nil {
		return err
	}

	s.log.updateCommitIndex(conf.CommitIndex)
	return nil
}

//--------------------------------------
// Debugging
//--------------------------------------

func (s *server) debugln(v ...interface{}) {
	if logLevel > Debug {
		debugf("[%s Term:%d] %s", s.name, s.Term(), fmt.Sprintln(v...))
	}
}

func (s *server) traceln(v ...interface{}) {
	if logLevel > Trace {
		tracef("[%s] %s", s.name, fmt.Sprintln(v...))
	}
}
