# Raft

> 源码链接：[https://github.com/goraft/raft](https://github.com/goraft/raft)

**总状态图**：
```go
              ________
           --|Snapshot|                 timeout
           |  --------                  ______
recover    |       ^                   |      |
snapshot / |       |snapshot           |      |
higher     |       |                   v      |     recv majority votes
term       |    --------    timeout    -----------                        -----------
           |-> |Follower| ----------> | Candidate |--------------------> |  Leader   |
                --------               -----------                        -----------
                   ^          higher term/ |                         higher term |
                   |            new leader |                                     |
                   |_______________________|____________________________________ |
```

## 1 command
```go
var commandTypes map[string]Command // 存储 command

func newCommand(name string, data []byte) (Command, error)  // 新建 command
func RegisterCommand(command Command)                       // 注册 command 到 commandTypes
```

## 2 log_entry 
```go
// log_entry是客户端发起的command存储在日志文件中的内容
type LogEntry struct {
	pb       *protobuf.LogEntry // proto 编码的 log_entry
	Position int64              // position in the log file
	log      *Log               // 预写日志
	event    *ev                // 日志事件
}

func (*LogEntry).Command() []byte
func (*LogEntry).CommandName() string
func (*LogEntry).Decode(r io.Reader) (int, error) // Decode from log file
func (*LogEntry).Encode(w io.Writer) (int, error) // Encode into log file
func (*LogEntry).Index() uint64
func (*LogEntry).Term() uint64
```

## 3 log
```go
// log 持久存储的日志项的集合。
type Log struct {
	ApplyFunc   func(*LogEntry, Command) (interface{}, error) // 日志被应用至状态机的方法
	file        *os.File
	path        string
	entries     []*LogEntry // 内存日志项缓存
	commitIndex uint64      // 日志提交点，小于该提交点的日志均已经被应用至状态机
	mutex       sync.RWMutex
	startIndex  uint64  // 缓存中第一个日志项索引
	startTerm   uint64
	initialized bool    // Log 是否初始化
}

func (*Log).CommitIndex() uint64 // 返回 log_entry 提交点
func (*Log).appendEntries(entries []*protobuf.LogEntry) error // 添加log_entries
func (*Log).appendEntry(entry *LogEntry) error
func (*Log).close()
func (*Log).commitInfo() (index uint64, term uint64) // 返回提交点的 index/term
func (*Log).compact(index uint64, term uint64) error // 将 log 写入文件
func (*Log).containsEntry(index uint64, term uint64) bool // log_entry 是否存在于内存
func (*Log).createEntry(term uint64, command Command, e *ev) (*LogEntry, error) // 新建 log_entry
func (*Log).currentIndex() uint64
func (*Log).currentTerm() uint64
func (*Log).flushCommitIndex()
func (*Log).getEntriesAfter(index uint64, maxLogEntriesPerRequest uint64) ([]*LogEntry, uint64) // 返回给定 index 后的 log_entries，没有返回 nil
func (*Log).getEntry(index uint64) *LogEntry
func (*Log).internalCurrentIndex() uint64 // 获取当前 index (无锁)
func (*Log).isEmpty() bool
func (*Log).lastCommandName() string // 返回最后 log_entry 的 command name
func (*Log).lastInfo() (index uint64, term uint64) // 返回最后 log_entry 的 index/term
func (*Log).nextIndex() uint64 // return next index
func (*Log).open(path string) error // 打开日志文件并读取现有条目，日志可以保持打开状态，并继续向日志末尾追加条目
func (*Log).setCommitIndex(index uint64) error // 设置提交点
func (*Log).sync() error // sync to disk
func (*Log).truncate(index uint64, term uint64) error // 将日志截断为给定的索引和项，这仅在索引处的日志未提交时才有效
func (*Log).updateCommitIndex(index uint64)
func (*Log).writeEntry(entry *LogEntry, w io.Writer) (int64, error) // append log_entry
```

## 4 append_entries

### 4.1 AppendEntriesRequest
```go
// The request sent to a server to append entries to the log.
type AppendEntriesRequest struct {
    Term         uint64
    PrevLogIndex uint64
    PrevLogTerm  uint64
    CommitIndex  uint64
    LeaderName   string
    Entries      []*protobuf.LogEntry
}

func (*AppendEntriesRequest).Decode(r io.Reader) (int, error) 
func (*AppendEntriesRequest).Encode(w io.Writer) (int, error) 
```

### 4.2 AppendEntriesResponse
```go
type AppendEntriesResponse struct {
    pb     *protobuf.AppendEntriesResponse // response
    peer   string   // 目标节点
    append bool     // 是否追加成功
}

func (*AppendEntriesResponse).CommitIndex() uint64 // 对应节点 commit index
func (*AppendEntriesResponse).Decode(r io.Reader) (int, error) 
func (*AppendEntriesResponse).Encode(w io.Writer) (int, error) 
func (*AppendEntriesResponse).Index() uint64    // 对应节点最后的 index
func (*AppendEntriesResponse).Success() bool    // 是否追加成功
func (*AppendEntriesResponse).Term() uint64     // 对应节点最后的 term
```

## 5 request_vote

### 5.1 RequestVoteRequest
```go
// The request sent to a server to vote for a candidate to become a leader. 
// 发送到服务器的请求，以投票给候选人成为领导者。
type RequestVoteRequest struct {
    peer          *Peer
    Term          uint64
    LastLogIndex  uint64
    LastLogTerm   uint64
    CandidateName string
}

func (*RequestVoteRequest).Decode(r io.Reader) (int, error) 
func (*RequestVoteRequest).Encode(w io.Writer) (int, error) 
```

### 5.2 RequestVoteResponse
```go
type RequestVoteResponse struct {
    peer        *Peer
    Term        uint64
    VoteGranted bool // 是否投票
}

func (*RequestVoteResponse).Decode(r io.Reader) (int, error)
func (*RequestVoteResponse).Encode(w io.Writer) (int, error)
```

## 6 snapshot

### 6.1 Snapshot 
```go
// Snapshot 表示系统当前状态的内存表示
type Snapshot struct {
    LastIndex uint64 `json:"lastIndex"`
    LastTerm  uint64 `json:"lastTerm"`

    // Cluster configuration.
    Peers []*Peer `json:"peers"`
    State []byte  `json:"state"` // state machine
    Path  string  `json:"path"`
}

func (*Snapshot).remove() error // deletes the snapshot file
func (*Snapshot).save() error   // writes the snapshot to file
```

### 6.2 SnapshotRequest
```go
// 设置接收快照状态请求
type SnapshotRequest struct {
    LeaderName string
    LastIndex  uint64
    LastTerm   uint64
}

func (*SnapshotRequest).Decode(r io.Reader) (int, error)
func (*SnapshotRequest).Encode(w io.Writer) (int, error)
```

### 6.3 SnapshotResponse
```go
type SnapshotResponse struct {
    Success bool `json:"success"` // 成功后设置 server 状态为 snapshoting，后续继续接收 snapshot
}

func (*SnapshotResponse).Decode(r io.Reader) (int, error)
func (*SnapshotResponse).Encode(w io.Writer) (int, error)
```

### 6.4 SnapshotRecoveryRequest
```go
// state machine request
type SnapshotRecoveryRequest struct {
    LeaderName string
    LastIndex  uint64
    LastTerm   uint64
    Peers      []*Peer
    State      []byte // state machine
}

func (*SnapshotRecoveryRequest).Decode(r io.Reader) (int, error)
func (*SnapshotRecoveryRequest).Encode(w io.Writer) (int, error)
```

### 6.5 SnapshotRecoveryResponse
```go
// Recover state sent from request. then send response
type SnapshotRecoveryResponse struct {
    Term        uint64
    Success     bool
    CommitIndex uint64
}

func (*SnapshotRecoveryResponse).Decode(r io.Reader) (int, error)
func (*SnapshotRecoveryResponse).Encode(w io.Writer) (int, error)
```

## 7 state_machine
```go
// StateMachine 是允许 server 保存和恢复状态机的接口
type StateMachine interface {
	Save() ([]byte, error)
	Recovery([]byte) error
}
```

## 8 Transporter
```go
// Transporter 是允许 server 将请求传输到其他节点的接口。
type Transporter interface {
	SendVoteRequest(server Server, peer *Peer, req *RequestVoteRequest) *RequestVoteResponse
	SendAppendEntriesRequest(server Server, peer *Peer, req *AppendEntriesRequest) *AppendEntriesResponse
	SendSnapshotRequest(server Server, peer *Peer, req *SnapshotRequest) *SnapshotResponse
	SendSnapshotRecoveryRequest(server Server, peer *Peer, req *SnapshotRecoveryRequest) *SnapshotRecoveryResponse
}
```

## 9 event

### 9.1 Event
```go
// raft 中的事件接口
type Event interface {
	Type() string
	Source() interface{}
	Value() interface{}
	PrevValue() interface{}
}
```

```go 
// 用于定义事件实例
type event struct {
    typ       string        // 事件类型
    source    interface{}   // 分派事件的对象
    value     interface{}   // 事件关联的当前值 
    prevValue interface{}   // 事件相关的前一个值
}

func (*event).PrevValue() interface{}
func (*event).Source() interface{}
func (*event).Type() string
func (*event).Value() interface{}

// 目前定义的事件种类
StateChangeEventType                = "stateChange"
LeaderChangeEventType               = "leaderChange"
TermChangeEventType                 = "termChange"
CommitEventType                     = "commit"
AddPeerEventType                    = "addPeer"
RemovePeerEventType                 = "removePeer"
HeartbeatIntervalEventType          = "heartbeatInterval"
ElectionTimeoutThresholdEventType   = "electionTimeoutThreshold"
HeartbeatEventType                  = "heartbeat"
```

### 9.2 eventDispatcher(观察者模式)
```go 
// EventListener is a function that can receive event notifications.
type EventListener func(Event)

// EventListeners represents a collection of individual listeners.
type eventListeners []EventListener

// eventDispatcher 负责管理指定事件的侦听器，并将事件通知分派给这些侦听器
type eventDispatcher struct {
    sync.RWMutex
    source    interface{}
    listeners map[string]eventListeners
}

func (*eventDispatcher).AddEventListener(typ string, listener EventListener)
func (*eventDispatcher).DispatchEvent(e Event) // 当事件触发时，遍历执行这些侦听器
func (*eventDispatcher).RemoveEventListener(typ string, listener EventListener)
```

## 10 server

### 10.1 Peer
```go
type Peer struct {
    server            *server                           // peer中的某些方法会依赖server的状态，如peer内的appendEntries方法需要获取server的currentTerm
    Name              string  `json:"name"`             // 主机名
    ConnectionString  string  `json:"connectionString"` // peer的ip地址，形式为”ip:port”
    prevLogIndex      uint64                            // 该节点上一个 log_entry index
    stopChan          chan bool                         // stop channel
    heartbeatInterval time.Duration                     // ticker   
    lastActivity      time.Time                         // 记录peer的上次活跃时间
    sync.RWMutex            
}

func (*Peer).LastActivity() time.Time
func (*Peer).clone() *Peer  // Clones the state of the peer. The clone is not attached to a server and the heartbeat timer will not exist.
func (*Peer).flush()    // flushes an AppendEntries RPC or Snapshot RPC
func (*Peer).getPrevLogIndex() uint64
func (*Peer).heartbeat(c chan bool) // Listens to the heartbeat timeout and flushes an AppendEntries RPC.
func (*Peer).sendAppendEntriesRequest(req *AppendEntriesRequest)
func (*Peer).sendSnapshotRecoveryRequest()
func (*Peer).sendSnapshotRequest(req *SnapshotRequest)
func (*Peer).sendVoteRequest(req *RequestVoteRequest, c chan *RequestVoteResponse)
func (*Peer).setHeartbeatInterval(duration time.Duration)
func (*Peer).setLastActivity(now time.Time)
func (*Peer).setPrevLogIndex(value uint64)
func (*Peer).startHeartbeat()   // Starts the peer heartbeat.
func (*Peer).stopHeartbeat(flush bool)  // Stops the peer heartbeat.
```

### 10.2 server
```go
// 服务器参与共识协议，可以作为追随者、候选人或领导者。
type Server interface {
	Name() string   // server name          
	Context() interface{}   
	StateMachine() StateMachine // 获取 state machine
	Leader() string // 获取当前共识协议的 leader
	State() string  // 获取当前节点的角色状态
	Path() string   // 存储路径
	LogPath() string    // log Path
	SnapshotPath(lastIndex uint64, lastTerm uint64) string  
	Term() uint64
	CommitIndex() uint64
	VotedFor() string   // 投票给了谁
	MemberCount() int   // 当前共识协议成员数
	QuorumSize() int    // 检索仲裁所需的服务器数量 
	IsLogEmpty() bool
	LogEntries() []*LogEntry
	LastCommandName() string
	GetState() string
	ElectionTimeout() time.Duration
	SetElectionTimeout(duration time.Duration)
	HeartbeatInterval() time.Duration
	SetHeartbeatInterval(duration time.Duration)
	Transporter() Transporter   // 获取 server 直接通信对象
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
	Do(command Command) (interface{}, error)    // 客户命令执行入口
	TakeSnapshot() error    // 创建 snapshot
	LoadSnapshot() error    // 加载 snapshot
	AddEventListener(string, EventListener)
	FlushCommitIndex()
} 
```

```go
type server struct {
    *eventDispatcher

    name        string
    path        string
    state       string // 每个节点总是处于以下状态的一种：follower、candidate、leader
    transporter Transporter
    context     interface{}
    currentTerm uint64

    votedFor   string
    log        *Log
    leader     string
    peers      map[string]*Peer // raft中每个节点需要了解其他节点信息，尤其是leader节点
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

    stateMachine            StateMachine // 压缩、解压 log
    maxLogEntriesPerRequest uint64

    connectionString string // ip:port

    routineGroup sync.WaitGroup
}
```

## 11 关键流程

### 11.1 客户端请求
```go
func (s *server) Do(command Command) (interface{}, error) {
	return s.send(command)
}

// Sends an event to the event loop to be processed. The function will wait
// until the event is actually processed before returning.
func (s *server) send(value interface{}) (interface{}, error) {
	if !s.Running() {
		return nil, StopError
	}

	event := &ev{target: value, c: make(chan error, 1)}
	select {
	case s.c <- event:
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
```

`Do()` 作为客户端命令的入口。`send()` 会将命令写入 server 的 channel 中，然后等待命令的完成。而 server 作为 leader 角色启动时会开启 `leaderLoop()` 来处理所有的用户命令：

```go
func (s *server) leaderLoop() {
    logIndex, _ := s.log.lastInfo()
    ......
    // Begin to collect response from followers
    for s.State() == Leader {
        select {
        case <-s.stopped:
            ......
        case e := <-s.c:
            switch req := e.target.(type) {
            // 代表客户端命令
            case Command:
                s.processCommand(req, e)
                continue
            ......
            }
        }
    }
}
```

`processCommand()` 处理如下：

```go
// Processes a command.
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
		s.debugln("commit index ", commitIndex)
	}
}
```

这里首先会根据客户端的命令创建新的 log_entry，并将 log_entry 追加到 log 中，如果过程中由任何错误，就将这个错误写入 `e.c：e.c <- err`，这样等待在该 channel 的客户端就会收到通知，立即返回。

如果没有错误，这时候客户端还是处于等待状态的，这是因为虽然该 Command 被 leader 节点成功处理了，但是该 Command 的日志还没有被同步至大多数 Follow 节点，因此该 Command 也就无法被提交，所以发起该 Command 的客户端依然等在那，Command 被提交，这在后面的日志同步过程中会有所体现。


### 11.2 日志同步
日志同步是通过 leader 对 follow 的 heartbeat 时完成的：

```go
// Listens to the heartbeat timeout and flushes an AppendEntries RPC.
func (p *Peer) heartbeat(c chan bool) {
    stopChan := p.stopChan
    c <- true
    ticker := time.Tick(p.heartbeatInterval)
 
    for {
        select {
        case flush := <-stopChan:
            if flush {
                // before we can safely remove a node
                // we must flush the remove command to the node first
                p.flush()
                return
            } else {
                return
            }
 
        case <-ticker:
            start := time.Now()
            p.flush()
            duration := time.Now().Sub(start)
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
        p.sendAppendEntriesRequest(newAppendEntriesRequest(term, prevLogIndex, prevLogTerm, p.server.log.CommitIndex(), p.server.name, entries))
    } else {
        p.sendSnapshotRequest(newSnapshotRequest(p.server.name, p.server.snapshot))
    }
}
```

核心的逻辑是将 leader 上的日志通过构造一个 AppendEntriesRequest 发送给从节点，当然只同步那些 Follower 上还没有的日志，即 prevLogIndex 以后的 log entries。若 log entries 为空将会发送 snapshot。

```go
// Sends an AppendEntries request to the peer through the transport.
func (p *Peer) sendAppendEntriesRequest(req *AppendEntriesRequest) {
    resp := p.server.Transporter().SendAppendEntriesRequest(p.server, p, req)
    if resp == nil {
        p.server.DispatchEvent(newEvent(HeartbeatIntervalEventType, p, nil))
        return
    }
    p.setLastActivity(time.Now())
    // If successful then update the previous log index.
    p.Lock()
    if resp.Success() {
        ......
    }
    ......
    resp.peer = p.Name
    // Send response to server for processing.
    p.server.sendAsync(resp)
}
```

这里会将 Follower 的**心跳的响应**继续发送给 server。server 会在 `leaderLoop()` 中处理该类消息：

```go
func (s *server) leaderLoop() {
    logIndex, _ := s.log.lastInfo()
    ......
    // Begin to collect response from followers
    for s.State() == Leader {
        select {
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
```


处理 Follower 的响应在函数 `processAppendEntriesResponse()` 中：

```go
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
    if resp.append == true {
        s.syncedPeer[resp.peer] = true
    }
 
    if len(s.syncedPeer) < s.QuorumSize() {
        return
    }
    // Determine the committed index that a majority has.
    var indices []uint64
    indices = append(indices, s.log.currentIndex())
    for _, peer := range s.peers {
        indices = append(indices, peer.getPrevLogIndex())
    }
    sort.Sort(sort.Reverse(uint64Slice(indices)))
 
    commitIndex := indices[s.QuorumSize()-1]
    committedIndex := s.log.commitIndex
 
    if commitIndex > committedIndex {
        s.log.sync()
        s.log.setCommitIndex(commitIndex)
    }
}
```

这里会判断如果多数的 Follower 都已经同步日志了，那么就可以检查所有的 Follower 此时的日志点，并根据 log index 排序，leader 会算出这些 Follower 的提交点，然后提交，调用 `setCommitIndex()`。

```go
// Updates the commit index and writes entries after that index to the stable storage.
func (l *Log) setCommitIndex(index uint64) error {
    l.mutex.Lock()
    defer l.mutex.Unlock()
 
    // this is not error any more after limited the number of sending entries
    // commit up to what we already have
    if index > l.startIndex+uint64(len(l.entries)) {
        index = l.startIndex + uint64(len(l.entries))
    }
    if index < l.commitIndex {
        return nil
    }
 
    for i := l.commitIndex + 1; i <= index; i++ {
        entryIndex := i - 1 - l.startIndex
        entry := l.entries[entryIndex]
 
        l.commitIndex = entry.Index()
 
        // Decode the command.
        command, err := newCommand(entry.CommandName(), entry.Command())
        if err != nil {
            return err
        }
        returnValue, err := l.ApplyFunc(entry, command)
        if entry.event != nil {
            entry.event.returnValue = returnValue
            entry.event.c <- err
        }
        _, isJoinCommand := command.(JoinCommand)
        if isJoinCommand {
            return nil
        }
    }
    return nil
}
```

这里的提交主要是设置好 commitIndex，并且将日志项中的 Command 通过 `ApplyFunc()` 应用到状态机。最后，判断这个 LogEntry 是不是由客户直接发起的，如果是，那么还需要将状态机的处理结果通过 event.c 返回给客户端，这样，客户端就可以返回了。

### 11.3 选主

以 leader 角色运行时，会周期性的给 Follower 发送心跳，心跳的作用有二：一方面，Follower通过心跳确认Leader此时还是活着的；第二，Leader通过心跳将自身的日志同步发送给Follower。

但是，如果 Follower 在超过一定时间后没有收到 Leader 的心跳信息，就认定 Leader 可能离线，于是，该 Follower 就会变成 Candidate，发起一次选主，通知其他节点开始为我投票。

```go
func (s *server) followerLoop() {
    since := time.Now()
    electionTimeout := s.ElectionTimeout()
    timeoutChan := afterBetween(s.ElectionTimeout(), s.ElectionTimeout()*2)
 
    for s.State() == Follower {
        var err error
        update := false
        select {
        ......
        // 超过一定时间未收到请求
        case <-timeoutChan:
            if s.promotable() {
                // 状态变为Candidate
                s.setState(Candidate)
            } else {
                update = true
            }
        }
    }
    ......
}
 
// The main event loop for the server
func (s *server) loop() {
    defer s.debugln("server.loop.end")
 
    state := s.State()
 
    for state != Stopped {
        switch state {
        case Follower:
            s.followerLoop()
        // 状态变为Candidate后，进入candidateLoop
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
```

当节点状态由 Follower 变为 Candidate 后，就会进入 `candidateLoop()` 来触发一次选主过程。

```go
func (s *server) candidateLoop() {
    for s.State() == Candidate {
        if doVote {
            s.currentTerm++
            s.votedFor = s.name
            // 向所有其他节点发起Vote请求
            respChan = make(chan *RequestVoteResponse, len(s.peers))
            for _, peer := range s.peers {
                s.routineGroup.Add(1)
                go func(peer *Peer) {
                    defer s.routineGroup.Done()
                            
                    peer.sendVoteRequest(newRequestVoteRequest(s.currentTerm, s.name, lastLogIndex, lastLogTerm), respChan)
                    }(peer)
                }
                // 自己给自己投一票
                votesGranted = 1
                timeoutChan = afterBetween(s.ElectionTimeout(), s.ElectionTimeout()*2)
                doVote = false
            }
            // 如果多数节点同意我作为Leader，设置新状态
            if votesGranted == s.QuorumSize() {
                s.setState(Leader)
                return
        }
    
        // 等待其他节点的选主请求的响应
        select {
        case <-s.stopped:
            s.setState(Stopped)
            return
    
        case resp := <-respChan:
            if success := s.processVoteResponse(resp); success {
                votesGranted++
            }
        ......
        case <-timeoutChan:
            // 如果再一次超时了，重新发起选主请求
            doVote = true
        }
    }
}
```

上面描述了一个 Follower 节点变为 Candidate 后，如何发起一次选主，接下来看看一个节点在收到其他节点发起的选主请求后的处理，在函数 `processRequestVoteRequest()`：

```go
// Processes a "request vote" request.
func (s *server) processRequestVoteRequest(req *RequestVoteRequest) (*RequestVoteResponse, bool) {
    if req.Term < s.Term() {
        return newRequestVoteResponse(s.currentTerm, false), false
    }
    if req.Term > s.Term() {
        s.updateCurrentTerm(req.Term, "")
    } else if s.votedFor != "" && s.votedFor != req.CandidateName {
        return newRequestVoteResponse(s.currentTerm, false), false
    }
 
    lastIndex, lastTerm := s.log.lastInfo()
    if lastIndex > req.LastLogIndex || lastTerm > req.LastLogTerm {
        return newRequestVoteResponse(s.currentTerm, false), false
    }
    s.votedFor = req.CandidateName
    return newRequestVoteResponse(s.currentTerm, true), true
}
```

接受一个远程节点的选主请求需要满足以下条件：
1. 远程节点的 term 必须要大于等于当前节点的 term；
1. 远程节点的 log 必须比当前节点的更新；
1. 当前节点的 term 和远程节点的选主请求的 term 如果一样且当前节点未给任何其他节点投出自己的选票。

### 11.4 节点变更
在 Raft 协议中，节点的变更也是作为一个客户的命令通过一致性协议统一管理：也就是说，节点变更命令被写入 Leader 的日志，然后再由 Leader 同步到 Follower，最后如果多数 Follower 成功写入该日志，主节点提交该日志。

在 Go-Raft 中，存在两种节点变更命令：DefaultJoinCommand 和 DefaultLeaveCommand，对于这两种命令的处理关键在于这两个命令的 `Apply()` 方法，如下：

```go
func (c *DefaultJoinCommand) Apply(server Server) (interface{}, error) {
    err := server.AddPeer(c.Name, c.ConnectionString)
    return []byte("join"), err
}
 
func (c *DefaultLeaveCommand) Apply(server Server) (interface{}, error) {
    err := server.RemovePeer(c.Name)
    return []byte("leave"), err
}
```

增删节点最终的提交方法是 `AddPeer()` / `RemovePeer()`:

```go
func (s *server) AddPeer(name string, connectiongString string) error {
    if s.peers[name] != nil {
        return nil
    }
 
    if s.name != name {
        peer := newPeer(s, name, connectiongString, s.heartbeatInterval)
        // 如果是主上新增一个peer，那还需要启动后台协程发送
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
 
// Removes a peer from the server.
func (s *server) RemovePeer(name string) error {
    // Skip the Peer if it has the same name as the Server
    if name != s.Name() {
        // Return error if peer doesn't exist.
        peer := s.peers[name]
        if peer == nil {
            return fmt.Errorf("raft: Peer not found: %s", name)
        }
        // 如果是Leader，停止给移除节点的心跳协程
        if s.State() == Leader {
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
```

### 11.5 Snapshot
根据 Raft 论文描述，随着系统运行，存储命令的日志文件会一直增长，为了避免这种情况，论文中引入了 Snapshot。Snapshot 的出发点很简单：淘汰掉那些无用的日志项，那么问题就来了：哪些日志项是无用的，可以丢弃？如何丢弃无用日志项？
- 如果某个日志项中存储的用户命令(Command)已经被提交到状态机中，那么它就被视为无用的，可以被清理；
- 因为日志的提交是按照index顺序执行的，因此，只要知道当前副本的提交点(commit index)，那么在此之前的所有日志项必然也已经被提交了，因此，这个提交点之前（包括该提交点）的日志都可以被删除。实现上，只要将提交点之后的日志写入新的日志文件，再删除老的日志文件，就大功告成了；
- 最后需要注意的一点是：在回收日志文件之前，必须要对当前的系统状态机进行保存，否则，状态机数据丢失以后，又删了日志，状态真的就无法恢复了。

goraft 的 Snapshot 是由应用主动触发的，调用其内部函数 `TakeSnapshot()`：

```go
func (s *server) TakeSnapshot() error {
    ......
    lastIndex, lastTerm := s.log.commitInfo()
    ......
    path := s.SnapshotPath(lastIndex, lastTerm)
    s.pendingSnapshot = &Snapshot{lastIndex, lastTerm, nil, nil, path}
    // 首先应用保存状态机当前状态
    state, err := s.stateMachine.Save()
    if err != nil {
        return err
    }
 
    // 准备Snapshot状态：包括当前日志的index，当前peer等
    peers := make([]*Peer, 0, len(s.peers)+1)
    for _, peer := range s.peers {
        peers = append(peers, peer.clone())
    }
    s.pendingSnapshot.Peers = peers
    s.pendingSnapshot.State = state
    s.saveSnapshot()
 
    // 最后，回收日志项:s.log.compact()
    // 我们在快照之后保留一些日志条目。  
    // 我们不希望将整个快照发送到稍慢的机器 
    if lastIndex-s.log.startIndex > NumberOfLogEntriesAfterSnapshot {
        compactIndex := lastIndex - NumberOfLogEntriesAfterSnapshot
        compactTerm := s.log.getEntry(compactIndex).Term()
        s.log.compact(compactIndex, compactTerm)
    }
    return nil
}
```

Snapshot 以后重新获取 log 文件，调用 `compact()`：

```go
// compact the log before index (including index)
func (l *Log) compact(index uint64, term uint64) error {
	var entries []*LogEntry

	l.mutex.Lock()
	defer l.mutex.Unlock()

	if index == 0 {
		return nil
	}
	// nothing to compaction
	// the index may be greater than the current index if
	// we just recovery from on snapshot
	if index >= l.internalCurrentIndex() {
		entries = make([]*LogEntry, 0)
	} else {
		// get all log entries after index
		entries = l.entries[index-l.startIndex:]
	}

	// create a new log file and add all the entries
	new_file_path := l.path + ".new"
	file, err := os.OpenFile(new_file_path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		position, _ := l.file.Seek(0, os.SEEK_CUR)
		entry.Position = position

		if _, err = entry.Encode(file); err != nil {
			file.Close()
			os.Remove(new_file_path)
			return err
		}
	}
	file.Sync()

	old_file := l.file

	// rename the new log file
	err = os.Rename(new_file_path, l.path)
	if err != nil {
		file.Close()
		os.Remove(new_file_path)
		return err
	}
	l.file = file

	// close the old log file
	old_file.Close()

	// compaction the in memory log
	l.entries = entries
	l.startIndex = index
	l.startTerm = term
	return nil
}
```
