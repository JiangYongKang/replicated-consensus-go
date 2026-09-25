package raft

import (
	"sync"
)

// State 是副本角色。
type State uint8

const (
	Follower State = iota + 1
	Candidate
	Leader
)

func (s State) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// Config 描述一个副本的静态配置。
type Config struct {
	ID             int
	Peers          []int // 不含自己
	ElectionBase   int   // 基础选举超时（tick 数）
	ElectionSpread int   // 按副本 ID 错开的间隔，保证超时刻度互不相同
	Heartbeat      int   // 心跳间隔（tick 数）
}

func (n *Node) clusterSize() int { return len(n.cfg.Peers) + 1 }
func (n *Node) quorum() int      { return n.clusterSize()/2 + 1 }

// electionTimeout 是每个副本固定的（不使用随机数），
// 因而选举结果在给定初始条件下完全确定、可复现。
func (n *Node) electionTimeout() int {
	return n.cfg.ElectionBase + n.cfg.ID*n.cfg.ElectionSpread
}

// readState 记录一次 ReadIndex 线性读的多数派确认进度。
type readState struct {
	term   uint64
	acks   map[int]bool
	quorum bool
	index  uint64 // 取得多数派确认那一刻的 commitIndex
}

// Node 是单个 Raft 副本。
type Node struct {
	cfg Config
	net *SimNetwork
	ps  Persister

	mu sync.Mutex

	state      State
	term       uint64
	votedFor   int
	leaderHint int

	log []LogEntry // log[0] 为哨兵：index 0, term 0

	commitIndex uint64
	lastApplied uint64

	// 领导者易失状态
	nextIndex  map[int]uint64
	matchIndex map[int]uint64

	// 候选人得票 / 领导者读确认
	votesGranted map[int]bool
	reads        map[uint64]*readState

	// 确定性计时
	electionDeadline int
	lastHeartbeat    int
	ticks            int

	machine *Machine
	stopped bool

	// drive 推进集群一个时间单位；由 Cluster 注入。
	// 提案/读循环在释放锁后调用它，使单线程模拟仍能推进时间。
	drive func()
}

// NewNode 从 Persister 恢复一个副本。状态机总是重建：
// 重启后通过日志追赶重放已提交条目，重新得到幂等缓存。
func NewNode(cfg Config, net *SimNetwork, ps Persister) (*Node, error) {
	data, err := ps.Load()
	if err != nil {
		return nil, err
	}
	n := &Node{
		cfg:        cfg,
		net:        net,
		ps:         ps,
		state:      Follower,
		term:       data.CurrentTerm,
		votedFor:   data.VotedFor,
		log:        data.Log,
		nextIndex:  make(map[int]uint64),
		matchIndex: make(map[int]uint64),
		reads:      make(map[uint64]*readState),
		machine:    NewMachine(),
	}
	if n.votedFor == 0 && n.term == 0 {
		// 全新存储：编码里 int64(-1) 能正确还原，这里仅为防御。
		n.votedFor = -1
	}
	if len(n.log) == 0 {
		n.log = []LogEntry{{Index: 0, Term: 0}}
	}
	n.resetDeadlineLocked()
	net.register(cfg.ID)
	return n, nil
}

func (n *Node) resetDeadlineLocked() {
	n.electionDeadline = n.ticks + n.electionTimeout()
}

// Tick 推进一个确定性时间单位（超时检查与心跳发送）。
func (n *Node) Tick() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		return
	}
	n.ticks++
	switch n.state {
	case Follower, Candidate:
		if n.ticks >= n.electionDeadline {
			n.startElectionLocked()
		}
	case Leader:
		if n.ticks-n.lastHeartbeat >= n.cfg.Heartbeat {
			n.lastHeartbeat = n.ticks
			n.broadcastHeartbeatLocked()
		}
	}
}

// Step 处理一条入站消息。
func (n *Node) Step(m Message) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		return
	}
	// 规则：见到更高任期立即退回追随者，绝不允许旧任期节点静默接管。
	if m.Term > n.term && (m.Type == MsgVote || m.Type == MsgAppend ||
		m.Type == MsgVoteResp || m.Type == MsgAppendResp) {
		n.becomeFollowerLocked(m.Term, -1)
	}
	switch m.Type {
	case MsgVote:
		n.handleVoteReqLocked(m)
	case MsgVoteResp:
		n.handleVoteRespLocked(m)
	case MsgAppend:
		n.handleAppendReqLocked(m)
	case MsgAppendResp:
		n.handleAppendRespLocked(m)
	}
}

// Propose 提交一条写命令并等待其被多数派提交、应用。
// 返回的错误可区分：ErrNotLeaderKind / ErrNoQuorumKind /
// ErrStaleRequestKind / ErrClosedKind；同一 (ClientID, Seq) 重放返回首次结果。
func (n *Node) Propose(cmd Command) (Result, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		return Result{}, newError(ErrClosedKind, -1, "raft: node is stopped")
	}
	// 已应用过：幂等重放，直接给首次结果，不会重复执行。
	if r, ok := n.machine.Cached(cmd.ClientID, cmd.Seq); ok {
		return r, nil
	}
	// 同一客户端更靠后的请求已经应用：这是一个过期、乱序的旧请求。
	if n.machine.MaxSeq(cmd.ClientID) > cmd.Seq {
		return Result{}, newError(ErrStaleRequestKind, n.leaderHint,
			"raft: stale request: a newer request from the same client has been applied")
	}
	if n.state != Leader {
		return Result{}, newError(ErrNotLeaderKind, n.leaderHint,
			"raft: not the leader")
	}

	// 同一条未提交请求在同一领导者上重试：复用日志中的既有条目，不重复追加。
	target := uint64(0)
	for i := len(n.log) - 1; i > int(n.commitIndex); i-- {
		if c := n.log[i].Cmd; c.ClientID == cmd.ClientID && c.Seq == cmd.Seq {
			target = n.log[i].Index
			break
		}
	}
	if target == 0 {
		e := LogEntry{Index: n.lastIndex() + 1, Term: n.term, Cmd: cmd}
		n.log = append(n.log, e)
		target = e.Index
		n.matchIndex[n.cfg.ID] = target
		n.persistLocked()
	}
	n.broadcastHeartbeatLocked()

	deadline := n.ticks + n.electionTimeout()
	origTerm := n.term
	for {
		if r, ok := n.machine.Cached(cmd.ClientID, cmd.Seq); ok {
			return r, nil // 已提交并应用
		}
		if n.state != Leader || n.term != origTerm {
			return Result{}, newError(ErrNotLeaderKind, n.leaderHint,
				"raft: leadership lost before commit; retry the same request")
		}
		if n.ticks >= deadline {
			return Result{}, newError(ErrNoQuorumKind, n.leaderHint,
				"raft: no quorum acknowledged the entry before election timeout")
		}
		n.mu.Unlock()
		n.drive()
		n.mu.Lock()
	}
}

// Read 执行线性一致读（ReadIndex 风格）：
// 先取得多数派对当前领导者任期的确认，再等到该时刻的已提交日志全部应用后读本地。
func (n *Node) Read(key string) (Result, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		return Result{}, newError(ErrClosedKind, -1, "raft: node is stopped")
	}
	if n.state != Leader {
		return Result{}, newError(ErrNotLeaderKind, n.leaderHint, "raft: not the leader")
	}

	readID := n.net.NewReadID()
	rs := &readState{term: n.term, acks: map[int]bool{n.cfg.ID: true}}
	n.reads[readID] = rs
	n.broadcastReadLocked(readID)

	deadline := n.ticks + n.electionTimeout()
	for {
		if rs.quorum {
			if rs.index == 0 {
				rs.index = n.commitIndex
			}
			if n.lastApplied >= rs.index {
				return n.machine.Lookup(key), nil
			}
		}
		if n.state != Leader || n.term != rs.term {
			return Result{}, newError(ErrNotLeaderKind, n.leaderHint,
				"raft: leadership lost during read; retry")
		}
		if n.ticks >= deadline {
			return Result{}, newError(ErrNoQuorumKind, n.leaderHint,
				"raft: no quorum confirmed leadership before election timeout")
		}
		n.mu.Unlock()
		n.drive()
		n.mu.Lock()
	}
}

func (n *Node) State() (State, uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state, n.term
}

func (n *Node) CommitIndex() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.commitIndex
}

func (n *Node) AppliedIndex() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.lastApplied
}

func (n *Node) ID() int { return n.cfg.ID }

func (n *Node) Stop() {
	n.mu.Lock()
	n.stopped = true
	n.mu.Unlock()
}
