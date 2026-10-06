package raft

import (
	"context"
	"hash/fnv"
	"sync"
)

// Logger 与 slog/log 的 Printf 风格兼容；测试中可替换为 t.Logf。
type Logger interface {
	Printf(format string, args ...any)
}

type discardLogger struct{}

func (discardLogger) Printf(string, ...any) {}

// Config 描述一个副本的静态配置。
type Config struct {
	ID               string
	Peers            []string // 含自身，顺序在所有副本上必须一致
	Storage          Storage
	SM               StateMachine
	Transport        Transport
	Sched            *Scheduler
	Clock            Clock
	Log              Logger
	ElectionMin      int64 // 选举随机区间下界（虚拟毫秒）
	ElectionMax      int64
	Heartbeat        int64 // 心跳周期
	ProposalDeadline int64 // 等待多数确认的上限
	RandSeed         int64
}

// proposal 跟踪一个在途客户端写请求的复制进度。
type proposal struct {
	cmd     Command
	index   uint64
	future  *WriteFuture
	timerID int64
	acks    map[string]bool // 已确认该 index 的副本（含领导者自身）
}

func proposalKey(cmd Command) string { return cmd.ClientID + "#" + u64(cmd.Seq) }

func u64(v uint64) string {
	if v == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for v > 0 {
		i--
		b[i] = byte('0' + v%10)
		v /= 10
	}
	return string(b[i:])
}

// readWait 跟踪一个线性一致读在途确认（ReadIndex 心跳多数派）。
type readWait struct {
	id        uint64
	key       string
	readIndex uint64 // 读发起时的服务下界：max(commitIndex, barrierIndex)
	minBeat   uint64 // 只统计该轮次（含）之后的心跳响应，杜绝旧轮次响应冒充确认
	acks      map[string]bool
	future    *ReadFuture
	timerID   int64
}

// deferredCall 是“必须在释放节点锁之后”执行的回调，用于避免
// 节点锁 -> future 回调 -> 节点 API 的自死锁。
type deferredCall struct{ fn func() }

// Node 是一个 Raft 风格的复制副本，所有驱动（时钟/消息）均由外部环境注入。
type Node struct {
	mu  sync.Mutex
	cfg Config

	role Role
	hard HardState

	// 易失
	leaderID      string
	votes         map[string]bool
	nextIndex     map[string]uint64
	matchIndex    map[string]uint64
	pending       map[uint64]*proposal // index -> 在途提案
	inflight      map[string]*proposal // (ClientID#Seq) -> 在途提案（重发去重）
	pendingReads  map[uint64]*readWait // readID -> 在途线性一致读（支持并发）
	readSeq       uint64               // 读请求单调编号
	barrierIndex  uint64               // 本任期上任时追加的 no-op 栅栏序号；0 表示非本任期领导
	beatSeq       uint64               // 心跳轮次：读注册时递增，响应按轮次归属到读
	electionTimer int64 // Scheduler 事件 ID，-1 表示无
	heartTimer    int64
	closed        bool
	applied       uint64
	deferred      []func() // 解锁后按序执行的用户回调

	rngState uint64
}

func NewNode(cfg Config) *Node {
	if cfg.ElectionMin == 0 {
		cfg.ElectionMin = 150
	}
	if cfg.ElectionMax == 0 {
		cfg.ElectionMax = 300
	}
	if cfg.Heartbeat == 0 {
		cfg.Heartbeat = 50
	}
	if cfg.ProposalDeadline == 0 {
		cfg.ProposalDeadline = 1000
	}
	if cfg.Log == nil {
		cfg.Log = discardLogger{}
	}
	seed := uint64(cfg.RandSeed)
	if seed == 0 {
		h := fnv.New64a()
		h.Write([]byte("election-seed:" + cfg.ID))
		seed = h.Sum64()
	}
	n := &Node{
		cfg:           cfg,
		role:          Follower,
		electionTimer: -1,
		heartTimer:    -1,
		votes:         map[string]bool{},
		nextIndex:     map[string]uint64{},
		matchIndex:    map[string]uint64{},
		pending:       map[uint64]*proposal{},
		inflight:      map[string]*proposal{},
		pendingReads:  map[uint64]*readWait{},
		rngState:      seed | 1,
	}
	return n
}

// Start 加载持久化状态、重放已提交日志并以 Follower 身份开始选举计时。
func (n *Node) Start() error {
	n.mu.Lock()

	n.hard = n.cfg.Storage.HardState()
	// 重放已提交历史（重启后状态机与会话去重缓存由此重建）。
	for idx := n.applied + 1; idx <= n.hard.CommitIndex; idx++ {
		e, err := n.cfg.Storage.Entry(idx)
		if err != nil {
			return err
		}
		n.cfg.SM.Apply(e)
		n.applied = idx
	}
	n.cfg.Transport.Register(n.cfg.ID, func(m Message) { n.HandleMessage(m) })
	n.resetElectionTimer()
	n.logState("start: loaded term=%d commit=%d applied=%d role=Follower")
	defers := n.deferred
	n.deferred = nil
	n.mu.Unlock()
	for _, d := range defers {
		d()
	}
	return nil
}

func (n *Node) Close() error {
	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return nil
	}
	n.closed = true
	n.cancelTimers()
	n.failAllPendingLocked(ReasonClosed, "node closed")
	n.failReadLocked(ReasonClosed, "node closed")
	// 注意：不关闭 n.cfg.Storage——其生命周期由装配方（SimEnv 或真实进程）管理，
	// 节点重启会复用同一持久存储。
	defers := n.deferred
	n.deferred = nil
	n.mu.Unlock()
	for _, d := range defers {
		d()
	}
	return nil
}

func (n *Node) ID() string          { return n.cfg.ID }
func (n *Node) Term() uint64        { n.mu.Lock(); defer n.mu.Unlock(); return n.hard.Term }
func (n *Node) CurrentRole() Role   { n.mu.Lock(); defer n.mu.Unlock(); return n.role }
func (n *Node) LeaderHint() string  { n.mu.Lock(); defer n.mu.Unlock(); return n.leaderID }
func (n *Node) CommitIndex() uint64 { n.mu.Lock(); defer n.mu.Unlock(); return n.hard.CommitIndex }

// Propose 提交一个写命令。非领导者立即返回携带 ReasonNotLeader 的失败，
// 领导者在多数确认前不会让结果对调用方可见。
func (n *Node) Propose(ctx context.Context, cmd Command) *WriteFuture {
	var fut *WriteFuture
	n.runLocked(func() {
		if n.closed {
			fut = readyFuture(ApplyResult{}, &WriteError{Reason: ReasonClosed})
			return
		}
		if n.role != Leader {
			fut = readyFuture(ApplyResult{}, n.notLeaderErr("propose"))
			return
		}
		// 同领导者在途去重：相同 (ClientID, Seq) 的超时重发直接复用原 future，
		// 绝不新建日志条目，避免未提交垃圾堆积与重复应用。
		if cmd.ClientID != "" {
			if p, ok := n.inflight[proposalKey(cmd)]; ok {
				n.logf("propose-dedup: term=%d index=%d client=%s seq=%d (in-flight retry reuses proposal)",
					n.hard.Term, p.index, cmd.ClientID, cmd.Seq)
				fut = p.future
				return
			}
		}
		idx := n.cfg.Storage.LastIndex() + 1
		e := Entry{Term: n.hard.Term, Index: idx, Cmd: cmd}
		if err := n.cfg.Storage.Append(idx-1, []Entry{e}); err != nil {
			fut = readyFuture(ApplyResult{}, &WriteError{Reason: ReasonNoQuorum, Detail: err.Error()})
			return
		}
		p := &proposal{
			cmd:     cmd,
			index:   idx,
			future:  newWriteFuture(nil),
			acks:    map[string]bool{n.cfg.ID: true},
			timerID: -1,
		}
		p.timerID = n.cfg.Sched.AfterInternal(n.cfg.ProposalDeadline, func() { n.onProposalTimeout(idx) })
		n.pending[idx] = p
		n.matchIndex[n.cfg.ID] = idx
		if p.cmd.ClientID != "" {
			n.inflight[proposalKey(p.cmd)] = p
		}
		n.logf("propose: term=%d index=%d client=%s seq=%d -> replicating (majority=%d)",
			n.hard.Term, idx, cmd.ClientID, cmd.Seq, n.quorum())
		for _, peer := range n.cfg.Peers {
			if peer != n.cfg.ID {
				n.broadcastAppendLocked(peer)
			}
		}
		fut = p.future
	})
	return fut
}

// Read 发起一次线性一致读（ReadIndex），仅领导者可服务。
// 支持并发：每个读独立登记、独立计时，互不覆盖；心跳响应按轮次归属到
// 所有在该轮次（含）之后注册的读。
func (n *Node) Read(ctx context.Context, key string) *ReadFuture {
	var rf *ReadFuture
	n.runLocked(func() {
		if n.closed {
			rf = readyRead("", false, &WriteError{Reason: ReasonClosed})
			return
		}
		if n.role != Leader {
			rf = readyRead("", false, n.notLeaderErr("read"))
			return
		}
		// 服务下界：必须等到本任期栅栏提交（commitIndex 覆盖前任已提交历史），
		// 且本地已应用到读发起时的提交点，才能从状态机取值。
		readIndex := n.hard.CommitIndex
		if n.barrierIndex > readIndex {
			readIndex = n.barrierIndex
		}
		n.readSeq++
		n.beatSeq++ // 新一轮心跳：只承认该轮次（含）之后的响应
		rw := &readWait{
			id:        n.readSeq,
			key:       key,
			readIndex: readIndex,
			minBeat:   n.beatSeq,
			acks:      map[string]bool{n.cfg.ID: true},
			future:    newReadFuture(),
		}
		id := rw.id
		rw.timerID = n.cfg.Sched.AfterInternal(n.cfg.ProposalDeadline, func() { n.onReadTimeout(id) })
		n.pendingReads[rw.id] = rw
		for _, peer := range n.cfg.Peers {
			if peer != n.cfg.ID {
				n.broadcastAppendLocked(peer)
			}
		}
		n.logf("read: term=%d key=%q read#=%d readIndex=%d beat=%d -> awaiting heartbeat quorum=%d",
			n.hard.Term, key, rw.id, readIndex, n.beatSeq, n.quorum())
		rf = rw.future
	})
	return rf
}

// HandleMessage 是 Transport 投递消息的入口。
func (n *Node) HandleMessage(m Message) {
	n.runLocked(func() {
		if n.closed {
			return
		}
		if m.Term > n.hard.Term {
			n.becomeFollowerLocked(m.Term, "")
		}
		n.stepLocked(m)
	})
}

// ---- 计时 ----

func (n *Node) resetElectionTimer() {
	if n.electionTimer >= 0 {
		n.cfg.Sched.Cancel(n.electionTimer)
	}
	d := n.randElectionTimeout()
	n.electionTimer = n.cfg.Sched.AfterInternal(d, func() { n.tickElection() })
}

func (n *Node) resetHeartbeatTimer() {
	if n.heartTimer >= 0 {
		n.cfg.Sched.Cancel(n.heartTimer)
	}
	n.heartTimer = n.cfg.Sched.AfterInternal(n.cfg.Heartbeat, func() { n.tickHeartbeat() })
}

func (n *Node) cancelTimers() {
	if n.electionTimer >= 0 {
		n.cfg.Sched.Cancel(n.electionTimer)
		n.electionTimer = -1
	}
	if n.heartTimer >= 0 {
		n.cfg.Sched.Cancel(n.heartTimer)
		n.heartTimer = -1
	}
}

func (n *Node) tickElection() {
	n.runLocked(func() {
		if n.closed || n.role == Leader {
			return
		}
		n.logf("election-timeout: term=%d commit=%d -> starting election (basis: no heartbeat within randomized timeout)",
			n.hard.Term, n.hard.CommitIndex)
		n.campaignLocked()
	})
}

func (n *Node) tickHeartbeat() {
	n.runLocked(func() {
		if n.closed || n.role != Leader {
			return
		}
		n.heartTimer = -1
		for _, peer := range n.cfg.Peers {
			if peer != n.cfg.ID {
				n.broadcastAppendLocked(peer)
			}
		}
		n.resetHeartbeatTimer()
	})
}

// ---- 角色转换 ----

func (n *Node) campaignLocked() {
	wasLeader := n.role == Leader
	n.role = Candidate
	n.hard.Term++
	n.leaderID = ""
	n.barrierIndex = 0
	n.votes = map[string]bool{n.cfg.ID: true}
	n.hard.VoteFor = n.cfg.ID
	if !wasLeader {
		// 选举意味着旧领导权威失效：未决写必须明确失败，
		// 绝不允许在领导更替后残留为已提交结果。
		n.failAllPendingLocked(ReasonNotLeader, "election started before commit")
		n.failReadLocked(ReasonNotLeader, "election started")
	}
	if err := n.cfg.Storage.SaveHardState(n.hard); err != nil {
		n.logf("campaign: persist vote failed: %v", err)
	}
	lastIdx := n.cfg.Storage.LastIndex()
	lastTerm, _ := n.cfg.Storage.TermAt(lastIdx)
	n.logf("become-candidate: term=%d lastLog=(%d,%d) votes=1/%d (basis: self-vote, election safety: one leader per term)",
		n.hard.Term, lastTerm, lastIdx, len(n.cfg.Peers))
	n.resetElectionTimer()
	for _, peer := range n.cfg.Peers {
		if peer == n.cfg.ID {
			continue
		}
		n.sendLocked(Message{
			Type:        MsgVote,
			From:        n.cfg.ID,
			To:          peer,
			Term:        n.hard.Term,
			LastLogTerm: lastTerm,
			LastLogIdx:  lastIdx,
		})
	}
	// 单节点集群立即胜选。
	if n.quorum() == 1 {
		n.becomeLeaderLocked()
	}
}

func (n *Node) becomeFollowerLocked(term uint64, leader string) {
	oldRole := n.role
	n.role = Follower
	n.hard.Term = term
	n.hard.VoteFor = ""
	n.barrierIndex = 0
	if leader != "" {
		n.leaderID = leader
	}
	if oldRole == Leader {
		n.logf("step-down: term=%d -> follower (basis: discovered higher term; rejecting all in-flight writes as not_leader)", term)
		n.failAllPendingLocked(ReasonNotLeader, "leadership lost to higher term")
		n.failReadLocked(ReasonNotLeader, "leadership lost")
	}
	if err := n.cfg.Storage.SaveHardState(n.hard); err != nil {
		n.logf("become-follower: persist failed: %v", err)
	}
	if n.heartTimer >= 0 {
		n.cfg.Sched.Cancel(n.heartTimer)
		n.heartTimer = -1
	}
	n.resetElectionTimer()
}

func (n *Node) becomeLeaderLocked() {
	n.role = Leader
	n.leaderID = n.cfg.ID
	last := n.cfg.Storage.LastIndex()
	for _, peer := range n.cfg.Peers {
		n.nextIndex[peer] = last + 1
		n.matchIndex[peer] = 0
	}
	n.matchIndex[n.cfg.ID] = last
	n.logf("become-leader: term=%d commit=%d votes=%d/%d (basis: majority votes granted)",
		n.hard.Term, n.hard.CommitIndex, len(n.votes), len(n.cfg.Peers))
	// 提交栅栏：追加一条本任期 no-op。Raft 提交规则只允许按副本计数提交
	// 本任期条目；栅栏提交后，commitIndex 才确定覆盖前任已提交的全部历史，
	// 此后的线性一致读才能满足“领导更替后仍读到最新已确认写入”。
	idx := last + 1
	e := Entry{Term: n.hard.Term, Index: idx, Cmd: Command{Kind: CmdNop}}
	if err := n.cfg.Storage.Append(idx-1, []Entry{e}); err != nil {
		n.logf("become-leader: append barrier failed: %v", err)
	} else {
		n.barrierIndex = idx
		n.matchIndex[n.cfg.ID] = idx
		n.pending[idx] = &proposal{cmd: e.Cmd, index: idx, acks: map[string]bool{n.cfg.ID: true}, timerID: -1}
		n.logf("barrier: term=%d index=%d (basis: leader must commit a current-term entry before serving reads)",
			n.hard.Term, idx)
	}
	// 胜选：取消选举定时器（其回调绝不能在领导任期内迟到触发）。
	if n.electionTimer >= 0 {
		n.cfg.Sched.Cancel(n.electionTimer)
	}
	n.electionTimer = -1
	for _, peer := range n.cfg.Peers {
		if peer != n.cfg.ID {
			n.broadcastAppendLocked(peer)
		}
	}
	n.resetHeartbeatTimer()
}

// ---- 消息分派 ----

func (n *Node) stepLocked(m Message) {
	switch m.Type {
	case MsgVote:
		n.handleVoteLocked(m)
	case MsgVoteResp:
		n.handleVoteRespLocked(m)
	case MsgAppend:
		n.handleAppendLocked(m)
	case MsgAppendResp:
		if n.role == Leader {
			n.handleAppendRespLocked(m)
		}
	}
}

func (n *Node) handleVoteLocked(m Message) {
	grant := false
	basis := ""
	switch {
	case m.Term < n.hard.Term:
		basis = "candidate term is older -> explicit reject (stale_term)"
	case n.hard.VoteFor != "" && n.hard.VoteFor != m.From:
		basis = "already voted for " + n.hard.VoteFor + " in term -> reject"
	default:
		myLast := n.cfg.Storage.LastIndex()
		myTerm, _ := n.cfg.Storage.TermAt(myLast)
		upToDate := m.LastLogTerm > myTerm ||
			(m.LastLogTerm == myTerm && m.LastLogIdx >= myLast)
		if upToDate {
			grant = true
			n.hard.VoteFor = m.From
			_ = n.cfg.Storage.SaveHardState(n.hard)
			basis = "candidate log at least as up-to-date -> grant"
		} else {
			basis = "candidate log behind mine -> reject"
		}
	}
	n.resetElectionTimer()
	n.logf("vote: term=%d from=%s granted=%v commit=%d (%s)", n.hard.Term, m.From, grant, n.hard.CommitIndex, basis)
	n.sendLocked(Message{
		Type:    MsgVoteResp,
		From:    n.cfg.ID,
		To:      m.From,
		Term:    n.hard.Term,
		Granted: grant,
	})
}

func (n *Node) handleVoteRespLocked(m Message) {
	if n.role != Candidate || m.Term != n.hard.Term {
		return
	}
	if m.Granted {
		n.votes[m.From] = true
	}
	n.logf("vote-resp: term=%d from=%s granted=%v votes=%d/%d commit=%d",
		m.Term, m.From, m.Granted, len(n.votes), len(n.cfg.Peers), n.hard.CommitIndex)
	if len(n.votes) >= n.quorum() {
		n.becomeLeaderLocked()
	}
}

func (n *Node) handleAppendLocked(m Message) {
	reply := Message{Type: MsgAppendResp, From: n.cfg.ID, To: m.From, Term: n.hard.Term, BeatSeq: m.BeatSeq}

	if m.Term < n.hard.Term {
		reply.Success = false
		reply.Match = n.hard.CommitIndex
		n.logf("append: reject from=%s term=%d < mine=%d commit=%d (basis: stale leader term)",
			m.From, m.Term, n.hard.Term, n.hard.CommitIndex)
		n.sendLocked(reply)
		return
	}

	n.leaderID = m.From
	if n.role != Follower {
		// 发现同任期合法领导者（选举冲突的另一方已胜选）：退回跟随者。
		n.role = Follower
		if n.heartTimer >= 0 {
			n.cfg.Sched.Cancel(n.heartTimer)
			n.heartTimer = -1
		}
		n.failAllPendingLocked(ReasonNotLeader, "same-term leader established")
		n.failReadLocked(ReasonNotLeader, "same-term leader established")
		n.logf("append: stepping down to follower, same-term leader=%s term=%d", m.From, n.hard.Term)
	}
	n.resetElectionTimer()

	last := n.cfg.Storage.LastIndex()
	if m.PrevLogIdx > last {
		reply.Success = false
		reply.RejectHintIdx = last
		n.logf("append: reject from=%s prevIdx=%d > last=%d commit=%d (basis: gap, hint backfill)",
			m.From, m.PrevLogIdx, last, n.hard.CommitIndex)
		n.sendLocked(reply)
		return
	}
	if m.PrevLogIdx > 0 {
		pt, err := n.cfg.Storage.TermAt(m.PrevLogIdx)
		if err != nil || pt != m.PrevLogTerm {
			reply.Success = false
			reply.RejectHintIdx = m.PrevLogIdx - 1
			n.logf("append: reject from=%s prevIdx=%d term mismatch want=%d got=%d commit=%d (basis: conflicting term, truncate)",
				m.From, m.PrevLogIdx, m.PrevLogTerm, pt, n.hard.CommitIndex)
			n.sendLocked(reply)
			return
		}
	}

	if len(m.Entries) > 0 {
		if err := n.cfg.Storage.Append(m.PrevLogIdx, m.Entries); err != nil {
			n.logf("append: persist error: %v", err)
			reply.Success = false
			n.sendLocked(reply)
			return
		}
		n.logf("append: accepted from=%s entries=%d lastNow=%d term=%d (basis: prevLog matched, idempotent append)",
			m.From, len(m.Entries), n.cfg.Storage.LastIndex(), n.hard.Term)
	}
	match := n.cfg.Storage.LastIndex()
	if len(m.Entries) > 0 {
		match = m.Entries[len(m.Entries)-1].Index
	} else if m.PrevLogIdx < match {
		match = m.PrevLogIdx
	}

	if m.LeaderCommit > n.hard.CommitIndex {
		newCommit := m.LeaderCommit
		if match < newCommit {
			newCommit = match
		}
		n.hard.CommitIndex = newCommit
		_ = n.cfg.Storage.SaveHardState(n.hard)
		n.logf("append: commit advanced to=%d term=%d (basis: leaderCommit=%d clamped to match=%d)",
			newCommit, n.hard.Term, m.LeaderCommit, match)
	}
	n.applyCommittedLocked()

	reply.Success = true
	reply.Match = match
	n.sendLocked(reply)
}

func (n *Node) handleAppendRespLocked(m Message) {
	if m.Term < n.hard.Term {
		return
	}
	if m.Success {
		prevMatch := n.matchIndex[m.From]
		n.matchIndex[m.From] = max(prevMatch, m.Match)
		if m.Match+1 > n.nextIndex[m.From] {
			n.nextIndex[m.From] = m.Match + 1
		}
		// 记账在途提案的确认。
		for idx, p := range n.pending {
			if m.Match >= idx {
				p.acks[m.From] = true
			}
		}
		// 心跳响应按轮次归属：只计入在该读注册轮次（含）之后发出的心跳。
		for _, rw := range n.pendingReads {
			if m.BeatSeq >= rw.minBeat {
				rw.acks[m.From] = true
			}
		}
		n.tryCompleteReadsLocked()
		n.advanceCommitLocked()
		// 可能还有更多日志可发（成功响应后追赶窗口）。
		if n.cfg.Storage.LastIndex() >= n.nextIndex[m.From] {
			n.broadcastAppendLocked(m.From)
		}
		return
	}

	// 拒绝：领导者或任期落后都会走到这里；按提示退避后重发，保持追赶幂等。
	hint := m.RejectHintIdx
	cur := n.nextIndex[m.From]
	if hint > 0 && hint < cur {
		n.nextIndex[m.From] = hint + 1
	} else if cur > 1 {
		n.nextIndex[m.From] = cur - 1
	}
	n.logf("append-resp: rejected from=%s term=%d nextIdx->%d commit=%d (basis: backoff & retry, idempotent catchup)",
		m.From, m.Term, n.nextIndex[m.From], n.hard.CommitIndex)
	n.broadcastAppendLocked(m.From)
}

// ---- 复制与提交 ----

func (n *Node) broadcastAppendLocked(peer string) {
	next := n.nextIndex[peer]
	last := n.cfg.Storage.LastIndex()
	var entries []Entry
	if next <= last {
		es, err := n.cfg.Storage.Entries(next, last+1)
		if err != nil {
			n.logf("broadcast: read entries for %s: %v", peer, err)
			return
		}
		entries = es
	}
	prevIdx := next - 1
	prevTerm, _ := n.cfg.Storage.TermAt(prevIdx)
	n.sendLocked(Message{
		Type:         MsgAppend,
		From:         n.cfg.ID,
		To:           peer,
		Term:         n.hard.Term,
		PrevLogIdx:   prevIdx,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: n.hard.CommitIndex,
		LeaderHint:   n.cfg.ID,
		BeatSeq:      n.beatSeq,
	})
}

// advanceCommitLocked 实现 Raft 提交规则：仅提交“本任期”且被多数 match 的序号，
// 从高到低找到即可提交（日志无空洞）。
func (n *Node) advanceCommitLocked() {
	last := n.cfg.Storage.LastIndex()
	for idx := last; idx > n.hard.CommitIndex; idx-- {
		t, err := n.cfg.Storage.TermAt(idx)
		if err != nil || t != n.hard.Term {
			continue // 旧任期条目不能仅凭副本计数直接提交
		}
		count := 0
		for _, peer := range n.cfg.Peers {
			if n.matchIndex[peer] >= idx {
				count++
			}
		}
		if count >= n.quorum() {
			n.hard.CommitIndex = idx
			_ = n.cfg.Storage.SaveHardState(n.hard)
			n.logf("commit: term=%d index=%d acks=%d/%d (basis: majority match on current-term entry)",
				n.hard.Term, idx, count, len(n.cfg.Peers))
			// 立即把新的 commitIndex 推给所有跟随者，无需等下一次心跳；
			// 产生的外部消息也让确定性仿真自然收敛。
			for _, peer := range n.cfg.Peers {
				if peer != n.cfg.ID {
					n.broadcastAppendLocked(peer)
				}
			}
			break
		}
	}
	n.applyCommittedLocked()
}

// applyCommittedLocked 把已提交日志按序应用到状态机，并兑现对应客户端结论。
func (n *Node) applyCommittedLocked() {
	for n.applied < n.hard.CommitIndex {
		idx := n.applied + 1
		e, err := n.cfg.Storage.Entry(idx)
		if err != nil {
			return
		}
		res := n.cfg.SM.Apply(e)
		n.applied = idx
		if p, ok := n.pending[idx]; ok {
			if p.future != nil { // no-op 栅栏无客户端 future
				if res.Err != nil {
					n.enqueueWrite(p.future, res, &WriteError{
						Reason:     ReasonStaleRequest,
						LeaderHint: n.cfg.ID,
						Term:       n.hard.Term,
						Detail:     "applied command rejected as stale/duplicate-conflict",
					})
				} else {
					n.enqueueWrite(p.future, res, nil)
				}
			}
			n.cancelTimer(p.timerID)
			delete(n.pending, idx)
			if p.cmd.ClientID != "" {
				delete(n.inflight, proposalKey(p.cmd))
			}
		}
	}
	n.tryCompleteReadsLocked()
}

func (n *Node) onProposalTimeout(idx uint64) {
	n.runLocked(func() {
		p, ok := n.pending[idx]
		if !ok {
			return
		}
		reason := ReasonNoQuorum
		detail := "no follower acknowledgements within deadline"
		if len(p.acks) > 1 {
			reason = ReasonTimeout
			detail = "partial acks only, majority not reached"
		}
		if n.role != Leader {
			reason = ReasonNotLeader
			detail = "leadership changed while awaiting quorum"
		}
		n.logf("propose-timeout: term=%d index=%d acks=%d/%d -> %s",
			n.hard.Term, idx, len(p.acks), len(n.cfg.Peers), reason)
		n.enqueueWrite(p.future, ApplyResult{}, &WriteError{
			Reason:     reason,
			LeaderHint: n.leaderID,
			Term:       n.hard.Term,
			Detail:     detail,
		})
		delete(n.pending, idx)
		if p.cmd.ClientID != "" {
			delete(n.inflight, proposalKey(p.cmd))
		}
	})
}

// tryCompleteReadsLocked 检查全部在途读：心跳多数确认（轮次有效）且本地已应用
// 到该读的服务下界（含本任期栅栏）后，从状态机取值兑现；条件不满足的读继续等待。
func (n *Node) tryCompleteReadsLocked() {
	for id, rw := range n.pendingReads {
		if len(rw.acks) < n.quorum() {
			continue
		}
		if n.applied < rw.readIndex {
			continue // 栅栏/提交点尚未应用到本地，绝不返回陈旧值
		}
		v, found := n.cfg.SM.Read(rw.key)
		n.logf("read-ok: term=%d key=%q read#=%d readIndex=%d commit=%d (basis: heartbeat quorum=%d beat>=%d then local apply)",
			n.hard.Term, rw.key, rw.id, rw.readIndex, n.hard.CommitIndex, n.quorum(), rw.minBeat)
		n.enqueueRead(rw.future, v, found, nil)
		n.cancelTimer(rw.timerID)
		delete(n.pendingReads, id)
	}
}

func (n *Node) onReadTimeout(id uint64) {
	n.runLocked(func() {
		rw, ok := n.pendingReads[id]
		if !ok {
			return
		}
		reason := ReasonNoQuorum
		detail := "heartbeat quorum not confirmed within deadline"
		switch {
		case n.role != Leader:
			reason = ReasonNotLeader
			detail = "leadership changed while awaiting read quorum"
		case n.applied < rw.readIndex:
			detail = "commit barrier not applied within deadline (no quorum to commit current-term entry)"
		}
		n.logf("read-timeout: term=%d read#=%d acks=%d/%d applied=%d readIndex=%d -> %s",
			n.hard.Term, id, len(rw.acks), len(n.cfg.Peers), n.applied, rw.readIndex, reason)
		n.enqueueRead(rw.future, "", false, &WriteError{
			Reason:     reason,
			LeaderHint: n.leaderID,
			Term:       n.hard.Term,
			Detail:     detail,
		})
		delete(n.pendingReads, id)
	})
}

func (n *Node) failReadLocked(reason FailReason, detail string) {
	for id, rw := range n.pendingReads {
		n.cancelTimer(rw.timerID)
		n.enqueueRead(rw.future, "", false, &WriteError{Reason: reason, Term: n.hard.Term, Detail: detail})
		delete(n.pendingReads, id)
	}
}

func (n *Node) failAllPendingLocked(reason FailReason, detail string) {
	for idx, p := range n.pending {
		n.cancelTimer(p.timerID)
		if p.future != nil { // no-op 栅栏无客户端 future，静默丢弃即可
			n.enqueueWrite(p.future, ApplyResult{}, &WriteError{
				Reason:     reason,
				LeaderHint: n.leaderID,
				Term:       n.hard.Term,
				Detail:     detail,
			})
		}
		delete(n.pending, idx)
		if p.cmd.ClientID != "" {
			delete(n.inflight, proposalKey(p.cmd))
		}
	}
}

// cancelTimer 统一处理“无定时器”（timerID<0，如 no-op 栅栏）的情形。
func (n *Node) cancelTimer(id int64) {
	if id >= 0 {
		n.cfg.Sched.Cancel(id)
	}
}

// ---- 小工具 ----

func (n *Node) sendLocked(m Message) {
	if m.From == "" {
		m.From = n.cfg.ID
	}
	n.cfg.Transport.Send(m)
}

func (n *Node) notLeaderErr(op string) *WriteError {
	return &WriteError{
		Reason:     ReasonNotLeader,
		LeaderHint: n.leaderID,
		Term:       n.hard.Term,
		Detail:     op + " rejected: node is " + n.role.String(),
	}
}

func (n *Node) randElectionTimeout() int64 {
	// xorshift64*，确定性：同种子序列可复现。
	x := n.rngState
	x ^= x >> 12
	x ^= x << 25
	x ^= x >> 27
	n.rngState = x
	r := int64((x * 0x2545F4914F6CDD1D) >> 33)
	span := n.cfg.ElectionMax - n.cfg.ElectionMin + 1
	if span <= 0 {
		return n.cfg.ElectionMin
	}
	if r < 0 {
		r = -r
	}
	return n.cfg.ElectionMin + r%span
}

func (n *Node) quorum() int { return len(n.cfg.Peers)/2 + 1 }

func (n *Node) logf(format string, args ...any) {
	n.cfg.Log.Printf("[%s] "+format, append([]any{n.cfg.ID}, args...)...)
}

func (n *Node) logState(format string) {
	n.logf(format, n.hard.Term, n.hard.CommitIndex, n.applied)
}

func readyFuture(r ApplyResult, err error) *WriteFuture {
	f := newWriteFuture(nil)
	f.complete(r, err)
	return f
}

func readyRead(v string, found bool, err error) *ReadFuture {
	f := newReadFuture()
	f.complete(v, found, err)
	return f
}

// enqueue* 在持锁时只登记回调，统一由 runLocked 的调用方在解锁后派发，
// 从而杜绝“持节点锁执行用户回调、回调又进入节点 API”的重入死锁。
func (n *Node) enqueueWrite(f *WriteFuture, r ApplyResult, err error) {
	future := f
	result := r
	e := err
	n.deferred = append(n.deferred, func() { future.complete(result, e) })
}

func (n *Node) enqueueRead(f *ReadFuture, v string, found bool, err error) {
	future := f
	e := err
	n.deferred = append(n.deferred, func() { future.complete(v, found, e) })
}

func (n *Node) runLocked(fn func()) {
	n.mu.Lock()
	fn()
	defers := n.deferred
	n.deferred = nil
	n.mu.Unlock()
	for _, d := range defers {
		d()
	}
}
