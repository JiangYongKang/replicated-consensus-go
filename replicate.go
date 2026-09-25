package raft

// 本文件中的方法均在持有 n.mu 的前提下调用。

// ---- 日志辅助 ----

func (n *Node) entryAt(index uint64) LogEntry { return n.log[index] }
func (n *Node) lastIndex() uint64             { return n.log[len(n.log)-1].Index }
func (n *Node) lastTerm() uint64              { return n.log[len(n.log)-1].Term }

func (n *Node) persistLocked() {
	if err := n.ps.Save(&PersistentData{
		Version:     PersistentDataVersion,
		CurrentTerm: n.term,
		VotedFor:    n.votedFor,
		Log:         n.log,
	}); err != nil {
		// 模拟环境下 MemoryPersister 不会失败；真实环境中持久化失败
		// 必须阻止继续推进（此处直接 panic，由进程级监督处理）。
		panic(err)
	}
}

// ---- 角色转换 ----

func (n *Node) becomeFollowerLocked(term uint64, leaderHint int) {
	n.state = Follower
	n.term = term
	n.votedFor = -1
	n.reads = make(map[uint64]*readState)
	if leaderHint >= 0 {
		n.leaderHint = leaderHint
	}
	n.resetDeadlineLocked()
	n.persistLocked()
}

func (n *Node) becomeLeaderLocked() {
	n.state = Leader
	n.leaderHint = n.cfg.ID
	n.reads = make(map[uint64]*readState)
	li := n.lastIndex() + 1
	for _, p := range n.cfg.Peers {
		n.nextIndex[p] = li
		n.matchIndex[p] = 0
	}
	n.lastHeartbeat = n.ticks
	// 立即提交一条本任期 no-op：这样上一任期已复制到多数派的日志，
	// 能在 no-op 提交时被一并提交（Raft 只直接提交当前任期条目）。
	n.log = append(n.log, LogEntry{Index: li, Term: n.term, Cmd: Command{}})
	n.matchIndex[n.cfg.ID] = li
	n.persistLocked()
	n.broadcastHeartbeatLocked()
}

// startElectionLocked：任期 +1，转候选人，给自己投票并广播 RequestVote。
func (n *Node) startElectionLocked() {
	n.term++
	n.state = Candidate
	n.votedFor = n.cfg.ID
	n.votesGranted = map[int]bool{n.cfg.ID: true}
	n.reads = make(map[uint64]*readState)
	n.resetDeadlineLocked()
	n.persistLocked()

	for _, p := range n.cfg.Peers {
		n.net.send(Message{
			Type:         MsgVote,
			From:         n.cfg.ID,
			To:           p,
			Term:         n.term,
			PrevLogIndex: n.lastIndex(),
			PrevLogTerm:  n.lastTerm(),
		})
	}
	// 单节点集群：自己一票即成主。
	if len(n.votesGranted) >= n.quorum() {
		n.becomeLeaderLocked()
	}
}

// ---- RequestVote ----

func (n *Node) handleVoteReqLocked(m Message) {
	reply := Message{Type: MsgVoteResp, From: n.cfg.ID, To: m.From, Term: n.term}

	if m.Term < n.term {
		// 任期落后的候选人：明确拒绝，不静默跟随。
		reply.VoteGranted = false
		n.net.send(reply)
		return
	}

	upToDate := m.PrevLogTerm > n.lastTerm() ||
		(m.PrevLogTerm == n.lastTerm() && m.PrevLogIndex >= n.lastIndex())
	canVote := n.votedFor == -1 || n.votedFor == m.From

	if !(stateCanGrant(n.state) && canVote && upToDate) {
		reply.VoteGranted = false
		n.net.send(reply)
		return
	}

	n.votedFor = m.From
	n.resetDeadlineLocked()
	n.persistLocked()
	reply.VoteGranted = true
	n.net.send(reply)
}

// 领导人不应再给别人投票；候选人只能投给自己（在 startElection 中已投）。
func stateCanGrant(s State) bool { return s == Follower || s == Candidate }

func (n *Node) handleVoteRespLocked(m Message) {
	if n.state != Candidate || m.Term != n.term || !m.VoteGranted {
		return
	}
	n.votesGranted[m.From] = true
	if len(n.votesGranted) >= n.quorum() {
		n.becomeLeaderLocked()
	}
}

// ---- AppendEntries ----

func (n *Node) handleAppendReqLocked(m Message) {
	reply := Message{Type: MsgAppendResp, From: n.cfg.ID, To: m.From, Term: n.term, ReadID: m.ReadID}

	if m.Term < n.term {
		// 旧任期/旧日志的“领导者”：明确拒绝。
		reply.Success = false
		n.net.send(reply)
		return
	}

	// 同任期的合法领导者：候选人必须退回追随者。
	if n.state == Candidate {
		n.state = Follower
		n.votedFor = -1
		n.persistLocked()
	}
	n.resetDeadlineLocked()
	n.leaderHint = m.From

	if m.PrevLogIndex > n.lastIndex() {
		reply.Success = false // 本地日志太短，等领导者回退 nextIndex 后追赶
		n.net.send(reply)
		return
	}
	if n.log[m.PrevLogIndex].Term != m.PrevLogTerm {
		reply.Success = false // 同一位置任期不一致
		n.net.send(reply)
		return
	}

	// 追加新条目；重复投递时按 (index, term) 幂等跳过。
	changed := false
	idx := m.PrevLogIndex
	for _, e := range m.Entries {
		idx++
		if uint64(len(n.log)) > idx {
			if n.log[idx].Term == e.Term {
				continue // 同 index 同 term：内容必然相同，跳过（幂等）
			}
			n.log = append([]LogEntry(nil), n.log[:idx]...) // 冲突：截断
		}
		e.Index = idx
		n.log = append(n.log, e)
		changed = true
	}
	if changed {
		n.persistLocked()
	}

	if m.LeaderCommit > n.commitIndex {
		if m.LeaderCommit < n.lastIndex() {
			n.commitIndex = m.LeaderCommit
		} else {
			n.commitIndex = n.lastIndex()
		}
		n.applyCommittedLocked()
	}

	reply.Success = true
	reply.MatchIndex = n.lastIndex()
	n.net.send(reply)
}

func (n *Node) handleAppendRespLocked(m Message) {
	if n.state != Leader || m.Term != n.term {
		return
	}
	peer := m.From

	// ReadIndex 确认（成功与否的 AE 响应都表示该对等体在本任期存活，
	// 但标准做法只接受成功响应；这里同样要求 Success）。
	if m.ReadID != 0 && m.Success {
		if rs := n.reads[m.ReadID]; rs != nil && rs.term == n.term {
			rs.acks[peer] = true
			if !rs.quorum && len(rs.acks) >= n.quorum() {
				rs.quorum = true
				rs.index = n.commitIndex
			}
		}
	}

	if m.Success {
		if m.MatchIndex > n.matchIndex[peer] {
			n.matchIndex[peer] = m.MatchIndex
			n.nextIndex[peer] = m.MatchIndex + 1
		}
		n.advanceCommitLocked()
		return
	}

	// 追赶失败：把 nextIndex 回退一步并重试，直到匹配为止。
	// 每次回退立即重发，落后副本可以在一个 tick 内连续补齐（幂等）。
	if n.nextIndex[peer] > 1 {
		n.nextIndex[peer]--
	}
	n.sendAppendLocked(peer, 0)
}

func (n *Node) sendAppendLocked(peer int, readID uint64) {
	ni := n.nextIndex[peer]
	entries := make([]LogEntry, len(n.log)-int(ni))
	copy(entries, n.log[ni:]) // 复制切片，避免后续日志追加产生别名
	n.net.send(Message{
		Type:         MsgAppend,
		From:         n.cfg.ID,
		To:           peer,
		Term:         n.term,
		PrevLogIndex: ni - 1,
		PrevLogTerm:  n.log[ni-1].Term,
		Entries:      entries,
		LeaderCommit: n.commitIndex,
		ReadID:       readID,
	})
}

// advanceCommitLocked：仅直接提交当前任期条目 N；
// N 一旦提交，之前所有任期的条目随之提交（Raft 图 8 规则）。
func (n *Node) advanceCommitLocked() {
	for N := n.lastIndex(); N > n.commitIndex; N-- {
		if n.log[N].Term != n.term {
			return
		}
		count := 1
		for _, p := range n.cfg.Peers {
			if n.matchIndex[p] >= N {
				count++
			}
		}
		if count >= n.quorum() {
			n.commitIndex = N
			n.applyCommittedLocked()
			return
		}
	}
}

// applyCommittedLocked：把已提交日志顺序应用到本地状态机。
// 未提交条目永远不会被应用；领导者更替后残留的未提交旧条目
// 可能被新领导者的条目截断覆盖，因此不会“残留为已提交结果”。
func (n *Node) applyCommittedLocked() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		cmd := n.log[n.lastApplied].Cmd
		if cmd.Op == 0 {
			continue // 领导者就职 no-op
		}
		n.machine.Apply(cmd) // 内部按 (ClientID, Seq) 幂等
	}
}

func (n *Node) broadcastHeartbeatLocked() {
	for _, p := range n.cfg.Peers {
		n.sendAppendLocked(p, 0)
	}
}

func (n *Node) broadcastReadLocked(readID uint64) {
	for _, p := range n.cfg.Peers {
		n.sendAppendLocked(p, readID)
	}
}
