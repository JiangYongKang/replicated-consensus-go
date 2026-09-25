package raft

import "testing"

// TestStaleTermAndLaggingLogRejected 直接向副本投递构造消息，验证：
//   - 更旧任期的 AppendEntries 被拒绝（stale leader term）；
//   - 日志落后的候选人拉票被拒绝（不会让日志不齐者成为领导者）。
func TestStaleTermAndLaggingLogRejected(t *testing.T) {
	env := NewSimEnv(3).WithLogger(testLogger{t})
	defer env.Close()
	leader := env.Leader(2000)
	if leader == "" {
		t.Fatal("no leader")
	}
	env.Settle()
	curTerm := env.Nodes[leader].Term()

	// 先写一笔，让多数派日志前进到 index=1。
	cl := NewClient("t", env.Nodes, env.Sched)
	wf := cl.Set(nil, "k", "v")
	waitDone(t, env, wf, 1000, "write")

	followerID := env.IDs[0]
	if followerID == leader {
		followerID = env.IDs[1]
	}
	f := env.Nodes[followerID]

	// 1) 旧任期 AppendEntries：必须拒绝。
	staleMsg := Message{
		Type:        MsgAppend,
		From:        "ghost-old-leader",
		To:          followerID,
		Term:        curTerm - 1,
		PrevLogIdx:  0,
		PrevLogTerm: 0,
		Entries:     []Entry{{Term: curTerm - 1, Index: 1, Cmd: Command{Kind: CmdSet, Key: "x", Val: "y"}}},
	}
	f.HandleMessage(staleMsg)
	env.Settle()
	if f.Term() != curTerm {
		t.Fatalf("follower term=%d must remain %d after stale append", f.Term(), curTerm)
	}
	if v, ok := f.cfg.SM.Read("x"); ok {
		t.Fatalf("stale-term entry must never apply, got x=%q", v)
	}
	t.Logf("stale-term append rejected: node=%s stays term=%d commit=%d",
		followerID, f.Term(), f.CommitIndex())

	// 2) 日志落后候选人（LastLogIdx=0, term=0）在更高任期拉票：
	//    任期更高会让节点进入该任期，但其日志落后，投票必须被拒绝。
	laggard := Message{
		Type:        MsgVote,
		From:        "laggard",
		To:          followerID,
		Term:        curTerm + 1,
		LastLogTerm: 0,
		LastLogIdx:  0,
	}
	before := f.CommitIndex()
	f.HandleMessage(laggard)
	env.Settle()
	// 该落后候选人拿不到多数票，集群不应出现 term=curTerm+1 的领导者。
	for id, nd := range env.Nodes {
		if nd.CurrentRole() == Leader && nd.Term() == curTerm+1 {
			t.Fatalf("lagging candidate %s became leader at higher term!", id)
		}
	}
	t.Logf("lagging-log vote denied; followers retain commit=%d, no stale takeover", before)
}
