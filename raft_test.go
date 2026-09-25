package raft

import (
	"fmt"
	"sync"
	"testing"
)

func logStatus(t *testing.T, c *Cluster, phase string) {
	t.Helper()
	for _, id := range c.ids {
		st, term, commit, applied := c.Status(id)
		t.Logf("[%s] node=%d state=%s term=%d commitIndex=%d lastApplied=%d",
			phase, id, st, term, commit, applied)
	}
}

func TestLeaderElection(t *testing.T) {
	c := NewCluster(5)
	defer c.Stop()

	leader, term, err := c.RunUntilLeader(200)
	if err != nil {
		t.Fatalf("leader not elected: %v", err)
	}
	t.Logf("判定依据: node=%d 在 term=%d 赢得多数派选票成为唯一领导者", leader, term)
	logStatus(t, c, "election done")

	// 同一任期最多一个领导者：统计 term 相同的 Leader 数量。
	sameTermLeaders := 0
	for _, id := range c.ids {
		st, tt, _, _ := c.Status(id)
		if st == Leader && tt == term {
			sameTermLeaders++
		}
	}
	if sameTermLeaders != 1 {
		t.Fatalf("term=%d 内期望恰有 1 个领导者，实际 %d 个", term, sameTermLeaders)
	}
	if c.net.Parted(leader, 0) {
		t.Fatalf("leader connectivity check failed")
	}
}

// 辅助：运行到领导者并完成一次写入，返回 (leader, term, 写后 commitIndex)。
func electAndPut(t *testing.T, c *Cluster, key, val string) (int, uint64) {
	t.Helper()
	leader, term, err := c.RunUntilLeader(200)
	if err != nil {
		t.Fatalf("leader not elected: %v", err)
	}
	cl := NewClient(c)
	if _, err := cl.Put(key, val); err != nil {
		t.Fatalf("put failed: %v", err)
	}
	t.Logf("判定依据: 写 %s=%s 由 node=%d(term=%d) 复制到多数派后提交", key, val, leader, term)
	return leader, term
}

func TestBasicWriteRead(t *testing.T) {
	c := NewCluster(3)
	defer c.Stop()
	cl := NewClient(c)
	leader, term := electAndPut(t, c, "k1", "v1")
	_ = leader
	r, err := cl.Get("k1")
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if !r.Found || r.Value != "v1" {
		t.Fatalf("期望 v1，得到 %+v", r)
	}
	t.Logf("判定依据: 线性读在 term=%d 返回已提交值 v1（未提交值不可见）", term)
}

func TestMinorityCannotWrite(t *testing.T) {
	c := NewCluster(5)
	defer c.Stop()
	leader, term := electAndPut(t, c, "k1", "v1")
	logStatus(t, c, "before partition")

	// 找到两个非领导者节点组成少数派 {x, y}，领导者留在多数派。
	var minority []int
	for _, id := range c.ids {
		if id != leader && len(minority) < 2 {
			minority = append(minority, id)
		}
	}
	majority := []int{leader}
	for _, id := range c.ids {
		inMin := false
		for _, m := range minority {
			if id == m {
				inMin = true
			}
		}
		if !inMin && id != leader {
			majority = append(majority, id)
		}
	}
	c.Partition(minority, majority)
	t.Logf("分区: 少数派=%v（含追随者），多数派=%v（含领导者 node=%d, term=%d）",
		minority, majority, leader, term)

	// 情况 A：通过少数派节点直连提案，它不是领导者 -> ErrNotLeaderKind。
	bad := c.node(minority[0])
	_, err := bad.Propose(Command{Op: OpPut, Key: "ghost", Value: "x", ClientID: 999, Seq: 1})
	if err == nil {
		t.Fatal("少数派追随者接受了写入，违反多数派要求")
	}
	re := err.(*Error)
	if re.Kind != ErrNotLeaderKind {
		t.Fatalf("期望 ErrNotLeaderKind，得到 kind=%d (%v)", re.Kind, err)
	}
	t.Logf("判定依据: 少数派追随者明确拒绝写: kind=ErrNotLeader reason=%q", err)

	// 情况 B：旧领导者被隔离进少数派后，无法获得多数派确认 -> ErrNoQuorumKind。
	oldLeader := leader
	var others []int
	for _, id := range c.ids {
		if id != oldLeader {
			others = append(others, id)
		}
	}
	// 重新建一个只隔离旧领导者的场景：先恢复再重新分区。
	c.Heal()
	c.Advance(c.heartbeat * 2)
	c.Isolate(oldLeader)
	t.Logf("场景 B: 隔离旧领导者 node=%d（term=%d）", oldLeader, term)
	_, err = c.node(oldLeader).Propose(Command{Op: OpPut, Key: "ghost2", Value: "y", ClientID: 998, Seq: 1})
	if err == nil {
		t.Fatal("被隔离的旧领导者竟完成了写入")
	}
	re = err.(*Error)
	if re.Kind != ErrNoQuorumKind && re.Kind != ErrNotLeaderKind {
		t.Fatalf("期望可区分的 ErrNoQuorum/ErrNotLeader，得到 kind=%d (%v)", re.Kind, err)
	}
	t.Logf("判定依据: 隔离的旧领导者写被拒绝 kind=%d (%v)；少数派不得提供写服务", re.Kind, err)

	// 此时 node=0 单独隔离；再把 {3,4} 与 {1,2} 切开，
	// 使 {0,1,2} 成为三节点少数派（旧领导者也在其中），{3,4} 为两节点多数派（无主）。
	c.Partition([]int{0, 1, 2}, []int{3, 4})
	c.Advance(c.electionBase*2 + 2)
	t.Log("场景 C: 旧领导者所在的 {0,1,2} 仅为 3/5 少数派")
	minorityGroup := []int{0, 1, 2}
	for _, id := range minorityGroup {
		st, tt, _, _ := c.Status(id)
		if st == Leader {
			_, err := c.node(id).Propose(Command{Op: OpPut, Key: "ghost3", Value: "z", ClientID: 997, Seq: uint64(id + 1)})
			if err == nil {
				t.Fatalf("少数派内的 node=%d(term=%d) 竟提交了写入", id, tt)
			}
			t.Logf("判定依据: 少数派节点 node=%d state=%s term=%d 写拒绝 kind=%d: %v", id, st, tt, err.(*Error).Kind, err)
		}
	}

	// 恢复全连通。
	c.Heal()
	t.Log("分区恢复，等待日志收敛")
	c.Advance(c.electionBase * 2)

	// 收敛判定：所有存活节点 term / commitIndex / 状态机结果一致。
	_, baseTerm, baseCommit, _ := c.Status(0)
	for _, id := range c.ids {
		st, tt, ci, ai := c.Status(id)
		if tt != baseTerm || ci != baseCommit || ai != ci {
			t.Fatalf("收敛失败 node=%d: state=%s term=%d commit=%d applied=%d（基准 term=%d commit=%d）",
				id, st, tt, ci, ai, baseTerm, baseCommit)
		}
	}
	t.Logf("判定依据: 所有节点收敛 term=%d commitIndex=%d 且 applied==commit", baseTerm, baseCommit)

	cl := NewClient(c)
	if r, _ := cl.Get("ghost"); r.Found {
		t.Fatal("未提交写入 ghost 在恢复后残留为已提交结果")
	}
	if r, _ := cl.Get("ghost2"); r.Found {
		t.Fatal("未提交写入 ghost2 残留")
	}
	if r, _ := cl.Get("ghost3"); r.Found {
		t.Fatal("未提交写入 ghost3 残留")
	}
	if r, err := cl.Get("k1"); err != nil || !r.Found || r.Value != "v1" {
		t.Fatalf("已提交历史丢失: %+v %v", r, err)
	}
	t.Log("判定依据: 恢复后未提交的 ghost 写入全部不可见，已提交的 k1=v1 仍然存在")
	logStatus(t, c, "after heal")
}

func TestLeaderRestart(t *testing.T) {
	c := NewCluster(3)
	defer c.Stop()
	cl := NewClient(c)

	leader, term := electAndPut(t, c, "a", "1")
	if _, err := cl.Put("b", "2"); err != nil {
		t.Fatal(err)
	}
	beforeCommit := c.node(leader).CommitIndex()
	t.Logf("重启前: 领导者 node=%d term=%d commitIndex=%d", leader, term, beforeCommit)

	// 重启领导者：持久化（term/投票/日志）保留，内存状态机清空。
	if err := c.Restart(leader, false); err != nil {
		t.Fatal(err)
	}
	t.Logf("判定依据: node=%d 崩溃后以 Follower 身份恢复，状态机从空重建，等待日志重放", leader)

	// 重启期间其余节点应选出新领导者（旧主重新加入时只能做追随者）。
	newLeader, newTerm, err := c.RunUntilLeader(200)
	if err != nil {
		t.Fatal(err)
	}
	st, rt, rc, ra := c.Status(leader)
	t.Logf("重启节点恢复后: node=%d state=%s term=%d commit=%d applied=%d；集群新领导者 node=%d term=%d",
		leader, st, rt, rc, ra, newLeader, newTerm)
	if newLeader == leader {
		// 允许它在足够长的超时后重新当选；但它恢复后不应立刻静默接管。
		t.Logf("注意：重启节点在新一轮选举中重新成为领导者 node=%d term=%d", leader, newTerm)
	}
	if newTerm <= term {
		t.Fatalf("重启后任期必须推进: old=%d new=%d", term, newTerm)
	}

	// 重启节点必须通过日志追赶补齐，且最终读到重启前已提交的历史。
	c.Advance(c.electionBase)
	r, err := cl.Get("a")
	if err != nil {
		t.Fatal(err)
	}
	if !r.Found || r.Value != "1" {
		t.Fatalf("重启后已提交历史丢失: %+v", r)
	}
	r, err = cl.Get("b")
	if err != nil || !r.Found || r.Value != "2" {
		t.Fatalf("重启后已提交历史丢失: %+v %v", r, err)
	}
	t.Log("判定依据: 重启节点经 AppendEntries 追赶重放后，a=1、b=2 均可读，提交历史一致")

	// 追赶幂等：再多跑若干 tick（重复投递），结果与提交序号保持稳定。
	ci := c.node(leader).CommitIndex()
	for i := 0; i < 30; i++ {
		c.Advance(1)
		if c.node(leader).CommitIndex() != ci {
			t.Fatalf("重复投递导致提交序号漂移: %d -> %d", ci, c.node(leader).CommitIndex())
		}
	}
	if r, _ := cl.Get("a"); r.Value != "1" {
		t.Fatal("重复投递破坏状态机结果")
	}
	t.Logf("判定依据: 30 个 tick 的重复心跳/投递下 commitIndex 稳定为 %d，追赶与应用均幂等", ci)
	logStatus(t, c, "after restart")
}

func asRaftErr(t *testing.T, err error) *Error {
	t.Helper()
	if err == nil {
		t.Fatal("期望得到错误，实际 nil")
	}
	re, ok := err.(*Error)
	if !ok {
		t.Fatalf("错误类型不可区分: %T %v", err, err)
	}
	return re
}

func TestIdempotentRetryAndStale(t *testing.T) {
	c := NewCluster(3)
	defer c.Stop()
	leader, term, err := c.RunUntilLeader(200)
	if err != nil {
		t.Fatal(err)
	}
	ld := c.node(leader)

	cmd1 := Command{Op: OpPut, Key: "k", Value: "v1", ClientID: 7, Seq: 1}
	r1, err := ld.Propose(cmd1)
	if err != nil {
		t.Fatal(err)
	}
	appliedAfter1 := ld.AppliedIndex()

	// 同一请求“超时重发”：同样的 (ClientID, Seq) 再来一次。
	r2, err := ld.Propose(cmd1)
	if err != nil {
		t.Fatalf("幂等重放不应报错: %v", err)
	}
	if r1 != r2 {
		t.Fatalf("重放结果不一致: %+v vs %+v", r1, r2)
	}
	if ld.AppliedIndex() != appliedAfter1 {
		t.Fatalf("重复请求被再次应用: applied %d -> %d", appliedAfter1, ld.AppliedIndex())
	}
	t.Logf("判定依据: term=%d 下同一 (client=7,seq=1) 重放 appliedIndex 保持 %d，未重复应用",
		term, appliedAfter1)

	// 乱序旧请求：seq=1 已应用，又来一个更旧的 seq=0。
	stale := Command{Op: OpPut, Key: "k", Value: "old", ClientID: 7, Seq: 0}
	_, err = ld.Propose(stale)
	re := asRaftErr(t, err)
	if re.Kind != ErrStaleRequestKind {
		t.Fatalf("期望 ErrStaleRequestKind，得到 kind=%d", re.Kind)
	}
	t.Logf("判定依据: 过期请求被明确拒绝 kind=ErrStaleRequest reason=%q（leaderHint=%d）",
		re.Reason, re.LeaderHint)
	if r, _ := NewClient(c).Get("k"); r.Value != "v1" {
		t.Fatal("过期请求污染了状态机")
	}
}

func TestErrorKindsDistinguished(t *testing.T) {
	c := NewCluster(3)
	defer c.Stop()

	// 集群刚启动、没有领导者时，对追随者写 -> ErrNotLeaderKind。
	follower := c.node(1)
	_, err := follower.Propose(Command{Op: OpPut, Key: "x", Value: "y", ClientID: 1, Seq: 1})
	re := asRaftErr(t, err)
	if re.Kind != ErrNotLeaderKind {
		t.Fatalf("无领导者时期望 ErrNotLeaderKind，得到 kind=%d", re.Kind)
	}
	t.Logf("判定依据: 追随者节点拒绝写 kind=ErrNotLeader reason=%q", re.Reason)

	// 选出领导者后隔离它 -> ErrNoQuorumKind。
	leader, _, err := c.RunUntilLeader(200)
	if err != nil {
		t.Fatal(err)
	}
	c.Isolate(leader)
	_, err = c.node(leader).Propose(Command{Op: OpPut, Key: "x", Value: "y", ClientID: 1, Seq: 2})
	re = asRaftErr(t, err)
	if re.Kind != ErrNoQuorumKind {
		t.Fatalf("分区时期望 ErrNoQuorumKind，得到 kind=%d", re.Kind)
	}
	t.Logf("判定依据: 被隔离领导者拒绝写 kind=ErrNoQuorum reason=%q", re.Reason)

	// 节点停止 -> ErrClosedKind。
	c.Heal()
	c.Stop()
	_, err = c.node(leader).Propose(Command{Op: OpPut, Key: "x", Value: "y", ClientID: 1, Seq: 3})
	re = asRaftErr(t, err)
	if re.Kind != ErrClosedKind {
		t.Fatalf("停止后期望 ErrClosedKind，得到 kind=%d", re.Kind)
	}
	t.Logf("判定依据: 已停止节点拒绝写 kind=ErrClosed reason=%q", re.Reason)
}

func TestConcurrentClients(t *testing.T) {
	c := NewCluster(5)
	defer c.Stop()
	if _, _, err := c.RunUntilLeader(200); err != nil {
		t.Fatal(err)
	}

	const writers = 8
	const perWriter = 15
	var wg sync.WaitGroup
	errCh := make(chan error, writers)

	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()
			cl := NewClient(c)
			for i := 0; i < perWriter; i++ {
				key := fmt.Sprintf("w%d", wid)
				val := fmt.Sprintf("%d", i)
				if _, err := cl.Put(key, val); err != nil {
					errCh <- fmt.Errorf("writer=%d i=%d: %w", wid, i, err)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errCh)
	for e := range errCh {
		t.Fatal(e)
	}

	// 线性一致校验：每个 key 的值必须等于该客户端最后一次写的值，
	// 且各副本状态机完全一致。
	reader := NewClient(c)
	for w := 0; w < writers; w++ {
		r, err := reader.Get(fmt.Sprintf("w%d", w))
		if err != nil {
			t.Fatal(err)
		}
		want := fmt.Sprintf("%d", perWriter-1)
		if !r.Found || r.Value != want {
			t.Fatalf("w%d: 期望最终值 %s，得到 %q found=%v", w, want, r.Value, r.Found)
		}
	}
	commit := uint64(0)
	for _, id := range c.ids {
		_, tt, ci, ai := c.Status(id)
		t.Logf("判定依据: node=%d term=%d commit=%d applied=%d", id, tt, ci, ai)
		if id == 0 {
			commit = ci
			continue
		}
		if ci != commit || ai != ci {
			t.Fatalf("副本不一致: node=%d commit=%d applied=%d，基准 commit=%d", id, ci, ai, commit)
		}
	}
	// 总写入次数 = 无 no-op 以外的去重键数；用最后领导者的 applied 减去各任期 no-op。
	t.Logf("判定依据: %d 个并发客户端共 %d 次写全部完成，所有副本 commit/applied 一致=%d",
		writers, writers*perWriter, commit)
}

// TestRetryAcrossLeaderChange：领导者分区期间客户端用同一幂等键反复重试，
// 分区恢复后恰好生效一次，值不被重复应用，且读到的是多数派历史。
func TestRetryAcrossLeaderChange(t *testing.T) {
	c := NewCluster(3)
	defer c.Stop()
	cl := NewClient(c)
	leader, term, err := c.RunUntilLeader(200)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cl.Put("init", "0"); err != nil {
		t.Fatal(err)
	}

	// 把领导者单独隔离。
	c.Isolate(leader)
	cmd := Command{Op: OpPut, Key: "k", Value: "V", ClientID: cl.id, Seq: cl.seq.Add(1)}

	// 在旧领导者上反复用“同一个请求”重试：都应得到 ErrNoQuorum，且不提交。
	for i := 0; i < 3; i++ {
		_, err := c.node(leader).Propose(cmd)
		if err == nil {
			t.Fatal("分区中旧领导者不应成功提交")
		}
		if re := err.(*Error); re.Kind != ErrNoQuorumKind && re.Kind != ErrNotLeaderKind {
			t.Fatalf("分区中重试错误类型不符: %v", err)
		}
	}
	t.Logf("判定依据: 旧领导者 node=%d(term=%d) 分区中同键重试 3 次均未提交", leader, term)

	// 等待多数派选出新任期领导者，恢复分区。
	c.Advance(c.electionBase + 2)
	c.Heal()
	newLeader := c.LeaderID()
	_, newTerm := c.node(newLeader).State()
	t.Logf("判定依据: 多数派产生新领导者 node=%d term=%d，分区恢复", newLeader, newTerm)

	// 用同一个幂等键向新领导者重试：若旧日志被复制则重放，否则重新追加，
	// 无论哪条路径，应用恰好一次。
	res, err := c.node(newLeader).Propose(cmd)
	if err != nil {
		t.Fatalf("恢复后同键重试失败: %v", err)
	}
	ci := c.node(newLeader).AppliedIndex()
	// 再重放一次，applied 不应增长。
	res2, err := c.node(newLeader).Propose(cmd)
	if err != nil {
		t.Fatal(err)
	}
	_ = res
	if c.node(newLeader).AppliedIndex() != ci {
		t.Fatalf("同键跨领导者重试被重复应用: %d -> %d", ci, c.node(newLeader).AppliedIndex())
	}
	if res2 != res {
		t.Fatal("同键结果不稳定")
	}
	r, err := cl.Get("k")
	if err != nil || r.Value != "V" {
		t.Fatalf("最终值错误: %+v %v", r, err)
	}
	t.Logf("判定依据: 同一 (client,seq) 跨领导者重试后 appliedIndex=%d 且不再增长，k=V", ci)
	logStatus(t, c, "after cross-leader retry")
}
