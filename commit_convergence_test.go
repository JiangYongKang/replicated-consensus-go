package raft

import (
	"errors"
	"testing"
)

// assertSameCommittedHistory 断言一组副本的已提交历史逐条一致：
// commitIndex 相同，且 [1, commitIndex] 内每条日志的任期与命令体相同。
func assertSameCommittedHistory(t *testing.T, env *SimEnv, ids []string) {
	t.Helper()
	ref := env.Nodes[ids[0]]
	wantCommit := ref.CommitIndex()
	for _, id := range ids[1:] {
		nd := env.Nodes[id]
		if nd.CommitIndex() != wantCommit {
			t.Fatalf("node %s commit=%d, want converged commit=%d (ref=%s)",
				id, nd.CommitIndex(), wantCommit, ids[0])
		}
	}
	for idx := uint64(1); idx <= wantCommit; idx++ {
		refEntry, err := ref.cfg.Storage.Entry(idx)
		if err != nil {
			t.Fatalf("ref %s entry %d: %v", ids[0], idx, err)
		}
		for _, id := range ids[1:] {
			e, err := env.Nodes[id].cfg.Storage.Entry(idx)
			if err != nil {
				t.Fatalf("node %s missing committed entry %d: %v", id, idx, err)
			}
			if e.Term != refEntry.Term || e.Cmd != refEntry.Cmd {
				t.Fatalf("committed history diverged at index=%d: %s=(term=%d,cmd=%+v) %s=(term=%d,cmd=%+v)",
					idx, ids[0], refEntry.Term, refEntry.Cmd, id, e.Term, e.Cmd)
			}
		}
	}
	t.Logf("committed history identical across %v: commit=%d", ids, wantCommit)
}

// TestCommitConvergenceAfterPartitionHeal 验证领导更替完成后的提交收敛：
//   - 少数派旧领导带着一条未获多数确认的写（ghost）被分区；
//   - 多数派选出新领导并继续提交新写；
//   - 分区恢复后，所有可达副本的已提交历史收敛到同一点（逐条一致），
//     ghost 条目被截断、绝不会因为换了领导而变成已提交；
//   - 后续写从这条一致的历史上继续推进。
func TestCommitConvergenceAfterPartitionHeal(t *testing.T) {
	env := NewSimEnv(3).WithLogger(testLogger{t})
	defer env.Close()

	leader := env.Leader(2000)
	if leader == "" {
		t.Fatal("no leader")
	}
	cl := NewClient("conv", env.Nodes, env.Sched)
	f1 := cl.Set(nil, "a", "1")
	waitDone(t, env, f1, 2000, "write a=1")
	if err := f1.Err(); err != nil {
		t.Fatalf("write a=1: %v", err)
	}

	// 少数派分区：旧领导单独隔离。它随后收到的写永远凑不齐多数确认。
	env.Isolate(leader)
	minority := env.Nodes[leader]
	ghost := minority.Propose(nil, Command{Kind: CmdSet, Key: "ghost", Val: "x", ClientID: "g", Seq: 1})
	waitDone(t, env, ghost, 1500, "minority ghost write must fail")
	var we *WriteError
	if err := ghost.Err(); err == nil || !errors.As(err, &we) ||
		(we.Reason != ReasonNoQuorum && we.Reason != ReasonTimeout && we.Reason != ReasonNotLeader) {
		t.Fatalf("ghost write err=%v, want distinguishable no_quorum/timeout/not_leader", err)
	}
	ghostIdx := minority.cfg.Storage.LastIndex()
	t.Logf("PHASE-1 minority leader=%s holds uncommitted ghost at index=%d term=%d",
		leader, ghostIdx, minority.Term())

	// 多数派选出新领导并提交新写。
	env.Converge(3000)
	newLeader := ""
	for _, id := range env.IDs {
		if id != leader && env.Nodes[id].CurrentRole() == Leader {
			newLeader = id
		}
	}
	if newLeader == "" {
		t.Fatal("majority side failed to elect")
	}
	cl2 := NewClient("conv2", env.Nodes, env.Sched)
	f2 := cl2.Set(nil, "b", "2")
	waitDone(t, env, f2, 3000, "write b=2 in majority")
	if err := f2.Err(); err != nil {
		t.Fatalf("majority write b=2: %v", err)
	}
	t.Logf("PHASE-2 majority leader=%s term=%d commit=%d", newLeader, env.Nodes[newLeader].Term(), env.Nodes[newLeader].CommitIndex())

	// 分区恢复：旧领导必须放弃未确认的 ghost、追平多数派历史。
	env.HealAll()
	env.Converge(6000)

	assertSameCommittedHistory(t, env, env.IDs)
	for _, id := range env.IDs {
		nd := env.Nodes[id]
		if _, found := nd.cfg.SM.Read("ghost"); found {
			t.Fatalf("node %s applied uncommitted minority write (ghost became committed!)", id)
		}
		// ghost 占用的日志位置必须已被多数派历史覆盖或截断：
		// 该位置若仍存在，任期/命令必须与新领导的已提交历史一致。
		e, err := env.Nodes[newLeader].cfg.Storage.Entry(ghostIdx)
		if err == nil {
			mine, err2 := nd.cfg.Storage.Entry(ghostIdx)
			if err2 != nil || mine != e {
				t.Fatalf("node %s diverged entry at ghost index=%d", id, ghostIdx)
			}
		}
		if got := mustRead(nd, "a"); got != "1" {
			t.Fatalf("node %s a=%q, want 1", id, got)
		}
		if got := mustRead(nd, "b"); got != "2" {
			t.Fatalf("node %s b=%q, want 2", id, got)
		}
		t.Logf("converged node=%s term=%d commit=%d ghost=absent a=1 b=2", id, nd.Term(), nd.CommitIndex())
	}

	// 后续写必须从收敛后的历史上继续推进并被全部副本确认。
	f3 := cl2.Set(nil, "c", "3")
	waitDone(t, env, f3, 3000, "write c=3 after heal")
	if err := f3.Err(); err != nil {
		t.Fatalf("post-heal write c=3: %v", err)
	}
	env.Converge(3000)
	assertSameCommittedHistory(t, env, env.IDs)
	for _, id := range env.IDs {
		if got := mustRead(env.Nodes[id], "c"); got != "3" {
			t.Fatalf("node %s c=%q, want 3", id, got)
		}
	}
	t.Logf("PHASE-3 post-convergence write c=3 replicated to all; final commit=%d",
		env.Nodes[env.IDs[0]].CommitIndex())
}

// TestLaggingReplicaCaughtUpOrRejected 验证进度明显落后的副本：
// 恢复连通后被新领导追平到同一提交点；在追平之前它作为跟随者，
// 任何读/写请求都必须以 not_leader 明确拒绝，绝不拿着落后的提交点对外服务。
func TestLaggingReplicaCaughtUpOrRejected(t *testing.T) {
	env := NewSimEnv(3).WithLogger(testLogger{t})
	defer env.Close()

	leader := env.Leader(2000)
	if leader == "" {
		t.Fatal("no leader")
	}
	lagging := pickFollower(env, leader)

	// 隔离一个追随者，期间多数派持续提交多条写。
	env.Isolate(lagging)
	cl := NewClient("lag", env.Nodes, env.Sched)
	for _, kv := range [][2]string{{"k1", "1"}, {"k2", "2"}, {"k3", "3"}} {
		f := cl.Set(nil, kv[0], kv[1])
		waitDone(t, env, f, 2000, "write "+kv[0])
		if err := f.Err(); err != nil {
			t.Fatalf("write %s: %v", kv[0], err)
		}
	}
	commitAhead := env.Nodes[leader].CommitIndex()
	lagCommit := env.Nodes[lagging].CommitIndex()
	if lagCommit >= commitAhead {
		t.Fatalf("setup broken: %s commit=%d should lag behind %d", lagging, lagCommit, commitAhead)
	}
	t.Logf("PHASE-1 %s lagging: commit=%d vs leader commit=%d", lagging, lagCommit, commitAhead)

	// 落后期间：该副本不是领导者，读/写都必须明确拒绝（not_leader），
	// 而不是返回落后的本地值。
	rf := env.Nodes[lagging].Read(nil, "k1")
	waitDone(t, env, rf, 1000, "lagging replica read rejected")
	if err := rf.Err(); err == nil {
		t.Fatal("lagging replica served a read with a stale commit point")
	} else {
		var we *WriteError
		if !errors.As(err, &we) || we.Reason != ReasonNotLeader {
			t.Fatalf("lagging read reason=%v, want not_leader", err)
		}
	}
	wf := env.Nodes[lagging].Propose(nil, Command{Kind: CmdSet, Key: "x", Val: "y", ClientID: "lag", Seq: 1})
	waitDone(t, env, wf, 1000, "lagging replica write rejected")
	if err := wf.Err(); err == nil {
		t.Fatal("lagging replica accepted a write")
	} else {
		var we *WriteError
		if !errors.As(err, &we) || we.Reason != ReasonNotLeader {
			t.Fatalf("lagging write reason=%v, want not_leader", err)
		}
	}
	t.Logf("PHASE-2 lagging replica rejected read+write as not_leader while commit=%d", lagCommit)

	// 恢复连通：必须被追平到与领导者完全一致的已提交历史。
	env.Heal(lagging)
	env.Converge(6000)
	assertSameCommittedHistory(t, env, env.IDs)
	for _, kv := range [][2]string{{"k1", "1"}, {"k2", "2"}, {"k3", "3"}} {
		if got := mustRead(env.Nodes[lagging], kv[0]); got != kv[1] {
			t.Fatalf("lagging replica %s=%q after catch-up, want %s", kv[0], got, kv[1])
		}
	}
	t.Logf("PHASE-3 %s caught up: commit=%d (basis: idempotent append catch-up from leader)",
		lagging, env.Nodes[lagging].CommitIndex())
}
