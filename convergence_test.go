package raft

import (
	"testing"
)

// TestCommitConvergenceAfterLeaderChange 验证领导更替后的提交收敛：
//
//  1. 旧领导被隔离前提交 k1；隔离期间旧领导的未多数确认写（ghost）必须失败；
//  2. 多数派选出新领导后继续提交 k2、k3，此时多数派中还有一个副本 F 被隔离
//     （进度明显落后）；
//  3. 全部恢复并推进事件后：所有可达副本的 term/commitIndex 收敛到同一点，
//     状态机内容一致，ghost 在任何副本上都不存在；
//  4. 后续写入 k4 从这条一致历史继续追加，并再次收敛到全部副本。
func TestCommitConvergenceAfterLeaderChange(t *testing.T) {
	env := NewSimEnv(5).WithLogger(testLogger{t})
	defer env.Close()

	leader := env.Leader(2000)
	if leader == "" {
		t.Fatal("no leader")
	}
	cl := NewClient("conv", env.Nodes, env.Sched)

	w1 := cl.Set(nil, "k1", "v1")
	waitDone(t, env, w1, 2000, "write k1=v1")
	if err := w1.Err(); err != nil {
		t.Fatalf("write k1: %v", err)
	}
	t.Logf("PHASE-1 k1 committed on leader=%s term=%d commit=%d",
		leader, env.Nodes[leader].Term(), env.Nodes[leader].CommitIndex())

	// 隔离旧领导（1/5 少数派）；其未多数确认的写必须明确失败。
	env.Isolate(leader)
	ghost := env.Nodes[leader].Propose(nil, Command{Kind: CmdSet, Key: "ghost", Val: "x", ClientID: "ghost", Seq: 1})
	waitDone(t, env, ghost, 1500, "minority ghost write")
	if ghost.Err() == nil {
		t.Fatal("uncommitted minority write unexpectedly succeeded")
	}
	t.Logf("PHASE-2 old leader %s isolated; ghost write rejected: %v", leader, ghost.Err())

	// 多数派选出新领导；再隔离多数派中的一个跟随者 F，制造明显落后的副本。
	env.Converge(4000)
	newLeader := ""
	for _, id := range env.IDs {
		if id != leader && env.Nodes[id].CurrentRole() == Leader {
			newLeader = id
		}
	}
	if newLeader == "" {
		t.Fatal("majority side failed to elect a new leader")
	}
	lagging := ""
	for _, id := range env.IDs {
		if id != leader && id != newLeader {
			lagging = id
			break
		}
	}
	env.Isolate(lagging)
	t.Logf("PHASE-3 new leader=%s term=%d; lagging replica %s isolated",
		newLeader, env.Nodes[newLeader].Term(), lagging)

	// 新领导在 3/5 可达多数上继续提交 k2、k3。
	for _, kv := range [][2]string{{"k2", "v2"}, {"k3", "v3"}} {
		f := cl.Set(nil, kv[0], kv[1])
		waitDone(t, env, f, 3000, "write "+kv[0])
		if err := f.Err(); err != nil {
			t.Fatalf("write %s: %v", kv[0], err)
		}
	}
	lagCommit := env.Nodes[lagging].CommitIndex()
	headCommit := env.Nodes[newLeader].CommitIndex()
	if lagCommit >= headCommit {
		t.Fatalf("precondition broken: lagging %s commit=%d should be behind leader commit=%d",
			lagging, lagCommit, headCommit)
	}
	t.Logf("PHASE-4 k2,k3 committed: leader commit=%d; lagging %s still at commit=%d",
		headCommit, lagging, lagCommit)

	// 全部恢复：落后副本被追平，旧领导降为新任期的跟随者。
	env.HealAll()
	env.Converge(6000)

	// 收敛断言：所有副本的 term/commitIndex 一致，状态机内容一致，ghost 不存在。
	wantTerm := env.Nodes[newLeader].Term()
	wantCommit := env.Nodes[newLeader].CommitIndex()
	for _, id := range env.IDs {
		nd := env.Nodes[id]
		if nd.Term() != wantTerm {
			t.Fatalf("node %s term=%d, want converged term=%d", id, nd.Term(), wantTerm)
		}
		if nd.CommitIndex() != wantCommit {
			t.Fatalf("node %s commit=%d, want converged commit=%d (lagging replica not caught up)",
				id, nd.CommitIndex(), wantCommit)
		}
		for _, kv := range [][2]string{{"k1", "v1"}, {"k2", "v2"}, {"k3", "v3"}} {
			if v, ok := nd.cfg.SM.Read(kv[0]); !ok || v != kv[1] {
				t.Fatalf("node %s %s=%q/%v, want %s", id, kv[0], v, ok, kv[1])
			}
		}
		if _, ghost := nd.cfg.SM.Read("ghost"); ghost {
			t.Fatalf("node %s applied an uncommitted minority write!", id)
		}
	}
	t.Logf("PHASE-5 converged: all %d replicas term=%d commit=%d, k1..k3 present, ghost absent",
		len(env.IDs), wantTerm, wantCommit)

	// 后续写入从一致历史继续追加，并再次收敛到全部副本。
	w4 := cl.Set(nil, "k4", "v4")
	waitDone(t, env, w4, 3000, "write k4=v4")
	if err := w4.Err(); err != nil {
		t.Fatalf("write k4 after convergence: %v", err)
	}
	env.Converge(4000)
	finalCommit := env.Nodes[newLeader].CommitIndex()
	if finalCommit <= wantCommit {
		t.Fatalf("commit did not advance after k4: %d <= %d", finalCommit, wantCommit)
	}
	for _, id := range env.IDs {
		nd := env.Nodes[id]
		if nd.CommitIndex() != finalCommit {
			t.Fatalf("node %s commit=%d, want %d after k4", id, nd.CommitIndex(), finalCommit)
		}
		if v, ok := nd.cfg.SM.Read("k4"); !ok || v != "v4" {
			t.Fatalf("node %s k4=%q/%v, want v4", id, v, ok)
		}
	}
	t.Logf("PHASE-6 post-convergence write k4 replicated on consistent history: all commit=%d", finalCommit)
}
