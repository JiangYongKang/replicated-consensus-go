package raft

import (
	"errors"
	"testing"
)

// pickFollower 返回除 leader 外字典序最小的节点 ID。
func pickFollower(env *SimEnv, leader string) string {
	for _, id := range env.IDs {
		if id != leader {
			return id
		}
	}
	return ""
}

// TestReadYourWritesAfterLeaderChange 验证「领导更替后仍能读到已确认写入」：
//   - 写 k=v1 在旧领导任期确认成功；
//   - 副本 B 收到了日志条目但没来得及收到提交通知（提交广播被分区丢弃）；
//   - 旧领导被隔离后，B 以更高任期当选——B 的 commitIndex 明显落后于
//     已提交历史，必须靠本任期 no-op 栅栏把提交点推进到覆盖 k=v1 之后，
//     客户端改道到 B 的读才允许返回，且必须读到 v1（不是空值/旧值）。
func TestReadYourWritesAfterLeaderChange(t *testing.T) {
	env := NewSimEnv(3).WithLogger(testLogger{t})
	defer env.Close()

	leader := env.Leader(2000)
	if leader == "" {
		t.Fatal("no leader")
	}
	followerB := pickFollower(env, leader)
	// 让另一个追随者不是 B：B 将被塑造成“有条目但错过提交通知”的副本。
	cl := NewClient("ryw", env.Nodes, env.Sched)

	// 预热写：让客户端记住领导者提示，随后的关键写才能同步直达领导者，
	// 从而保证“隔离 B”发生在条目广播之后、提交广播之前的确定时间窗内。
	warm := cl.Set(nil, "warmup", "1")
	waitDone(t, env, warm, 2000, "warmup write")
	if err := warm.Err(); err != nil {
		t.Fatalf("warmup write: %v", err)
	}

	// 发起关键写：Propose 同步把条目广播给两个追随者（已在途，必送达），
	// 随后立即隔离 B——B 的确认回执与领导者随后的提交广播都会被分区丢弃，
	// 于是 B “有日志条目但没来得及收到提交通知”。
	wf := cl.Set(nil, "k", "v1")
	env.Isolate(followerB)
	waitDone(t, env, wf, 2000, "set k=v1")
	if err := wf.Err(); err != nil {
		t.Fatalf("write k=v1: %v", err)
	}
	t.Logf("PHASE-1 write committed by old leader=%s term=%d; follower %s missed commit notification",
		leader, env.Nodes[leader].Term(), followerB)

	ndB := env.Nodes[followerB]
	if ndB.CommitIndex() >= env.Nodes[leader].CommitIndex() {
		t.Fatalf("setup broken: %s commit=%d should lag behind leader commit=%d",
			followerB, ndB.CommitIndex(), env.Nodes[leader].CommitIndex())
	}
	if ndB.cfg.Storage.LastIndex() != env.Nodes[leader].cfg.Storage.LastIndex() {
		t.Fatalf("setup broken: %s lastIndex=%d should hold the same entries as leader lastIndex=%d",
			followerB, ndB.cfg.Storage.LastIndex(), env.Nodes[leader].cfg.Storage.LastIndex())
	}
	t.Logf("PHASE-1 %s lagging: commit=%d lastIndex=%d (leader commit=%d)",
		followerB, ndB.CommitIndex(), ndB.cfg.Storage.LastIndex(), env.Nodes[leader].CommitIndex())

	// 换帅：先恢复 B 的连通（否则两次 Isolate 叠加会把 B 与 A 也断开），
	// 再隔离旧领导——此后 A 与 B 相互可达、旧领导被排除在多数派之外。
	env.Heal(followerB)
	env.Isolate(leader)
	t.Logf("PHASE-2 isolated old leader=%s; waiting for new election", leader)
	env.Converge(4000)

	newLeader := ""
	for _, id := range env.IDs {
		if id != leader && env.Nodes[id].CurrentRole() == Leader {
			newLeader = id
		}
	}
	if newLeader == "" {
		t.Fatal("no new leader among reachable nodes")
	}
	t.Logf("PHASE-2 new leader=%s term=%d commit=%d", newLeader, env.Nodes[newLeader].Term(), env.Nodes[newLeader].CommitIndex())

	// 客户端自动改道：旧领导的读先以 no_quorum 失败，重试后落到新领导，
	// 必须读到已确认的 v1。
	rf := cl.Get(nil, "k")
	waitDone(t, env, rf, 5000, "read-your-writes after leader change")
	v, ok, err := rf.Get()
	if err != nil {
		t.Fatalf("read after leader change: %v", err)
	}
	if !ok || v != "v1" {
		t.Fatalf("read-your-writes violated: k=%q ok=%v, want v1", v, ok)
	}
	t.Logf("PHASE-3 read-your-writes ok: k=%s served by term=%d commit=%d",
		v, env.Nodes[newLeader].Term(), env.Nodes[newLeader].CommitIndex())

	// 后续写必须接在一致的历史上继续推进。
	wf2 := cl.Set(nil, "k2", "v2")
	waitDone(t, env, wf2, 3000, "write k2=v2 after leader change")
	if err := wf2.Err(); err != nil {
		t.Fatalf("write after leader change: %v", err)
	}

	// 恢复旧领导：全部副本的已提交历史必须收敛到同一点。
	env.HealAll()
	env.Converge(6000)
	wantCommit := env.Nodes[newLeader].CommitIndex()
	for _, id := range env.IDs {
		nd := env.Nodes[id]
		if nd.CommitIndex() != wantCommit {
			t.Fatalf("node %s commit=%d, want converged %d", id, nd.CommitIndex(), wantCommit)
		}
		if got := mustRead(nd, "k"); got != "v1" {
			t.Fatalf("node %s k=%q, want v1", id, got)
		}
		if got := mustRead(nd, "k2"); got != "v2" {
			t.Fatalf("node %s k2=%q, want v2", id, got)
		}
		t.Logf("converged node=%s term=%d commit=%d k=v1 k2=v2", id, nd.Term(), nd.CommitIndex())
	}
}

// TestReadYourWritesAfterLeaderRestart 验证旧领导被隔离并重启、再重新加入后，
// 客户端改道到新领导的读依然能读到重启前已确认的写入。
func TestReadYourWritesAfterLeaderRestart(t *testing.T) {
	env := NewSimEnv(3).WithLogger(testLogger{t})
	defer env.Close()

	leader := env.Leader(2000)
	if leader == "" {
		t.Fatal("no leader")
	}
	cl := NewClient("ryw-restart", env.Nodes, env.Sched)

	wf := cl.Set(nil, "cfg", "on")
	waitDone(t, env, wf, 2000, "set cfg=on")
	if err := wf.Err(); err != nil {
		t.Fatalf("write cfg=on: %v", err)
	}

	// 旧领导被隔离并在隔离期间重启（进程崩溃恢复），重新加入前集群已换帅。
	env.Isolate(leader)
	if err := env.RestartNode(leader); err != nil {
		t.Fatalf("restart old leader: %v", err)
	}
	t.Logf("PHASE-1 old leader=%s isolated+restarted", leader)
	env.Converge(4000)

	newLeader := ""
	for _, id := range env.IDs {
		if id != leader && env.Nodes[id].CurrentRole() == Leader {
			newLeader = id
		}
	}
	if newLeader == "" {
		t.Fatal("no new leader after old leader restart")
	}

	rf := cl.Get(nil, "cfg")
	waitDone(t, env, rf, 5000, "read-your-writes after leader restart")
	v, ok, err := rf.Get()
	if err != nil {
		t.Fatalf("read after leader restart: %v", err)
	}
	if !ok || v != "on" {
		t.Fatalf("read-your-writes violated after restart: cfg=%q ok=%v, want on", v, ok)
	}
	t.Logf("PHASE-2 read-your-writes ok: cfg=%s newLeader=%s term=%d", v, newLeader, env.Nodes[newLeader].Term())

	// 旧领导带伤回归后必须追平，而不是拿着落后的提交点对外服务。
	env.HealAll()
	env.Converge(6000)
	if got := mustRead(env.Nodes[leader], "cfg"); got != "on" {
		t.Fatalf("rejoined old leader cfg=%q, want on", got)
	}
	rf2 := env.Nodes[leader].Read(nil, "cfg")
	waitDone(t, env, rf2, 1000, "follower read rejected")
	if err := rf2.Err(); err == nil {
		t.Fatal("rejoined follower served a read directly; must reject as not_leader")
	} else {
		var we *WriteError
		if !errors.As(err, &we) || we.Reason != ReasonNotLeader {
			t.Fatalf("follower read reason=%v, want not_leader", err)
		}
		t.Logf("PHASE-3 rejoined follower read rejected reason=%s (basis: only leader serves reads)", we.Reason)
	}
}
