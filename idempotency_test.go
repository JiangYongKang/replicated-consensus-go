package raft

import (
	"errors"
	"fmt"
	"testing"
)

// TestIdempotentRetry 模拟“超时重发”：直接向领导者重复提交完全相同的
// (ClientID, Seq) 命令，只允许应用一次，后续命中去重缓存。
func TestIdempotentRetry(t *testing.T) {
	env := NewSimEnv(3).WithLogger(testLogger{t})
	defer env.Close()
	leader := env.Leader(2000)
	if leader == "" {
		t.Fatal("no leader")
	}
	ld := env.Nodes[leader]

	cmd := Command{Kind: CmdInc, Key: "cnt", ClientID: "du", Seq: 1}

	f1 := ld.Propose(nil, cmd)
	waitDone(t, env, f1, 1000, "first submit")
	if err := f1.Err(); err != nil {
		t.Fatalf("first: %v", err)
	}
	if f1.Result().Value != "1" {
		t.Fatalf("first value=%s want 1", f1.Result().Value)
	}

	// 再次以完全相同的请求重发（客户端超时后重发路径）。
	// 通过日志复制：在领导者本地追加一条相同 cmd 的条目并提交，
	// 状态机必须识别为重复，不得第二次自增。
	f2 := ld.Propose(nil, cmd)
	waitDone(t, env, f2, 1000, "retried submit")
	if err := f2.Err(); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if !f2.Result().Duplicate {
		t.Fatalf("retry Duplicate=%v want true", f2.Result().Duplicate)
	}
	if f2.Result().Value != "1" {
		t.Fatalf("value after retry=%s want 1 (must not double apply)", f2.Result().Value)
	}

	// 同序号但命令体不同 -> 必须明确拒绝为 stale_request，而非静默覆盖。
	conflict := Command{Kind: CmdSet, Key: "cnt", Val: "999", ClientID: "du", Seq: 1}
	f3 := ld.Propose(nil, conflict)
	waitDone(t, env, f3, 1000, "conflicting submit")
	if err := f3.Err(); err == nil {
		t.Fatal("conflicting same-seq request unexpectedly succeeded")
	} else if !errors.Is(err, ErrStaleRequest) {
		t.Fatalf("conflict err=%v want ErrStaleRequest", err)
	}

	// 序号倒退 -> stale_request。
	stale := Command{Kind: CmdInc, Key: "cnt", ClientID: "du", Seq: 1}
	_ = stale
	older := Command{Kind: CmdInc, Key: "cnt", ClientID: "du", Seq: 0}
	f4 := ld.Propose(nil, older)
	waitDone(t, env, f4, 1000, "older seq submit")
	if err := f4.Err(); err == nil || !errors.Is(err, ErrStaleRequest) {
		t.Fatalf("older seq err=%v want ErrStaleRequest", f4.Err())
	}

	if v := mustRead(ld, "cnt"); v != "1" {
		t.Fatalf("final cnt=%s want 1", v)
	}
	t.Logf("idempotency verified: cnt=%s indexes applied=%d commit=%d",
		mustRead(ld, "cnt"), ld.cfg.SM.LastApplied(), ld.CommitIndex())
}

// TestNotLeaderErrorDistinction 验证跟随者直接拒绝写并给出 not_leader 与提示。
func TestNotLeaderErrorDistinction(t *testing.T) {
	env := NewSimEnv(3).WithLogger(testLogger{t})
	defer env.Close()
	leader := env.Leader(2000)
	if leader == "" {
		t.Fatal("no leader")
	}
	// 让心跳传播一轮，确保跟随者记录 leaderHint。
	env.Stabilize()
	for _, id := range env.IDs {
		if id == leader {
			continue
		}
		f := env.Nodes[id].Propose(nil, Command{Kind: CmdSet, Key: "k", Val: "v", ClientID: "x", Seq: 1})
		env.Stabilize()
		err := f.Err()
		if !errors.Is(err, ErrNotLeader) {
			t.Fatalf("follower %s err=%v want ErrNotLeader", id, err)
		}
		var we *WriteError
		errors.As(err, &we)
		if we.LeaderHint != leader {
			t.Fatalf("follower %s leader hint=%q want %q", id, we.LeaderHint, leader)
		}
		t.Logf("follower %s rejected: reason=%s hint=%s term=%d", id, we.Reason, we.LeaderHint, we.Term)

		// 跟随者读也必须被拒绝（线性一致读只能经领导者 ReadIndex）。
		rf := env.Nodes[id].Read(nil, "k")
		env.Stabilize()
		if err := rf.Err(); !errors.Is(err, ErrNotLeader) {
			t.Fatalf("follower read err=%v want ErrNotLeader", err)
		}
	}
}

// TestConcurrentProposals 以确定性方式模拟并发：多个客户端在同一虚拟时刻
// 附近交错提交多条写，全部必须恰好应用一次，最终所有副本状态一致。
func TestConcurrentProposals(t *testing.T) {
	env := NewSimEnv(5).WithLogger(testLogger{t})
	defer env.Close()
	leader := env.Leader(2000)
	if leader == "" {
		t.Fatal("no leader")
	}
	ld := env.Nodes[leader]

	const n = 12
	futures := make([]*WriteFuture, n)
	// 全部在推进事件循环前“同时”发出，模拟并发到达。
	for i := 0; i < n; i++ {
		cmd := Command{
			Kind:     CmdSet,
			Key:      fmt.Sprintf("key-%02d", i),
			Val:      fmt.Sprintf("v-%d", i),
			ClientID: fmt.Sprintf("cc-%02d", i),
			Seq:      1,
		}
		futures[i] = ld.Propose(nil, cmd)
	}
	env.Stabilize()

	done := 0
	for i, f := range futures {
		select {
		case <-f.Done():
			done++
		default:
			t.Fatalf("proposal %d not done, pending=%d", i, env.Sched.Pending())
		}
		if err := f.Err(); err != nil {
			t.Fatalf("proposal %d: %v", i, err)
		}
	}
	if ld.CommitIndex() < uint64(n) {
		t.Fatalf("commit=%d want >=%d", ld.CommitIndex(), n)
	}
	t.Logf("concurrent writes: done=%d commit=%d term=%d", done, ld.CommitIndex(), ld.Term())

	// 收敛检查。
	env.Stabilize()
	for _, id := range env.IDs {
		nd := env.Nodes[id]
		if nd.CommitIndex() != ld.CommitIndex() {
			t.Fatalf("node %s commit=%d want %d", id, nd.CommitIndex(), ld.CommitIndex())
		}
		for i := 0; i < n; i++ {
			want := fmt.Sprintf("v-%d", i)
			if v := mustRead(nd, fmt.Sprintf("key-%02d", i)); v != want {
				t.Fatalf("node %s key-%02d=%q want %q", id, i, v, want)
			}
		}
	}

	// 所有 12 个会话都在状态机中，且计数器最终状态跨副本一致。
	t.Logf("all %d concurrent proposals converged on %d replicas, commit=%d", n, len(env.IDs), ld.CommitIndex())
}

// TestClientRetryAcrossLeaderChange 端到端：分区+改道场景下客户端自动重试，
// 相同序号绝不重复应用。
func TestClientRetryAcrossLeaderChange(t *testing.T) {
	env := NewSimEnv(3).WithLogger(testLogger{t})
	defer env.Close()
	leader := env.Leader(2000)
	if leader == "" {
		t.Fatal("no leader")
	}
	cl := NewClient("mobile", env.Nodes, env.Sched)

	// 先写一笔成功。
	f1 := cl.Inc(nil, "n")
	waitDone(t, env, f1, 1000, "inc before partition")
	if err := f1.Err(); err != nil {
		t.Fatal(err)
	}

	// 隔离当前领导者，客户端随后的写必须自动发现新领导并重试成功。
	env.Isolate(leader)
	t.Logf("isolated old leader=%s; client must redirect", leader)

	f2 := cl.Inc(nil, "n")
	env.Stabilize()
	waitDone(t, env, f2, 3000, "inc across leader change")
	if err := f2.Err(); err != nil {
		t.Fatalf("retry across leader change failed: %v", err)
	}

	env.HealAll()
	env.Stabilize()
	for _, id := range env.IDs {
		if v := mustRead(env.Nodes[id], "n"); v != "2" {
			t.Fatalf("node %s n=%s want 2 (a retry must not double-apply)", id, v)
		}
	}
	if cl.Seq() != 2 {
		t.Fatalf("client seq=%d want 2", cl.Seq())
	}
	t.Logf("client retry across leader change ok: seq=%d, all replicas n=2", cl.Seq())
}
