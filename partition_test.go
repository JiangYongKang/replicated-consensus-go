package raft

import (
	"errors"
	"testing"
)

type doneFuture interface {
	Done() <-chan struct{}
}

func waitDone(t *testing.T, env *SimEnv, f doneFuture, ms int64, what string) {
	t.Helper()
	deadline := env.Clock.Now() + ms
	// 驱动策略：在 deadline 前不断触发事件；当“外部事件已清空”（无网络消息/
	// 客户端重试）时，再补触发一轮内部事件以兑现提案/读取超时，之后结束——
	// 这样既允许等待内部超时，又不会在永久分区的无限重选中空转。
	for env.Clock.Now() < deadline {
		select {
		case <-f.Done():
			return
		default:
		}
		if env.Sched.PendingExternal() > 0 {
			env.Sched.StepExternal()
			continue
		}
		env.Sched.RunFor(ms)
		break
	}
	select {
	case <-f.Done():
		return
	default:
		t.Fatalf("%s: not completed by vtime %d (pending external=%d)",
			what, deadline, env.Sched.PendingExternal())
	}
}

// TestPartitionMinorityCannotWrite 验证：
//   - 多数派分区中的领导者可持续写入；
//   - 被隔离的少数派领导者写请求得到可区分的 no_quorum，且其本地状态机不变；
//   - 分区恢复后所有副本的已提交历史与状态完全收敛。
func TestPartitionMinorityCannotWrite(t *testing.T) {
	env := NewSimEnv(3).WithLogger(testLogger{t})
	defer env.Close()

	leader := env.Leader(2000)
	if leader == "" {
		t.Fatal("no leader")
	}
	t.Logf("PHASE-1 leader=%s term=%d", leader, env.Nodes[leader].Term())

	cl := NewClient("cA", env.Nodes, env.Sched)
	f1 := cl.Set(nil, "x", "1")
	waitDone(t, env, f1, 1000, "write x=1")
	if err := f1.Err(); err != nil {
		t.Fatalf("write x=1: %v", err)
	}

	// 把领导者单独隔离（1/3 少数派）。
	env.Isolate(leader)
	t.Logf("PHASE-2 isolated leader=%s (minority partition)", leader)

	// 隔离后直接向旧领导者提交：它收不到多数确认，必须失败为 no_quorum/timeout。
	minority := env.Nodes[leader]
	f2 := minority.Propose(nil, Command{Kind: CmdSet, Key: "ghost", Val: "z", ClientID: "ghost", Seq: 1})
	waitDone(t, env, f2, 1000, "minority write rejection")
	if err := f2.Err(); err == nil {
		t.Fatal("minority write unexpectedly succeeded (must not be visible)")
	} else {
		var we *WriteError
		if !errors.As(err, &we) {
			t.Fatalf("minority write error not *WriteError: %v", err)
		}
		if we.Reason != ReasonNoQuorum && we.Reason != ReasonTimeout {
			t.Fatalf("minority write reason=%s, want no_quorum/timeout", we.Reason)
		}
		t.Logf("minority write rejected reason=%s term=%d detail=%q", we.Reason, we.Term, we.Detail)
	}

	// 多数派侧应选出新领导，新领导可继续写。
	env.Stabilize()
	majorityLeader := ""
	for id, nd := range env.Nodes {
		if id == leader {
			continue
		}
		if nd.CurrentRole() == Leader {
			majorityLeader = id
		}
	}
	if majorityLeader == "" {
		t.Fatal("majority side failed to elect a new leader")
	}
	t.Logf("PHASE-3 majority elected new leader=%s term=%d", majorityLeader, env.Nodes[majorityLeader].Term())
	if env.Nodes[majorityLeader].Term() <= env.Nodes[leader].Term() {
		t.Fatalf("new leader term=%d must exceed old leader term=%d",
			env.Nodes[majorityLeader].Term(), env.Nodes[leader].Term())
	}

	cl2 := NewClient("cB", env.Nodes, env.Sched)
	f3 := cl2.Set(nil, "y", "2")
	env.Stabilize()
	waitDone(t, env, f3, 1000, "write y=2 in majority")
	if err := f3.Err(); err != nil {
		t.Fatalf("majority write y=2: %v", err)
	}

	// 恢复分区：旧领导必须以更高任期退回跟随者，并追平已提交历史。
	env.HealAll()
	t.Log("PHASE-4 partition healed; expecting convergence")
	env.Stabilize()

	// 提交序号的具体取值随任期起始 no-op 条目数量变化，这里断言“全部收敛到同一点”。
	wantCommit := env.Nodes[majorityLeader].CommitIndex()
	for _, id := range env.IDs {
		nd := env.Nodes[id]
		if nd.Term() != env.Nodes[majorityLeader].Term() {
			t.Fatalf("node %s term=%d, want converged term=%d",
				id, nd.Term(), env.Nodes[majorityLeader].Term())
		}
		if nd.CommitIndex() != wantCommit {
			t.Fatalf("node %s commit=%d, want converged commit=%d", id, nd.CommitIndex(), wantCommit)
		}
		v, ok := nd.cfg.SM.Read("x")
		if !ok || v != "1" {
			t.Fatalf("node %s x=%q/%v want 1", id, v, ok)
		}
		v, ok = nd.cfg.SM.Read("y")
		if !ok || v != "2" {
			t.Fatalf("node %s y=%q/%v want 2", id, v, ok)
		}
		if _, ghost := nd.cfg.SM.Read("ghost"); ghost {
			t.Fatalf("node %s applied an uncommitted minority write!", id)
		}
		t.Logf("converged node=%s term=%d commit=%d x=%s y=%s ghost=absent",
			id, nd.Term(), nd.CommitIndex(), mustRead(nd, "x"), mustRead(nd, "y"))
	}
}

func mustRead(nd *Node, key string) string {
	v, _ := nd.cfg.SM.Read(key)
	return v
}
