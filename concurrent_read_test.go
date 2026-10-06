package raft

import (
	"errors"
	"testing"
)

// TestConcurrentReads 验证领导者同时服务多个并发读：
// 一次性发出多个读（不推进事件），随后统一推进——每个读都必须拿到
// 自己键的正确结果，不得互相覆盖、丢结果或永远卡住。
func TestConcurrentReads(t *testing.T) {
	env := NewSimEnv(3).WithLogger(testLogger{t})
	defer env.Close()

	leader := env.Leader(2000)
	if leader == "" {
		t.Fatal("no leader")
	}
	cl := NewClient("cr", env.Nodes, env.Sched)
	for _, kv := range [][2]string{{"k1", "v1"}, {"k2", "v2"}, {"k3", "v3"}} {
		f := cl.Set(nil, kv[0], kv[1])
		waitDone(t, env, f, 2000, "write "+kv[0])
		if err := f.Err(); err != nil {
			t.Fatalf("write %s: %v", kv[0], err)
		}
	}
	ld := env.Nodes[leader]
	t.Logf("PHASE-1 writes committed: leader=%s term=%d commit=%d", leader, ld.Term(), ld.CommitIndex())

	// 不推进任何事件，连续发出 10 个并发读（含一个不存在的键）。
	type want struct {
		key   string
		val   string
		found bool
	}
	wants := []want{
		{"k1", "v1", true}, {"k2", "v2", true}, {"k3", "v3", true},
		{"k1", "v1", true}, {"k2", "v2", true}, {"k3", "v3", true},
		{"k1", "v1", true}, {"k2", "v2", true}, {"k3", "v3", true},
		{"absent", "", false},
	}
	futs := make([]*ReadFuture, len(wants))
	for i, w := range wants {
		futs[i] = ld.Read(nil, w.key) // 仅登记，不推进事件：全部读同时在途
	}
	t.Logf("PHASE-2 issued %d concurrent reads on leader=%s", len(futs), leader)

	// 统一推进事件，逐个断言：每个读都完成且结果与自己的键对应。
	for i, f := range futs {
		waitDone(t, env, f, 2000, "concurrent read "+wants[i].key)
	}
	for i, f := range futs {
		v, ok, err := f.Get()
		if err != nil {
			t.Fatalf("read #%d key=%s: %v", i, wants[i].key, err)
		}
		if ok != wants[i].found || v != wants[i].val {
			t.Fatalf("read #%d key=%s = %q/%v, want %q/%v (results crossed between concurrent reads?)",
				i, wants[i].key, v, ok, wants[i].val, wants[i].found)
		}
	}
	t.Logf("PHASE-3 all %d concurrent reads completed with correct per-key results, term=%d commit=%d",
		len(futs), ld.Term(), ld.CommitIndex())
}

// TestConcurrentReadsNoQuorum 验证无法确认多数派时的并发读：
// 领导者在被隔离（少数派）状态下收到多个并发读，每个读都必须以
// 可区分的 no_quorum 失败，不得返回陈旧值，也不得永远卡住。
func TestConcurrentReadsNoQuorum(t *testing.T) {
	env := NewSimEnv(3).WithLogger(testLogger{t})
	defer env.Close()

	leader := env.Leader(2000)
	if leader == "" {
		t.Fatal("no leader")
	}
	cl := NewClient("crq", env.Nodes, env.Sched)
	wf := cl.Set(nil, "k1", "v1")
	waitDone(t, env, wf, 2000, "write k1=v1")
	if err := wf.Err(); err != nil {
		t.Fatalf("write k1: %v", err)
	}

	// 隔离领导者（少数派），随后并发发出多个读。
	env.Isolate(leader)
	ld := env.Nodes[leader]
	const n = 5
	futs := make([]*ReadFuture, n)
	for i := 0; i < n; i++ {
		futs[i] = ld.Read(nil, "k1")
	}
	t.Logf("PHASE-1 leader=%s isolated with %d concurrent reads in flight", leader, n)

	// 推进超过读超时：所有读都必须以 no_quorum 完成（无一遗漏、无一卡住）。
	env.RunFor(1000)
	for i, f := range futs {
		if !futureDone(f) {
			t.Fatalf("read #%d still stuck after read timeout", i)
		}
		v, ok, err := f.Get()
		if err == nil {
			t.Fatalf("read #%d returned value %q without quorum (must fail distinctly)", i, v)
		}
		var we *WriteError
		if !errors.As(err, &we) {
			t.Fatalf("read #%d error not *WriteError: %v", i, err)
		}
		if we.Reason != ReasonNoQuorum {
			t.Fatalf("read #%d reason=%s, want no_quorum", i, we.Reason)
		}
		if ok {
			t.Fatalf("read #%d reported found=true despite quorum failure (stale value leaked)", i)
		}
	}
	t.Logf("PHASE-2 all %d concurrent reads failed distinctly with no_quorum; no stale value served", n)
}
