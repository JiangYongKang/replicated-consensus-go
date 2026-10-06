package raft

import (
	"errors"
	"strconv"
	"testing"
)

// TestConcurrentReads 验证领导者同一时刻服务多个读请求：
// 每个读都拿到自己键的正确结果，不互相覆盖、不丢结果、不卡死。
func TestConcurrentReads(t *testing.T) {
	env := NewSimEnv(3).WithLogger(testLogger{t})
	defer env.Close()

	leader := env.Leader(2000)
	if leader == "" {
		t.Fatal("no leader")
	}
	cl := NewClient("cr", env.Nodes, env.Sched)

	const keyCount = 5
	for i := 0; i < keyCount; i++ {
		f := cl.Set(nil, "key-"+strconv.Itoa(i), "val-"+strconv.Itoa(i))
		waitDone(t, env, f, 2000, "seed write")
		if err := f.Err(); err != nil {
			t.Fatalf("seed write %d: %v", i, err)
		}
	}

	// 同一时刻发起一批读：每个键两个并发读 + 一个不存在键的读，
	// 全部在驱动任何事件之前注册，模拟真正的同时到达。
	ld := env.Nodes[leader]
	type want struct {
		key   string
		val   string
		found bool
	}
	var futures []*ReadFuture
	var wants []want
	for i := 0; i < keyCount; i++ {
		for r := 0; r < 2; r++ {
			futures = append(futures, ld.Read(nil, "key-"+strconv.Itoa(i)))
			wants = append(wants, want{"key-" + strconv.Itoa(i), "val-" + strconv.Itoa(i), true})
		}
	}
	futures = append(futures, ld.Read(nil, "no-such-key"))
	wants = append(wants, want{"no-such-key", "", false})

	for i, f := range futures {
		waitDone(t, env, f, 3000, "concurrent read "+wants[i].key)
	}
	for i, f := range futures {
		v, ok, err := f.Get()
		if err != nil {
			t.Fatalf("read %d (%s): %v", i, wants[i].key, err)
		}
		if ok != wants[i].found || v != wants[i].val {
			t.Fatalf("read %d (%s) = %q/%v, want %q/%v (results crossed?)",
				i, wants[i].key, v, ok, wants[i].val, wants[i].found)
		}
	}
	t.Logf("concurrent reads: %d reads all returned their own correct results, leader term=%d commit=%d",
		len(futures), ld.Term(), ld.CommitIndex())

	// 并发读与客户端自动改道混用：客户端并发 Get 也必须各自正确。
	var cfutures []*ReadFuture
	for i := 0; i < keyCount; i++ {
		cfutures = append(cfutures, cl.Get(nil, "key-"+strconv.Itoa(i)))
	}
	for i, f := range cfutures {
		waitDone(t, env, f, 3000, "client concurrent read")
		v, ok, err := f.Get()
		if err != nil || !ok || v != "val-"+strconv.Itoa(i) {
			t.Fatalf("client read key-%d = %q/%v err=%v", i, v, ok, err)
		}
	}
	t.Logf("client-side concurrent reads: %d reads all correct", len(cfutures))
}

// TestConcurrentReadsFailDistinguishably 验证多数派不可用时：
// 一批并发读全部以可区分的 no_quorum 失败，绝不返回陈旧值，也绝不卡死。
func TestConcurrentReadsFailDistinguishably(t *testing.T) {
	env := NewSimEnv(3).WithLogger(testLogger{t})
	defer env.Close()

	leader := env.Leader(2000)
	if leader == "" {
		t.Fatal("no leader")
	}
	cl := NewClient("crf", env.Nodes, env.Sched)
	f := cl.Set(nil, "k", "v")
	waitDone(t, env, f, 2000, "seed write")
	if err := f.Err(); err != nil {
		t.Fatalf("seed write: %v", err)
	}

	// 隔离领导者：它仍自认为是领导，但心跳凑不齐多数。
	env.Isolate(leader)
	ld := env.Nodes[leader]

	const readCount = 4
	var futures []*ReadFuture
	for i := 0; i < readCount; i++ {
		futures = append(futures, ld.Read(nil, "k"))
	}
	for i, f2 := range futures {
		waitDone(t, env, f2, 2000, "isolated concurrent read must fail")
		v, ok, err := f2.Get()
		if err == nil {
			t.Fatalf("read %d returned %q/%v under no quorum; must fail distinguishably", i, v, ok)
		}
		var we *WriteError
		if !errors.As(err, &we) || we.Reason != ReasonNoQuorum {
			t.Fatalf("read %d reason=%v, want no_quorum", i, err)
		}
		if ok || v != "" {
			t.Fatalf("read %d leaked a stale value %q/%v alongside failure", i, v, ok)
		}
	}
	t.Logf("concurrent reads under no quorum: %d reads all failed with no_quorum, none stuck, no stale values", readCount)
}
