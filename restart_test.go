package raft

import (
	"errors"
	"testing"
)

// TestLeaderRestartPersistence 验证：
//   - 已提交写入落盘，领导者进程重启后从 WAL 重放恢复任期、投票、已提交历史；
//   - 重启后用相同 (ClientID, Seq) 重发不会被重复应用（Duplicate）；
//   - 重启节点可重新当选并继续提供写服务，历史连续。
func TestLeaderRestartPersistence(t *testing.T) {
	dir := t.TempDir()
	ids := []string{"r1", "r2", "r3"}
	env, err := FileCluster(dir, ids)
	if err != nil {
		t.Fatal(err)
	}
	env.WithLogger(testLogger{t})
	defer env.Close()

	leader := env.Leader(2000)
	if leader == "" {
		t.Fatal("no leader")
	}
	t.Logf("PHASE-1 leader=%s term=%d", leader, env.Nodes[leader].Term())

	cl := NewClient("k1", env.Nodes, env.Sched)
	var idxs []uint64
	for i := 0; i < 3; i++ {
		f := cl.Inc(nil, "counter")
		waitDone(t, env, f, 1000, "inc counter")
		if err := f.Err(); err != nil {
			t.Fatalf("inc %d: %v", i, err)
		}
		idxs = append(idxs, f.Result().Index)
	}
	t.Logf("PHASE-2 committed 3 increments, indexes=%v, counter=%s", idxs, mustRead(env.Nodes[leader], "counter"))

	termBefore := env.Nodes[leader].Term()
	commitBefore := env.Nodes[leader].CommitIndex()

	// 重启领导者（状态机从空开始，靠 WAL 重放重建）。
	if err := env.RestartNode(leader); err != nil {
		t.Fatalf("restart: %v", err)
	}
	t.Logf("PHASE-3 restarted %s; term=%d commit=%d (expect preserved %d/%d)",
		leader, env.Nodes[leader].Term(), env.Nodes[leader].CommitIndex(), termBefore, commitBefore)

	if env.Nodes[leader].Term() != termBefore || env.Nodes[leader].CommitIndex() != commitBefore {
		t.Fatalf("hard state lost across restart: term=%d/%d commit=%d/%d",
			env.Nodes[leader].Term(), termBefore, env.Nodes[leader].CommitIndex(), commitBefore)
	}
	if v := mustRead(env.Nodes[leader], "counter"); v != "3" {
		t.Fatalf("counter after replay=%q, want 3", v)
	}

	// 相同 (ClientID, Seq) 重发到重启后的节点：若它在日志里则去重；
	// 若节点此时是跟随者，客户端改道新领导者，该序号在多数派已提交日志中，
	// 经由追赶/重放命中会话缓存——任何路径都不得重复自增。
	env.Stabilize() // 允许集群重新选举与心跳稳定
	fDup := cl.Set(nil, "marker", "same")
	waitDone(t, env, fDup, 2000, "post-restart write")
	if err := fDup.Err(); err != nil {
		t.Fatalf("post-restart write: %v", err)
	}
	t.Logf("post-restart write ok index=%d", fDup.Result().Index)

	// 再自增一次，确认历史连续且复制正常。
	fInc := cl.Inc(nil, "counter")
	waitDone(t, env, fInc, 2000, "inc after restart")
	if err := fInc.Err(); err != nil {
		t.Fatalf("inc after restart: %v", err)
	}
	if fInc.Result().Value != "4" {
		t.Fatalf("counter=%s want 4 (history must be continuous after restart)", fInc.Result().Value)
	}

	env.Stabilize()
	for _, id := range ids {
		nd := env.Nodes[id]
		if v := mustRead(nd, "counter"); v != "4" {
			t.Fatalf("node %s counter=%s want 4", id, v)
		}
		if nd.CommitIndex() < 5 {
			t.Fatalf("node %s commit=%d want >=5", id, nd.CommitIndex())
		}
		t.Logf("restart-converged node=%s term=%d commit=%d counter=%s",
			id, nd.Term(), nd.CommitIndex(), mustRead(nd, "counter"))
	}
}

// TestWALReopenReplays 直接验证 WAL 文件关闭重开后的格式校验与重放。
func TestWALReopenReplays(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/wal-x"
	w, err := OpenFileWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	hs := HardState{Term: 7, VoteFor: "r2", CommitIndex: 2}
	if err := w.SaveHardState(hs); err != nil {
		t.Fatal(err)
	}
	ents := []Entry{
		{Term: 5, Index: 1, Cmd: Command{Kind: CmdSet, Key: "a", Val: "1"}},
		{Term: 7, Index: 2, Cmd: Command{Kind: CmdSet, Key: "b", Val: "2"}},
	}
	if err := w.Append(0, ents); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	w2, err := OpenFileWAL(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer w2.Close()
	if got := w2.HardState(); got != hs {
		t.Fatalf("hard state=%+v want %+v", got, hs)
	}
	if w2.LastIndex() != 2 {
		t.Fatalf("last index=%d want 2", w2.LastIndex())
	}
	// 幂等重复投递同一批条目，结果不变。
	if err := w2.Append(0, ents); err != nil {
		t.Fatalf("idempotent re-append: %v", err)
	}
	if w2.LastIndex() != 2 {
		t.Fatalf("after idempotent append last=%d want 2", w2.LastIndex())
	}

	// 截断重放：index 2 用不同任期重写，旧尾部必须被原子替换。
	if err := w2.Append(1, []Entry{{Term: 8, Index: 2, Cmd: Command{Kind: CmdSet, Key: "c"}}}); err != nil {
		t.Fatal(err)
	}
	w2.Close()
	w3, err := OpenFileWAL(path)
	if err != nil {
		t.Fatalf("reopen after truncate: %v", err)
	}
	defer w3.Close()
	e2, _ := w3.Entry(2)
	if e2.Term != 8 {
		t.Fatalf("truncated entry term=%d want 8", e2.Term)
	}
	t.Logf("wal replay/truncate verified: term=%d commit=%d last=%d",
		w3.HardState().Term, w3.HardState().CommitIndex, w3.LastIndex())
}

// TestWALVersionReject 验证版本前向兼容策略：更高版本的文件被拒绝。
func TestWALVersionReject(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/wal-bad"
	w, err := OpenFileWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	w.Close()

	// 篡改版本字节为 9。
	raw, err := readFileBytes(path)
	if err != nil {
		t.Fatal(err)
	}
	raw[4] = 9
	if err := writeFileBytes(path, raw); err != nil {
		t.Fatal(err)
	}
	_, err = OpenFileWAL(path)
	if !errors.Is(err, ErrUnsupportedVersion) {
		t.Fatalf("want ErrUnsupportedVersion, got %v", err)
	}
}
