package raft

import (
	"sort"
	"testing"
)

// cutDirected 在当前分区规则基础上切断若干有向边（from -> to）。
// 注意：只影响切断之后 Send 的消息；已排入调度器的在途消息仍会送达。
func cutDirected(env *SimEnv, cuts ...[2]string) {
	rules := cloneRules(env.Transport.rules)
	for _, c := range cuts {
		if row := rules[c[0]]; row != nil {
			row[c[1]] = false
		}
	}
	env.Transport.SetPartitions(rules)
}

// stepExternalUntil 逐个推进外部事件，直到 cond 成立或步数耗尽。
func stepExternalUntil(env *SimEnv, cond func() bool, maxSteps int) bool {
	for i := 0; i < maxSteps; i++ {
		if cond() {
			return true
		}
		if !env.Sched.StepExternal() {
			return cond()
		}
	}
	return cond()
}

func lastIndexOf(env *SimEnv, id string) uint64 {
	return env.Nodes[id].cfg.Storage.LastIndex()
}

func futureDone(f doneFuture) bool {
	select {
	case <-f.Done():
		return true
	default:
		return false
	}
}

// TestReadYourWritesAfterLeaderChange 验证「领导更替后读己之写」。
//
// 场景构造（逐事件推进，完全确定，不依赖时序巧合）：
//  1. 写 k=v1 复制到 A、B 后、旧领导 L 收到多数确认之前，切断 L<->B：
//     L 仍能凭 L+A 多数提交并确认写成功，但提交通知在发送时即被丢弃，
//     B 日志里有该条目、commitIndex 却停留在旧水位；
//  2. 再切断 A->L（保留 L->A）：L 继续向 A 发心跳，A 永不重选；
//     B 收不到心跳而发起选举，A 投票给 B —— B 以「提交点落后」的
//     状态成为任期更高的新领导者；
//  3. 客户端自动改道后读 k，必须读到 v1，不得返回空值/旧值。
//
// 若领导者上任不追加本任期 no-op 屏障、读不等待 commitIndex 越过
// 任期起点，本场景下 B 会用落后的本地状态服务读，测试必然失败。
func TestReadYourWritesAfterLeaderChange(t *testing.T) {
	env := NewSimEnv(3).WithLogger(testLogger{t})
	defer env.Close()

	leader := env.Leader(2000)
	if leader == "" {
		t.Fatal("no leader")
	}
	env.Settle()
	oldTerm := env.Nodes[leader].Term()
	t.Logf("PHASE-1 leader=%s term=%d commit=%d", leader, oldTerm, env.Nodes[leader].CommitIndex())

	var followers []string
	for _, id := range env.IDs {
		if id != leader {
			followers = append(followers, id)
		}
	}
	sort.Strings(followers)
	a, b := followers[0], followers[1]

	// 直接向领导者提案（不经客户端自动改道），以便逐事件控制复制进度。
	wf := env.Nodes[leader].Propose(nil, Command{Kind: CmdSet, Key: "k", Val: "v1", ClientID: "ryw", Seq: 1})
	entryIdx := lastIndexOf(env, leader)

	// 推进到 B 已追加该条目（同一时刻 A 也已追加），但 L 尚未收到多数确认。
	if !stepExternalUntil(env, func() bool { return lastIndexOf(env, b) >= entryIdx }, 50) {
		t.Fatalf("%s never received the entry", b)
	}
	// 此刻切断 L<->B：随后的提交广播在发送时即被丢弃，B 永远收不到它。
	cutDirected(env, [2]string{leader, b}, [2]string{b, leader})
	t.Logf("PHASE-2 entry replicated to %s; links %s<->%s cut before commit broadcast", b, leader, b)

	// 推进到写确认成功（L+A 多数提交）。
	if !stepExternalUntil(env, func() bool { return futureDone(wf) }, 50) {
		t.Fatal("write never committed on L+A majority")
	}
	if err := wf.Err(); err != nil {
		t.Fatalf("write k=v1: %v", err)
	}
	lc, bc := env.Nodes[leader].CommitIndex(), env.Nodes[b].CommitIndex()
	if bc >= lc {
		t.Fatalf("precondition broken: %s commit=%d should lag leader commit=%d", b, bc, lc)
	}
	t.Logf("PHASE-3 write committed: leader commit=%d; %s still at commit=%d (notification dropped)", lc, b, bc)

	// 再切断 A->L：旧领导收不到任何更高任期的回包，永远以为自己在任；
	// B 选举超时后发起选举并获胜（A 投票），成为提交点落后的新领导。
	cutDirected(env, [2]string{a, leader})
	env.Converge(4000)
	newLeader := env.Nodes[b]
	if newLeader.CurrentRole() != Leader {
		t.Fatalf("%s should have won the election, role=%s", b, newLeader.CurrentRole())
	}
	if newLeader.Term() <= oldTerm {
		t.Fatalf("new leader term=%d must exceed old term=%d", newLeader.Term(), oldTerm)
	}
	t.Logf("PHASE-4 stale-commit follower %s elected: term=%d commit=%d", b, newLeader.Term(), newLeader.CommitIndex())

	// 客户端自动改道后读 k：旧领导的读只能以 no_quorum 失败（收不到多数确认），
	// 新领导必须返回已确认的写入。
	cl := NewClient("ryw", env.Nodes, env.Sched)
	rf := cl.Get(nil, "k")
	waitDone(t, env, rf, 6000, "read-your-writes after leader change")
	v, ok, err := rf.Get()
	if err != nil {
		t.Fatalf("read after leader change: %v", err)
	}
	if !ok || v != "v1" {
		t.Fatalf("read-your-writes violated: k=%q found=%v, want v1", v, ok)
	}
	t.Logf("read-your-writes honored: k=%s served at term=%d commit=%d",
		v, newLeader.Term(), newLeader.CommitIndex())
}

// TestReadYourWritesAfterLeaderRestart 验证「旧领导重启」后的读己之写：
// 写确认成功 -> 旧领导进程重启（丢失全部易失状态）-> 其余副本选出新领导 ->
// 客户端改道读同一键，必须读到已确认的写入。
func TestReadYourWritesAfterLeaderRestart(t *testing.T) {
	env := NewSimEnv(3).WithLogger(testLogger{t})
	defer env.Close()

	leader := env.Leader(2000)
	if leader == "" {
		t.Fatal("no leader")
	}
	env.Settle()

	cl := NewClient("ryw-restart", env.Nodes, env.Sched)
	wf := cl.Set(nil, "k", "v1")
	waitDone(t, env, wf, 2000, "write k=v1")
	if err := wf.Err(); err != nil {
		t.Fatalf("write k=v1: %v", err)
	}
	t.Logf("PHASE-1 write committed on leader=%s term=%d commit=%d",
		leader, env.Nodes[leader].Term(), env.Nodes[leader].CommitIndex())

	// 旧领导重启：易失状态（matchIndex/在途提案等）全部丢失，仅持久状态保留。
	if err := env.RestartNode(leader); err != nil {
		t.Fatalf("restart: %v", err)
	}
	env.Isolate(leader) // 重启后暂时隔离，迫使其余副本选出新领导
	env.Converge(4000)

	newLeader := ""
	for _, id := range env.IDs {
		if id != leader && env.Nodes[id].CurrentRole() == Leader {
			newLeader = id
		}
	}
	if newLeader == "" {
		t.Fatal("no new leader after old leader restart+isolation")
	}
	t.Logf("PHASE-2 old leader %s restarted; new leader=%s term=%d commit=%d",
		leader, newLeader, env.Nodes[newLeader].Term(), env.Nodes[newLeader].CommitIndex())

	rf := cl.Get(nil, "k")
	waitDone(t, env, rf, 6000, "read-your-writes after leader restart")
	v, ok, err := rf.Get()
	if err != nil {
		t.Fatalf("read after leader restart: %v", err)
	}
	if !ok || v != "v1" {
		t.Fatalf("read-your-writes violated after restart: k=%q found=%v, want v1", v, ok)
	}
	t.Logf("read-your-writes honored after restart: k=%s served by %s term=%d",
		v, newLeader, env.Nodes[newLeader].Term())
}
