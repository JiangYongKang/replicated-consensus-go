package raft

import (
	"strings"
	"testing"
)

// TestLinearizableRead 验证读路径：只有领导者能服务读（ReadIndex 心跳确认），
// 读到的值不早于已确认写入。
func TestLinearizableRead(t *testing.T) {
	env := NewSimEnv(3).WithLogger(testLogger{t})
	defer env.Close()
	leader := env.Leader(2000)
	if leader == "" {
		t.Fatal("no leader")
	}
	cl := NewClient("reader", env.Nodes, env.Sched)

	wf := cl.Set(nil, "color", "blue")
	waitDone(t, env, wf, 1000, "set color=blue")

	rf := cl.Get(nil, "color")
	waitDone(t, env, rf, 1000, "linearizable read color")
	v, ok, err := rf.Get()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !ok || v != "blue" {
		t.Fatalf("read color=%q ok=%v want blue", v, ok)
	}
	t.Logf("linearizable read: color=%s term=%d commit=%d", v, env.Nodes[leader].Term(), env.Nodes[leader].CommitIndex())

	// 读一个不存在的键：found=false 但不报错。
	rf2 := cl.Get(nil, "missing-key")
	waitDone(t, env, rf2, 1000, "read missing")
	_, found, err := rf2.Get()
	if err != nil || found {
		t.Fatalf("missing key: found=%v err=%v", found, err)
	}
}

// captureLogger 收集日志中形如 "term=N" 与 "index=N" 的关键判定序列。
type captureLogger struct {
	t  *testing.T
	sb strings.Builder
}

func (l *captureLogger) Printf(format string, args ...any) {
	l.t.Logf(format, args...)
	l.sb.WriteString(format)
	for _, a := range args {
		l.sb.WriteString(" " + toStr(a))
	}
	l.sb.WriteString("\n")
}

func toStr(a any) string {
	switch v := a.(type) {
	case uint64:
		return u64(v)
	case int:
		return u64(uint64(v))
	case bool:
		if v {
			return "true"
		}
		return "false"
	default:
		return "x"
	}
}

func runDeterminismScenario(t *testing.T) string {
	env := NewSimEnv(3)
	defer env.Close()
	cap := &captureLogger{t: t}
	env.WithLogger(cap)

	leader := env.Leader(2000)
	cl := NewClient("det", env.Nodes, env.Sched)
	for _, val := range []string{"a", "b", "c"} {
		f := cl.Set(nil, "k", val)
		waitDone(t, env, f, 1000, "set "+val)
	}
	// 一次领导者更替：隔离当前领导者再恢复。
	env.Isolate(leader)
	env.Converge(2000)
	env.HealAll()
	env.Converge(3000)

	var sb strings.Builder
	for _, id := range env.IDs {
		nd := env.Nodes[id]
		sb.WriteString(id + ":" + u64(nd.Term()) + ":" + u64(nd.CommitIndex()) + ":" + mustRead(nd, "k") + ";")
	}
	sb.WriteString("|")
	// 只保留含 become-leader/commit 的协议关键行，比较其任期与序号序列。
	for _, line := range strings.Split(cap.sb.String(), "\n") {
		if strings.Contains(line, "become-leader") || strings.Contains(line, "commit: term") {
			sb.WriteString(line)
			sb.WriteString("\n")
		}
	}
	return sb.String()
}

// TestDeterministicReproduction 同一脚本跑两次，领导者/任期/提交序列必须逐字节一致。
func TestDeterministicReproduction(t *testing.T) {
	first := runDeterminismScenario(t)
	second := runDeterminismScenario(t)
	if first != second {
		t.Fatalf("non-deterministic reproduction\n--- first ---\n%s\n--- second ---\n%s", first, second)
	}
	t.Logf("deterministic trace:\n%s", first)
}
