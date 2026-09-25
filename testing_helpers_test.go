package raft

import "testing"

// testLogger 把节点协议日志桥接到 t.Logf，使单测输出中包含
// 任期(term)、提交序号(commit/index)与判定依据(basis)。
type testLogger struct{ t *testing.T }

func (l testLogger) Printf(format string, args ...any) { l.t.Logf(format, args...) }
