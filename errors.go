package raft

import "errors"

// FailReason 以可区分的方式标识写入/读取失败的原因。
type FailReason string

const (
	ReasonNotLeader    FailReason = "not_leader"    // 当前节点不是领导者
	ReasonNoQuorum     FailReason = "no_quorum"     // 多数派不可用
	ReasonStaleTerm    FailReason = "stale_term"    // 请求方任期落后
	ReasonStaleRequest FailReason = "stale_request" // 客户端请求序号过旧或与在途请求冲突
	ReasonTimeout      FailReason = "timeout"       // 等待提交超时
	ReasonClosed       FailReason = "closed"        // 节点已关闭
)

// 哨兵错误，便于 errors.Is 判定。
var (
	ErrNotLeader    = errors.New("raft: node is not the leader")
	ErrNoQuorum     = errors.New("raft: no quorum available")
	ErrStaleTerm    = errors.New("raft: request from an older term")
	ErrStaleRequest = errors.New("raft: stale or conflicting client request")
	ErrTimeout      = errors.New("raft: request timed out")
	ErrClosed       = errors.New("raft: node closed")

	ErrUnsupportedVersion = errors.New("raft: unsupported wal format version")
	ErrNotWAL             = errors.New("raft: file is not a replicated log")
	ErrCorruptWAL         = errors.New("raft: corrupt wal record")
)

var reasonSentinel = map[FailReason]error{
	ReasonNotLeader:    ErrNotLeader,
	ReasonNoQuorum:     ErrNoQuorum,
	ReasonStaleTerm:    ErrStaleTerm,
	ReasonStaleRequest: ErrStaleRequest,
	ReasonTimeout:      ErrTimeout,
	ReasonClosed:       ErrClosed,
}

// WriteError 携带可区分的失败原因以及排查所需的上下文。
type WriteError struct {
	Reason        FailReason
	LeaderHint    string // 若已知，提示当前可能的领导者
	Term          uint64 // 拒绝方当前任期
	RequestedTerm uint64 // 请求方携带的任期（如适用）
	Detail        string
}

func (e *WriteError) Error() string {
	s := "raft: " + string(e.Reason)
	if e.Detail != "" {
		s += ": " + e.Detail
	}
	if e.LeaderHint != "" {
		s += " (leader hint: " + e.LeaderHint + ")"
	}
	return s
}

// Is 使 errors.Is(err, ErrNotLeader) 等判定对 WriteError 生效。
func (e *WriteError) Is(target error) bool {
	if s, ok := reasonSentinel[e.Reason]; ok && target == s {
		return true
	}
	return false
}
