package raft

// ErrorKind 用于让调用方区分不同的失败原因，禁止把所有失败归一化。
type ErrorKind int

const (
	// ErrNotLeaderKind 当前节点不是领导者，或已被更高任期的领导者取代。
	ErrNotLeaderKind ErrorKind = iota + 1
	// ErrNoQuorumKind 无法在选举超时内得到多数派确认。
	ErrNoQuorumKind
	// ErrStaleRequestKind 请求的任期或日志落后，被明确拒绝。
	ErrStaleRequestKind
	// ErrClosedKind 节点已关闭。
	ErrClosedKind
	// ErrUnsupportedVersionKind 持久化数据版本不受支持（例如来自更新版本）。
	ErrUnsupportedVersionKind
)

// Error 是复制组件对外返回的可区分错误。
type Error struct {
	Kind       ErrorKind
	LeaderHint int // 当已知当前领导者时给出提示，未知为 -1
	Reason     string
}

func (e *Error) Error() string {
	return e.Reason
}

func newError(kind ErrorKind, hint int, reason string) *Error {
	return &Error{Kind: kind, LeaderHint: hint, Reason: reason}
}
