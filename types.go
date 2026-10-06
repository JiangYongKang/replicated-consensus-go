package raft

// Role 是副本在任期中的角色。
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// CommandKind 标识状态机命令类型。
type CommandKind string

const (
	CmdSet  CommandKind = "set"
	CmdInc  CommandKind = "inc"
	CmdGet  CommandKind = "get"  // get 仅用于读取路径，正常不会进入日志
	CmdNoop CommandKind = "noop" // 领导者上任时追加的本任期空条目，用于安全推进 commitIndex
)

// Command 是复制到各副本的确定性写命令。
type Command struct {
	Kind CommandKind `json:"kind"`
	Key  string      `json:"key"`
	Val  string      `json:"val,omitempty"`
	// ClientID/Seq 用于端到端幂等去重。
	ClientID string `json:"client_id,omitempty"`
	Seq      uint64 `json:"seq,omitempty"`
}

// Entry 是一条 Raft 日志项。
type Entry struct {
	Term  uint64  `json:"term"`
	Index uint64  `json:"index"`
	Cmd   Command `json:"cmd"`
}

// MessageType 枚举副本间 RPC。
type MessageType int

const (
	MsgVote MessageType = iota + 1
	MsgVoteResp
	MsgAppend
	MsgAppendResp
)

func (t MessageType) String() string {
	switch t {
	case MsgVote:
		return "Vote"
	case MsgVoteResp:
		return "VoteResp"
	case MsgAppend:
		return "Append"
	case MsgAppendResp:
		return "AppendResp"
	default:
		return "Unknown"
	}
}

// Message 是副本之间交换的消息。零值字段表示不适用。
type Message struct {
	Type MessageType

	From string
	To   string

	Term        uint64 // 发送方任期
	LastLogTerm uint64 // Vote: 候选者最后一条日志的任期
	LastLogIdx  uint64 // Vote: 候选者最后一条日志的序号

	PrevLogIdx   uint64  // Append: 紧邻新条目之前的日志序号
	PrevLogTerm  uint64  // Append: 该位置日志的任期
	Entries      []Entry // Append: 待追加条目（心跳为空）
	LeaderCommit uint64  // Append: 领导者已提交序号
	LeaderHint   string  // Append: 领导者 ID，便于客户端重定向

	Granted       bool   // 响应：是否同意
	Success       bool   // 响应：追加是否成功
	Match         uint64 // AppendResp: 跟随者最后匹配序号
	RejectHintIdx uint64 // AppendResp: 冲突提示序号
}
