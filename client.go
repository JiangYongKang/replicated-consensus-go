package raft

import "sync/atomic"

// Client 是绑定在 Cluster 上的确定性客户端：
// 自动在副本间寻找领导者，超时后重发“同一个请求”（相同幂等键），
// 因此网络重试不会造成重复应用。
type Client struct {
	cluster *Cluster
	id      uint64
	seq     atomic.Uint64
	leader  int
}

var clientIDCounter atomic.Uint64

func NewClient(c *Cluster) *Client {
	return &Client{
		cluster: c,
		id:      clientIDCounter.Add(1),
		leader:  -1,
	}
}

func (cl *Client) Put(key, value string) (Result, error) {
	cmd := Command{Op: OpPut, Key: key, Value: value, ClientID: cl.id, Seq: cl.seq.Add(1)}
	return cl.write(cmd, len(cl.cluster.ids)*2+2)
}

func (cl *Client) Delete(key string) (Result, error) {
	cmd := Command{Op: OpDelete, Key: key, ClientID: cl.id, Seq: cl.seq.Add(1)}
	return cl.write(cmd, len(cl.cluster.ids)*2+2)
}

// Get 执行线性一致读；仅在领导者上执行，失败后按提示换节点重试。
func (cl *Client) Get(key string) (Result, error) {
	budget := len(cl.cluster.ids) * 4
	for attempt := 0; attempt < budget; attempt++ {
		if cl.leader < 0 {
			cl.leader = cl.cluster.LeaderID()
		}
		if cl.leader < 0 {
			cl.cluster.Advance(cl.cluster.electionBase)
			continue
		}
		r, err := cl.cluster.node(cl.leader).Read(key)
		if err == nil {
			return r, nil
		}
		re, ok := err.(*Error)
		if !ok {
			return r, err
		}
		switch re.Kind {
		case ErrNotLeaderKind:
			cl.leader = re.LeaderHint
			if cl.leader < 0 {
				cl.leader = cl.cluster.LeaderID()
			}
			cl.cluster.Advance(1)
		case ErrNoQuorumKind:
			cl.cluster.Advance(cl.cluster.heartbeat)
		default:
			return r, re
		}
	}
	return Result{}, newError(ErrNoQuorumKind, -1, "raft: client retry budget exhausted")
}

func (cl *Client) write(cmd Command, budget int) (Result, error) {
	lastErr := newError(ErrNoQuorumKind, -1, "raft: no attempts made")
	for attempt := 0; attempt < budget; attempt++ {
		if cl.leader < 0 {
			cl.leader = cl.cluster.LeaderID()
		}
		if cl.leader < 0 {
			cl.cluster.Advance(cl.cluster.electionBase)
			continue
		}
		r, err := cl.cluster.node(cl.leader).Propose(cmd)
		if err == nil {
			return r, nil
		}
		re, ok := err.(*Error)
		if !ok {
			return r, err
		}
		lastErr = re
		switch re.Kind {
		case ErrNotLeaderKind:
			// 换由响应方提示的领导者；没有提示就重新发现。
			cl.leader = re.LeaderHint
			if cl.leader < 0 {
				cl.leader = cl.cluster.LeaderID()
			}
			cl.cluster.Advance(1)
		case ErrNoQuorumKind:
			// 仍可能是领导者（分区）：继续用同一个幂等键重发，
			// 分区恢复后第一次提交即生效，不会重复应用。
			cl.cluster.Advance(cl.cluster.heartbeat)
		case ErrStaleRequestKind, ErrClosedKind:
			return r, re
		}
	}
	return Result{}, lastErr
}
