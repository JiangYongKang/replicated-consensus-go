package raft

import (
	"path/filepath"
	"strconv"
)

// SimEnv 把时钟、调度器、回环传输与一组副本装配为一个可复现的单机集群。
// 所有时间推进、消息投递、客户端重试都只经过同一个 Scheduler，
// 因而相同测试代码的任期推进、提交序号与最终结论完全确定。
type SimEnv struct {
	Clock     *SimClock
	Sched     *Scheduler
	Transport *LoopTransport
	Nodes     map[string]*Node
	IDs       []string

	storageDir string // 非空表示 FileWAL 集群
	storages   map[string]Storage
	useFile    bool

	// convergeWindowMs 是 Converge 在“外部静默”后额外等待的内部时间窗
	// （一个选举超时 + 传播余量），让恢复后的选举/心跳能被触发。
	convergeWindowMs int64
}

func envIDs(n int) []string {
	ids := make([]string, n)
	for i := 0; i < n; i++ {
		ids[i] = "n" + strconv.Itoa(i+1)
	}
	return ids
}

// NewSimEnv 创建 n 个内存存储副本的集群并全部 Start。
func NewSimEnv(n int) *SimEnv {
	env := &SimEnv{
		Clock:    NewSimClock(0),
		IDs:      envIDs(n),
		Nodes:    map[string]*Node{},
		storages: map[string]Storage{},
		useFile:  false,
	}
	env.Sched = NewScheduler(env.Clock)
	env.Transport = NewLoopTransport(env.Sched)
	env.Transport.SetDelay(2)
	env.convergeWindowMs = 350

	for _, id := range env.IDs {
		st := NewMemoryStorage()
		env.storages[id] = st
		env.Nodes[id] = env.buildNode(id, st, NewKVStore(), 0)
	}
	env.Transport.SetPartitions(FullyConnected(env.IDs))
	for _, id := range env.IDs {
		if err := env.Nodes[id].Start(); err != nil {
			panic(err)
		}
	}
	return env
}

// FileCluster 在 dir 下为每个副本创建独立 WAL 文件，供重启测试。
func FileCluster(dir string, ids []string) (*SimEnv, error) {
	env := &SimEnv{
		Clock:      NewSimClock(0),
		IDs:        append([]string(nil), ids...),
		Nodes:      map[string]*Node{},
		storages:   map[string]Storage{},
		useFile:    true,
		storageDir: dir,
	}
	env.Sched = NewScheduler(env.Clock)
	env.Transport = NewLoopTransport(env.Sched)
	env.Transport.SetDelay(2)
	env.convergeWindowMs = 350

	for _, id := range ids {
		st, err := OpenFileWAL(filepath.Join(dir, "wal-"+id))
		if err != nil {
			return nil, err
		}
		env.storages[id] = st
		env.Nodes[id] = env.buildNode(id, st, NewKVStore(), 0)
	}
	env.Transport.SetPartitions(FullyConnected(ids))
	for _, id := range ids {
		if err := env.Nodes[id].Start(); err != nil {
			return nil, err
		}
	}
	return env, nil
}

func (env *SimEnv) buildNode(id string, st Storage, sm StateMachine, seed int64) *Node {
	return NewNode(Config{
		ID:               id,
		Peers:            env.IDs,
		Storage:          st,
		SM:               sm,
		Transport:        env.Transport,
		Sched:            env.Sched,
		Clock:            env.Clock,
		Log:              discardLogger{},
		ElectionMin:      150,
		ElectionMax:      300,
		Heartbeat:        50,
		ProposalDeadline: 400,
		RandSeed:         seed,
	})
}

// WithLogger 为所有节点安装同一日志器（通常为 t.Logf 封装）。
func (env *SimEnv) WithLogger(l Logger) *SimEnv {
	for _, nd := range env.Nodes {
		nd.cfg.Log = l
	}
	return env
}

// RestartNode 关闭节点并基于同一持久存储重新启动，模拟进程重启。
// 状态机从空状态重放全部已提交日志（验证崩溃恢复与去重缓存重建）。
func (env *SimEnv) RestartNode(id string) error {
	old := env.Nodes[id]
	if err := old.Close(); err != nil {
		return err
	}
	st := env.storages[id]
	fresh := env.buildNode(id, st, NewKVStore(), 0)
	if old.cfg.Log != nil {
		fresh.cfg.Log = old.cfg.Log
	}
	env.Nodes[id] = fresh
	return fresh.Start()
}

// Isolate 将 id 与其他节点双向隔离。
func (env *SimEnv) Isolate(id string) {
	rules := Isolated(env.Transport.rules, env.IDs, id)
	env.Transport.SetPartitions(rules)
}

// Heal 恢复 id 到其他所有节点的双向连通；
// 与 id 无关的其他隔离状态保持不变。
func (env *SimEnv) Heal(id string) {
	rules := FullyConnected(env.IDs)
	// 保留当前其他隔离状态：仅把与 id 相关的边恢复。
	cur := env.Transport.rules
	for a := range rules {
		for b := range rules[a] {
			if a == id || b == id {
				continue
			}
			rules[a][b] = cur[a][b]
		}
	}
	env.Transport.SetPartitions(rules)
}

// HealAll 恢复全连通。
func (env *SimEnv) HealAll() {
	env.Transport.SetPartitions(FullyConnected(env.IDs))
}

// Leader 推进事件直至某个节点成为领导者（或超过 waitMs 虚拟时间）。
// 返回领导者 ID；无人胜选时返回空串。
func (env *SimEnv) Leader(waitMs int64) string {
	deadline := env.Clock.Now() + waitMs
	for env.Clock.Now() <= deadline {
		var leader string
		for id, nd := range env.Nodes {
			if nd.CurrentRole() == Leader {
				leader = id
				break
			}
		}
		if leader != "" {
			return leader
		}
		if !env.Sched.Tick() {
			break
		}
	}
	return ""
}

// RunFor 推进虚拟时间并触发其中全部事件。
func (env *SimEnv) RunFor(ms int64) int { return env.Sched.RunFor(ms) }

// Settle 处理全部待投递的外部消息/重试，直到外部事件清空（协议相对静默）。
func (env *SimEnv) Settle() int { return env.Sched.Settle(100000) }

// Converge 反复“处理外部消息 + 给一个选举周期的内部时间窗”，
// 直到虚拟时间窗内不再出现外部活动或达到总上限。用于分区恢复后等待
// 多轮选举/追赶完成；永久分区场景受 totalMs 上界保护，必然终止。
func (env *SimEnv) Converge(totalMs int64) int {
	end := env.Clock.Now() + totalMs
	n := 0
	for env.Clock.Now() < end {
		env.Sched.Settle(100000)
		if env.Sched.PendingExternal() > 0 {
			continue
		}
		// 外部已静默；给一个选举超时的内部窗口，看是否产生新的外部消息。
		env.Sched.RunFor(env.convergeWindowMs)
		n++
		if env.Sched.PendingExternal() == 0 {
			// 一整个窗口没有任何新外部活动：已真正收敛。
			break
		}
	}
	return n
}

// Stabilize 保留旧名：等价于一次完整的有界收敛。
func (env *SimEnv) Stabilize() int {
	env.Sched.Settle(100000)
	return env.Converge(2000)
}

func (env *SimEnv) Close() error {
	var first error
	for _, id := range env.IDs {
		if err := env.Nodes[id].Close(); err != nil && first == nil {
			first = err
		}
		// 存储由环境统一关闭（节点 Close 不接管其生命周期）。
		if st := env.storages[id]; st != nil {
			if err := st.Close(); err != nil && first == nil {
				first = err
			}
		}
	}
	return first
}
