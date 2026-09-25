# 多副本强一致状态机复制 — 设计与验证文档

本模块是一套可在**单进程内**运行的 Raft 风格复制组件：多个副本通过可控的回环
网络交换消息，由确定性的离散时钟（tick）驱动，不依赖墙钟与真实网络，因此分区、
重启与并发重试场景下的结论**确定且可复现**。

- 语言：Go（module: `github.com/highcumontoa/replicated-consensus-go`）
- 入口：
  - `cluster.go`：`Cluster` 装配副本、驱动时钟、分区/恢复、重启
  - `node.go` / `replicate.go`：单个副本的 Raft 状态机
  - `network.go`：可控回环网络（分区、消息投递）
  - `client.go`：自动发现领导者、超时同键重发的确定性客户端
  - `machine.go`：确定性 KV 状态机（内置幂等响应缓存）
  - `storage.go` / `storage_fmt.go` / `entry.go`：持久化格式
  - `errors.go`：可区分的错误类型

---

## 1. 复制协议

### 1.1 角色、任期与选举

- 每个副本处于 `Follower`、`Candidate`、`Leader` 三种状态之一。
- 副本在自己的选举超时内没有收到合法领导者的心跳，就把 `currentTerm += 1`，
  转为候选人，给自己投票并向其他副本发送 `RequestVote`。
- **同一任期最多一个领导者**：一个副本同一任期最多投出一张票（`votedFor`
  随 `currentTerm` 持久化），候选人必须得到多数派选票才能成为领导者。
- 选举超时为 `ElectionBase + nodeID * ElectionSpread`，是**固定值而非随机值**：
  错开的刻度避免活锁，同时保证同一初始条件下选举结果每次相同（可复现）。
- 任何消息携带的任期高于本地任期时，接收方立即退回追随者并持久化新任期；
  旧任期节点发来的投票/复制请求被**明确拒绝**，不会静默接管。
- 日志落后的候选人拿不到票：`RequestVote` 仅在候选人的
  `(lastLogTerm, lastLogIndex)` 不比本地旧时才可能获得投票。

### 1.2 日志复制与提交

- 领导者把写请求作为 `LogEntry{Index, Term, Command}` 追加到本地日志并立即
  通过 `AppendEntries` 广播；追随者在 `(prevLogIndex, prevLogTerm)` 匹配时追加，
  冲突时截断本地冲突后缀。
- 领导者只在**当前任期**的条目上直接推进 `commitIndex`（Raft 图 8 规则），
  某条目得到多数派 `matchIndex` 覆盖后提交；之前任期的条目随之自动提交。
- **只有 `commitIndex` 之前的条目会被应用到状态机**。领导者更替时未提交的旧
  条目可能被新领导者的条目截断覆盖，因此绝不会“残留为已提交结果”。
- 领导者就职时立即复制一条本任期 **no-op** 空条目，使得上一任期已复制到多数派
  但尚未提交的日志在 no-op 提交时一并生效。
- 追随者根据 `leaderCommit` 推进本地 `commitIndex` 并重放应用；重启后内存状态机
  为空，由 `AppendEntries` 把已提交日志重放补齐。
- **追赶幂等**：相同 `(index, term)` 的重复投递直接跳过；`AppendEntries`
  响应失败时领导者把 `nextIndex` 回退一位并重发，可在一个 tick 内连续补齐，
  重复心跳不会改变已收敛的提交序号（见 `TestLeaderRestart` 中 30 tick 稳定性断言）。

### 1.3 客户端语义

- 写命令携带 `(ClientID, Seq)` 幂等键。状态机为每个键缓存首次应用结果：
  - 已提交的同键请求重放，直接返回首次结果，**不重复应用**；
  - 同一领导者上同一条未提交请求重试时复用既有日志条目，不重复追加；
  - 跨领导者重试（旧领导者分区期间超时）时，无论旧日志被复制还是被截断重写，
    状态机按幂等键只生效一次（见 `TestRetryAcrossLeaderChange`）。
  - 更靠后的请求已应用后又收到更旧序号的请求，返回 `ErrStaleRequestKind`。
- 线性一致读采用 ReadIndex 方式：领导者先为本轮读取得多数派存活确认，再等到
  该时刻的 `commitIndex` 全部应用后读本地状态机，确保不会读到旧主的陈旧数据。
- 提案/读的等待超时为一个选举超时：到期仍未获多数派确认则失败，调用方可凭
  同一幂等键安全重试。

### 1.4 可区分的失败原因（`errors.go`）

| ErrorKind | 触发场景 |
|---|---|
| `ErrNotLeaderKind` | 对非领导者提案；或等待期间失去领导地位（更高任期出现）。错误携带 `LeaderHint` |
| `ErrNoQuorumKind` | 是（或曾是）领导者，但选举超时内没有得到多数派确认（典型：分区中的旧主） |
| `ErrStaleRequestKind` | 请求序号过期：同一客户端更新的请求已经应用 |
| `ErrClosedKind` | 节点已停止 |
| `ErrUnsupportedVersionKind` | 持久化数据来自更新版本，拒绝启动接管 |

### 1.5 确定性模拟模型

- 一个 `tick` = 所有节点先按 ID 顺序处理超时（可能触发选举/心跳），随后
  `SimNetwork.Deliver` 把本轮排队的消息按确定顺序交给目标节点；回复在同一轮内
  排空，直到没有新消息。
- 分区期间的消息**直接丢弃**（不排队），对端表现为超时；`Partition` 是组间双向
  隔离，`Isolate` 隔离单节点，`Heal` 恢复全连通。
- 没有真实 goroutine 参与协议推进：`Node.Propose/Read` 在等待循环中通过注入的
  `drive` 回调推进集群时间，因此即使并发客户端（真实 goroutine）压测，每个副本的
  状态转移仍然确定，提交序号逐次运行一致（见下文验证）。

---

## 2. 持久化格式

每个副本在响应投票/追加之前必须先持久化的“硬状态 + 日志”由 `Persister` 保存
（`MemoryPersister` 供测试，`FilePersister` 采用临时文件 + fsync + rename 原子落盘）。

文件布局（v1，全部小端字节序）：

| 偏移 | 长度 | 字段 |
|---|---|---|
| 0 | 4 | magic，ASCII `"RAFT"` |
| 4 | 4 | CRC-32/IEEE，校验范围为其后所有字节 |
| 8 | 2 | format version（uint16，当前为 `1`） |
| 10 | 8 | currentTerm（uint64） |
| 18 | 8 | votedFor（int64，`-1` 表示本任期未投票） |
| 26 | 4 | 日志条目数 n（uint32） |
| 30 | … | n 个 LogEntry |

每个 `LogEntry` 为：

| 长度 | 字段 |
|---|---|
| 8 | Index（uint64） |
| 8 | Term（uint64） |
| 1 | Op（1=Put, 2=Delete, 0=领导者 no-op） |
| 4+ | Key：uint32 长度前缀 + UTF-8 字节 |
| 4+ | Value：uint32 长度前缀 + UTF-8 字节 |
| 8 | ClientID（uint64，幂等键） |
| 8 | Seq（uint64，幂等键） |

### 2.1 版本兼容策略

1. **magic 不匹配**或文件截断：视为非本组件数据，报错，拒绝启动。
2. **CRC 不匹配**：视为介质损坏，报错，避免用脏数据参与选举。
3. **`version > PersistentDataVersion`（数据来自未来版本）**：返回
   `ErrUnsupportedVersionKind`，节点拒绝加载，防止旧二进制误解析新格式后
   “静默接管”。
4. **`version <= PersistentDataVersion`**：当前只有 v1；未来引入 v2 时，读取路径
   按版本分派并对旧版本执行前向迁移，保证旧数据可被新二进制打开（向后兼容）；
   新版本保存时可继续写 v1 或显式升级，升级为一次性、单向操作。
5. 文件不存在：视为全新节点（term=0、votedFor=-1、空日志）。

---

## 3. 验证方法

### 3.1 运行测试

```bash
go test ./... -race -count=10 -v   # 全部用例，竞态检测，重复 10 次
```

测试覆盖与需求的对应关系：

| 测试 | 覆盖需求 |
|---|---|
| `TestLeaderElection` | 任期推进、同任期唯一领导者 |
| `TestBasicWriteRead` | 多数派提交后才可见、线性一致读 |
| `TestMinorityCannotWrite` | 分区少数派（追随者 / 被隔离旧主 / 少数派新主）均不得写；错误分别为 NotLeader / NoQuorum；恢复后历史一致收敛、未提交写入不残留 |
| `TestLeaderRestart` | 崩溃恢复、状态机重放、落后副本幂等追赶、重复投递稳定性 |
| `TestIdempotentRetryAndStale` | 同键重发 exactly-once、过期请求 `ErrStaleRequestKind` |
| `TestErrorKindsDistinguished` | NotLeader / NoQuorum / Closed 错误可区分 |
| `TestConcurrentClients` | 8 个真实 goroutine 客户端并发写 120 次，最终值与各副本 commit/applied 一致（-race） |
| `TestRetryAcrossLeaderChange` | 分区中同键多次重试不提交，恢复后跨领导者恰好应用一次 |
| `TestFilePersisterRoundTrip` | 持久化往返（含 UTF-8） |
| `TestPersisterVersionCompat` | 未来版本拒绝、v1 兼容、CRC 损坏检出、缺省全新状态 |

### 3.2 可观测性

每个关键阶段测试日志都会打印**任期（term）、提交序号（commitIndex/lastApplied）
与判定依据**，例如：

```
判定依据: 隔离的旧领导者写被拒绝 kind=2 (raft: no quorum acknowledged ...)
判定依据: 所有节点收敛 term=5 commitIndex=3 且 applied==commit
判定依据: 同一 (client,seq) 跨领导者重试后 appliedIndex=4 且不再增长，k=V
```

### 3.3 确定性的手工验证

```bash
go test -run TestConcurrentClients -count=5 -v | grep 'commit='
```

每次运行各节点 `commitIndex` 完全相同（当前固定参数下为 121，即 120 条客户端写
+ 1 条领导者就职 no-op），说明结果可复现。

### 3.4 作为库使用的最小示例

```go
c := raft.NewCluster(3)
defer c.Stop()
if _, _, err := c.RunUntilLeader(200); err != nil { /* ... */ }
cl := raft.NewClient(c)
_, _ = cl.Put("key", "value")   // 内部完成选举发现与同键重试
v, err := cl.Get("key")        // 线性一致读
c.Partition([]int{0}, []int{1, 2}) // 制造分区
c.Heal()                           // 恢复并收敛
_ = c.Restart(0, false)            // 基于持久化重启节点
```
