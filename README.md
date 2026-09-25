# replicated-consensus-go

单机可运行的**多副本强一致状态机复制**组件。用一个进程内、回环网络、
**完全确定性虚拟时钟**模拟多个副本与客户端，可复现地验证：

- 领导者选举与任期推进（同一任期至多一个领导者）；
- 多数派提交（写入在多数确认前对调用方不可见）；
- 网络分区、领导者重启、并发与重试下结论一致；
- 客户端超时重发的端到端幂等；
- 可区分的失败原因（不把所有错误归一化）。

算法为教学/验证用途的 **Raft** 变体，实现刻意保持单文件可读、无第三方依赖。

---

## 1. 复制协议

### 1.1 角色与任期

每个副本在任一时刻是 `Follower` / `Candidate` / `Leader`（见 `types.go` 的 `Role`）。

- 任期 `term` 单调递增，持久化在 `HardState{Term, VoteFor, CommitIndex}`。
- 跟随者在随机选举超时（`ElectionMin..ElectionMax`）内收不到合法领导者的
  `AppendEntries`，就自增任期成为候选人并拉票。
- 候选人获得多数投票才成为领导者；**同一任期最多一个领导者**
  （每个节点每任期最多投一票，且投票在比较候选人日志“至少一样新”后才给出）。
- 任何节点收到更高任期的消息立即退回跟随者；候选人收到同任期合法领导者的
  `AppendEntries` 也退回跟随者。

关键不变量与对应判定依据都会打印进测试日志，例如：

```
[n3] become-leader: term=1 commit=0 votes=2/3 (basis: majority votes granted)
[n3] vote: term=2 from=n2 granted=false commit=3 (candidate log behind mine -> reject)
[n3] step-down: term=2 -> follower (basis: discovered higher term; ...)
```

### 1.2 日志复制与提交

- 客户端写命令经 `Node.Propose` 追加为 `Entry{Term, Index, Command}`。
- 领导者为每个跟随者维护 `nextIndex / matchIndex`，通过
  `AppendEntries(PrevLogIdx, PrevLogTerm, Entries, LeaderCommit)` 推送。
- 跟随者做一致性检查：`PrevLogIdx/PrevLogTerm` 不匹配就拒绝并回退，
  领导者按响应里的冲突提示退避重发；**追赶过程在重复投递下幂等**
  （相同 `(Index,Term)` 跳过，冲突位置截断后重写）。
- 提交规则严格遵循 Raft：只有当某条**本任期**日志被多数 `matchIndex` 覆盖时，
  领导者才推进 `commitIndex`。旧任期条目不能仅凭副本计数提交，
  从而避免“已提交后被覆盖”。
- `commitIndex` 通过后续 `AppendEntries.LeaderCommit` 传播；
  领导者在提交推进的当刻会立即补发一轮（不必等下一次心跳）。
- 已提交日志按序应用到确定性状态机（`KVStore`）。**未提交的写入在领导更替后
  绝不残留为已提交结果**：选举/下台会把在途提案明确失败为 `not_leader`，
  少数派日志在恢复后被多数派历史截断覆盖。

### 1.3 少数派不得服务写

被隔离在少数派分区的领导者继续接收写请求时，无法收集多数确认：
- 请求在 `ProposalDeadline` 后以 `no_quorum`（无任何跟随者确认）或
  `timeout`（仅部分确认）失败，**结果对调用方不可见**；
- 其本地未提交日志在分区恢复后被多数派历史截断（测试中断言 `ghost` 键不存在）。

### 1.4 线性一致读

`Node.Read` 采用 ReadIndex 思路：领导者先广播一轮心跳确认自己仍被多数承认，
然后等待本地应用到该提交点，再返回本地状态。跟随者的读请求被
`not_leader` 拒绝（客户端改道领导者）。

### 1.5 客户端幂等（超时重发不重复应用）

每条客户端写命令携带单调递增的 `(ClientID, Seq)`：

- 状态机维护每客户端会话：相同 `(ClientID, Seq)` 且命令体相同 →
  返回首次结果并标记 `Duplicate=true`，**绝不第二次改变状态**；
- 同序号但命令体不同、或序号倒退 → 明确返回 `stale_request`，不静默覆盖；
- 会话缓存在重启后由已提交日志重放重建，无需单独持久化；
- 领导者对同一 `(ClientID, Seq)` 的在途提案去重，重发直接复用原 future，
  不产生未提交垃圾条目。

### 1.6 可区分的失败原因

`WriteError.Reason`（见 `errors.go`）区分：

| Reason | 含义 |
| --- | --- |
| `not_leader` | 当前节点不是领导者（附 `LeaderHint` 与当前 `Term`） |
| `no_quorum` | 多数派不可用（少数派分区） |
| `stale_term` | 请求来自更旧任期，被明确拒绝 |
| `stale_request` | 客户端序号倒退或同序号命令冲突 |
| `timeout` | 仅有部分确认、在期限内未达多数 |
| `closed` | 节点已关闭 |

可用 `errors.Is(err, raft.ErrNotLeader)` 等哨兵判定，也可解析 `*WriteError`
拿到任期与领导者提示。

---

## 2. 持久化格式（WAL）

实现在 `wal.go`，单文件、append-only、小端二进制、自描述：

```
header  : magic(4B = "RFLW") | version(1B) | flags(2B LE) | reserved(1B)     共 8 字节
record* : type(1B) | crc32c(4B LE) | payloadLen(4B LE) | payload(payloadLen B)
```

- `type=1` HardState：payload 为 `encoding/gob` 编码的 `HardState`，重放时后者覆盖；
- `type=2` EntryBatch：payload 为 gob 编码的 `[]Entry`，按记录顺序重放；
- 每条记录用 **CRC32C（Castagnoli）** 校验；
- 崩溃写到一半的尾部记录（头部/负载截断或校验失败）在最后一条完整记录处停止重放，
  并把文件截断到该位置，返回包装了 `ErrCorruptWAL` 的错误；
- 日志冲突需要截断旧尾部时，用“写临时文件 + fsync + rename”原子重写整个 WAL，
  不会留下半新半旧的文件。

内存视图见 `MemoryStorage`（无重启场景），文件视图见 `FileWAL`（重启场景）。
两者实现同一 `Storage` 接口，遵循相同的幂等追加规则：
相同 `(Index,Term)` 跳过、任期冲突截断、超出末尾追加。

### 2.1 版本兼容策略

- 文件头第 5 字节为格式版本；当前为 **v1**。
- **读旧不读新**：仅接受 `0 < version <= 当前版本`；更高版本一律以
  `ErrUnsupportedVersion` 拒绝，避免旧程序误解新格式而破坏数据。
- magic 不匹配返回 `ErrNotWAL`。
- v1 内部的演进预留：重放遇到**未知 record type 跳过而非报错**，
  以便未来以“新增记录类型”的方式做向后兼容扩展；
  语义变化（而非纯新增）必须提升版本字节。

---

## 3. 确定性与可复现

- `SimClock`：毫秒粒度的可控虚拟时钟，不读墙钟、不起后台 goroutine。
- `Scheduler`：所有事件（选举超时、心跳、提案超时、网络投递、客户端重试）
  都按 `(时间, 入队序号)` 全序触发；同一时刻严格按入队顺序执行。
- `LoopTransport`：回环传输，消息**只经调度器**延迟投递（默认 2ms），
  从不在调用栈内联执行；`SetPartitions` 可任意设置有向可达关系。
- 选举随机化由固定种子的 xorshift 产生，同种子序列可复现。
- 为避免“永久分区中少数派无限重选”使仿真不终止，事件分为
  外部事件（网络消息/客户端重试）与内部事件（选举/心跳/超时）：
  `Scheduler.Settle/SimEnv.Settle` 处理到外部事件静默；
  `SimEnv.Converge` 在外部静默后再给一个选举周期的内部时间窗，
  用于分区恢复后的多轮选举与追赶，并有总虚拟时间上界。

`TestDeterministicReproduction` 把同一脚本连跑两次，逐字节比较
领导者、任期、提交序号序列；`go test -count=N` 可重复验证。

---

## 4. 验证方法

```bash
go test ./...                 # 全部场景
go test -race ./...           # 竞态检测
go test -count=10 ./...       # 可重复性
go test -v -run TestPartition # 查看带 term/commit/basis 的协议日志
go test -cover ./...          # 覆盖率（约 78%+）
```

测试矩阵：

| 测试 | 覆盖点 |
| --- | --- |
| `TestPartitionMinorityCannotWrite` | 少数派不可写(`no_quorum`)、多数派重选更高任期、恢复后历史收敛、未提交写不残留 |
| `TestLeaderRestartPersistence` | FileWAL 落盘、崩溃重放、去重缓存重建、重启后历史连续 |
| `TestWALReopenReplays` | WAL 关闭重开、幂等重复追加、冲突原子截断重写 |
| `TestWALVersionReject` | 更高版本文件被 `ErrUnsupportedVersion` 拒绝 |
| `TestIdempotentRetry` | 相同 `(ClientID,Seq)` 只应用一次、冲突/旧序号返回 `stale_request` |
| `TestNotLeaderErrorDistinction` | 跟随者读写返回 `not_leader` 并带正确 `LeaderHint` |
| `TestConcurrentProposals` | 12 个客户端并发写，5 副本恰好各应用一次并收敛 |
| `TestClientRetryAcrossLeaderChange` | 分区+改道下客户端自动重试，相同序号不重复应用 |
| `TestLinearizableRead` | ReadIndex 读多数确认后返回，跟随者读被改道 |
| `TestDeterministicReproduction` | 两次运行的任期/提交序列逐字节一致 |

### 4.1 在代码中装配一个集群

```go
clock := raft.NewSimClock(0)
sched := raft.NewScheduler(clock)
tr := raft.NewLoopTransport(sched)

env, _ := raft.FileCluster(t.TempDir(), []string{"r1", "r2", "r3"}) // 或 NewSimEnv(3)
defer env.Close()
env.WithLogger(logger)

leader := env.Leader(2000)            // 推进事件直到选出领导者
client := raft.NewClient("app", env.Nodes, sched)

f := client.Set(ctx, "key", "value") // 内部自动发现领导者/重试/幂等
env.Settle()                          // 驱动到外部事件静默
if err := f.Err(); err != nil { ... } // errors.Is 区分失败原因

env.Isolate(leader)                  // 制造分区
env.HealAll(); env.Converge(3000)    // 恢复并等待收敛
```

---

## 5. 代码地图

| 文件 | 职责 |
| --- | --- |
| `types.go` | 角色、命令、日志条目、RPC 消息 |
| `errors.go` | 可区分失败原因与哨兵错误 |
| `node.go` | Raft 选举/复制/提交/读状态机（核心） |
| `storage.go` | `Storage` 接口与 `MemoryStorage` |
| `wal.go` | 文件 WAL：二进制格式、CRC、崩溃重放、原子截断重写 |
| `statemachine.go` | 确定性 KV 状态机与会话幂等去重 |
| `transport.go` | 确定性回环传输与分区规则 |
| `clock.go` | 虚拟时钟与内/外部事件调度器 |
| `future.go` | 写/读 future（单次兑现、完成回调） |
| `client.go` | 带会话序号、领导者发现与自动重试的客户端 |
| `sim.go` | 集群装配、重启、分区、收敛驱动 |

## 6. 范围与限制

- 面向单机仿真与协议验证，不含真实网络 RPC、成员变更、日志快照与日志压缩；
- 存储接口已预留，可接入真实持久层与网络传输；
- 状态机仅实现 `set / inc` 两类确定性写，足以演示幂等与并发收敛。
