# replicated-consensus-go

单进程可运行的多副本强一致状态机复制组件（Raft 风格）：以可控时钟与回环网络
模拟多个副本与客户端，覆盖领导者选举、日志复制、分区、崩溃重启与并发幂等重试，
结果确定且可复现。

```go
c := raft.NewCluster(3)
defer c.Stop()
_, _, _ = c.RunUntilLeader(200)
cl := raft.NewClient(c)
_, _ = cl.Put("key", "value")
v, _ := cl.Get("key")
```

## 运行验证

```bash
go test ./... -race -count=10 -v
```

协议语义、错误分类、持久化格式与版本兼容策略、测试与需求的对应关系见
[docs/design.md](docs/design.md)。
