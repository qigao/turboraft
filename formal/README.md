# TurboRaft bounded SPIN models

这些模型是 TurboRaft Core、membership transition、Ready 处理协议和
snapshot metadata 的有限状态抽象。模型之间通过显式契约组合，不把
CoroNet、TLS、wire bytes 或应用 snapshot payload 放进同一个状态空间。

## 模型边界

| 文件 | 验证边界 | 不负责证明 |
|---|---|---|
| `raft_core.pml` | 静态三节点的 term、vote、leader completeness、commit safety | C 实现的逐条代码等价性、动态 membership |
| `membership.pml` | stable/joint/final transition、old/new quorum、单一 pending transition | log replication 和网络传输 |
| `ready_lifecycle.pml` | `persist -> send -> apply -> advance`、失败后 fault、部分发送 | WAL 文件格式实现细节 |
| `snapshot_meta.pml` | snapshot boundary、configuration metadata、compact/install 前置条件 | snapshot bytes、digest、chunk transport |

模型中的所有容量都是验证边界，不是生产配置。没有反例只表示在声明的
有限节点、term、log 和队列范围内通过。

当前 `raft_core.pml` 使用 3 个节点、最多 2 条 log entry 和 2 个 term；其余
模型也使用固定的小配置，以便在开发机上完成穷举。

## 本地运行

需要 `spin.exe` 和 Clang。PowerShell 入口会把生成的 `pan.c`、可执行文件和
trail 放在临时目录，不污染工作树：

```powershell
pwsh -File .\formal\verify_spin.ps1
pwsh -File .\formal\verify_spin.ps1 -Model membership.pml
pwsh -File .\formal\verify_spin.ps1 -Model ready_lifecycle.pml
pwsh -File .\formal\verify_spin.ps1 -Model snapshot_meta.pml
```

发现反例时，脚本会保留临时目录路径；使用 SPIN 的 `-t -p` 重放对应
`.trail`，再将事件序列转换为 C simulation 的输入 trace。

## 组合契约

1. Core 在一个 owner 调用中产生一个 bounded effects batch。
2. 产生非空 effects 后，Core 不接受下一次输入，直到 `advance` 或进入 fault。
3. Storage commit 成功前，effects 中的网络消息不可见。
4. 传输按消息逐条接受；中途失败时，已经接受的前缀保留，Runtime fault，Core 不 advance。
5. Snapshot point 只在 `applied == commit` 且没有 outstanding Ready 时产生。
6. Snapshot metadata 的 configuration 必须是该 boundary 的 committed configuration。

这些契约与现有 `tr_raft_runtime_process()`、`tr_raft_core_snapshot_point()`
和 `tr_raft_core_compact()` 的边界对应。模型不是生产代码的替代品；C 单元测试、
chaos、fuzzing 和恢复测试仍然必须运行。
