# Ready 持久化与状态机结算分离实施计划

**目标：** 在不改变现有 `tr_raft_runtime_process()` 原子 batch 语义的前提下，增加一个有界、单 owner、按日志索引确认的异步 apply 路径，使 CFlow Statechart 的 tagged macrostep settlement 能安全推进 Raft `applied_index`。

**关联：** TurboRaft #22、#23；Salts #234、#236。

## 架构决定

现有 Ready 同时承载持久化、消息发送和 committed-entry application。旧 `tr_raft_core_advance()` 一次确认全部三类工作，因此继续作为兼容接口，行为不变。新路径先以 `tr_raft_core_acknowledge_ready()` 确认持久化、消息交接及 entry ownership transfer，推进 volatile dispatched cursor 并关闭 Ready；随后 `tr_raft_core_acknowledge_applied_entry(core, index)` 只接受 `applied_index + 1` 且不超过 dispatched boundary，逐条推进连续前缀。split mode 与旧 `advance()` 不可混用。

异步路径使用独立 opaque `tr_raft_apply_runtime_t`，避免扩大公开的 `tr_raft_runtime_t`。它在产生任何外部副作用前，把 committed entries 复制到创建时配置的固定容量中；随后持久化与消息只执行一次。Runtime 每次最多允许一个 application entry in flight，并通过 `try_apply`/`poll_settlement` SPI 接收精确 token 结算。`APPLIED` 推进一个索引；`PENDING` 只授权相同 token 的精确重试；`GAP`、`CONFLICT`、`UNKNOWN` 使 Runtime fail fast。配置 entry 自动在 owner 线程确认，不进入 app 状态机。

CFlow 适配器是独立可选 target `TurboRaft::CFlowStateMachine`，只依赖 `TurboRaft::Core` 与 `Salts::CFlow`。它在 Statechart 初始化前提供 V5 hook，在初始化后绑定专用 instance。Raft entry 由应用 decoder 转成 call-scoped `cflow_event_view`；`try_send_tagged()` 成功后 CFlow 拥有 payload 副本。settlement hook 只把值类型结果写入一个 mutex 保护的固定槽并返回，Raft owner 线程稍后 poll；hook 不调用 Core、不阻塞、不保存 CFlow 借用指针。

## 数据与生命周期协议

- **数据单元：** committed entry 是固定大小 `tr_raft_entry_t` 副本；in-flight 身份是非零日志索引 token；settlement 是 token、outcome、cause 的值类型记录。
- **事实源：** Core 的 `applied_index` 是 Raft 已应用前缀事实源；volatile dispatched cursor 只避免运行期重复派发。应用状态及其持久化 applied marker 由状态机负责，Runtime 只在收到结算证明后推进 Core。
- **所有权：** `start()` 在返回前复制 Ready entries；Ready、decoder 输出 Event 和 CFlow settlement 参数均为 call-scoped borrow。Runtime 销毁自己的固定 entry 存储，CFlow instance 与 callback user 仍由应用按顺序销毁。
- **拓扑：** Runtime 的 start/poll/destroy 是单 owner；CFlow settlement producer 可来自 SerialExecutor 或控制线程，适配器槽是 MPSC-to-single-owner，但协议限制同一时刻仅一个 Raft Event in flight。
- **顺序：** 全局日志索引严格递增；Runtime 不越过未结算 application entry。配置 entry 仅在之前的索引已经确认后推进。
- **容量：** `max_pending_entries` 是必填正数；创建时检查 `count * sizeof(tr_raft_entry_t)` 溢出并一次性分配，运行中不扩容。超过容量时在 storage、transport、CFlow 之前返回 `SALTS_ENOBUFS`。
- **背压：** CFlow mailbox FULL 保持 Runtime active 且没有 in-flight Event；后续 poll 只重试当前索引。没有丢弃、覆盖或无界分配。
- **失败：** storage/transport、非法 settlement、CFlow CLOSED/FAILED、GAP/CONFLICT/UNKNOWN 均 fault Runtime；Ready 若已完成 durable/send 阶段则已关闭，Core 只保留已经确认的连续前缀。继续服务前必须以应用持久化 marker 重建 Core。
- **关闭：** 先停止 Runtime start，再等待/取消 CFlow work 并消费 terminal settlement；非故障 active Runtime 的 destroy 返回 `SALTS_EBUSY`。销毁 Runtime 后再销毁 CFlow instance，解绑并销毁适配器。故障 Runtime 只能作为进程级恢复的一部分释放，不伪造 Core advance。
- **观测：** 每次 drive 返回 phase、cause、durable、已发送数量、in-flight token 与 `applied_through`；读取不推进状态。

## 兼容性与回滚

- 旧 Core/Runtime 结构体、枚举数值、函数签名和 batch 回调不改；Redis Lua 原子 batch 路径继续使用旧接口。
- 新头文件和 target 是纯增量；`TurboRaft::Core` 不链接 CFlow，只有显式链接 `TurboRaft::CFlowStateMachine` 的使用方获得该依赖。
- 若新路径需要回滚，删除新增 target/header/source 即可；旧调用方和存储格式不受影响。
- 公开行为风险集中在 Core 新确认函数。验证须覆盖重复、跳号、越界、partial-failure、恢复 suffix 和旧 full-advance 回归。

## Task 1：Core 连续 applied-prefix API

**文件：** `include/turboraft/raft_core.h`、`src/core/raft_core.c`、`tests/core/test_raft_core.c`、`tests/core/test_raft_recovery.c`

- [x] 先写测试：`acknowledge_ready()` 关闭 Ready 但不推进 applied，随后逐条确认连续索引。
- [x] 运行 focused test，确认因 API 缺失而 RED。
- [x] 实现 dispatched cursor、永久 split mode 与 next-index/boundary 校验。
- [x] 写并验证重复、跳号、未 dispatch、超出 boundary 的 fail-fast 测试。
- [x] 写恢复测试：只持久化成功前缀后重建 Core，下一 Ready 仅暴露 suffix。

## Task 2：有界异步 entry apply Runtime

**文件：** `include/turboraft/raft_apply_runtime.h`、`src/runtime/raft_apply_runtime.c`、`CMakeLists.txt`、`tests/core/test_raft_apply_runtime.c`、`tests/core/CMakeLists.txt`

- [x] 先写测试：两条 application entry 中第一条 `APPLIED`、第二条 `UNKNOWN`，Core 只推进第一条，storage/message 各执行一次。
- [x] 运行 focused target，确认声明或实现缺失导致 RED。
- [x] 增加 ABI V1 config、admission、settlement、result、create/start/poll/destroy 公共契约。
- [x] 实现固定 entry 容量、一次性 durability/message phase、单 in-flight token 与逐条 Core ack。
- [x] 覆盖 FULL 重试、PENDING 同 token 重试、错误 token、配置 entry、容量+1、重复 start、active destroy 和旧 batch Runtime 行为不变。

## Task 3：CFlow V5 tagged-settlement 适配器

**文件：** `include/turboraft/raft_cflow_state_machine.h`、`src/integrations/raft_cflow_state_machine.c`、`CMakeLists.txt`、`tests/core/test_raft_cflow_state_machine.c`、`tests/core/CMakeLists.txt`、`cmake/TurboRaftConfig.cmake.in`

- [x] 先写集成测试：decoder 将 entry payload 映射为 typed Event，CFlow 完成 macrostep 后 Runtime poll 推进相同 index。
- [x] 验证测试因适配器缺失而 RED。
- [x] 实现 create/hooks/bind/state-machine-SPI/unbind/destroy，V5 hook 只写一个固定 settlement 槽。
- [x] 覆盖 mailbox FULL、DROP 作为同 token `PENDING` 重试、FAILED/CANCELLED、错误 token fail-fast、未绑定和错误销毁顺序。
- [x] 使用 CMakeUtils 配置 target；找不到 `Salts::CFlow` 时配置阶段 fail fast，不增加 DLL 复制或兼容 fallback。

## Task 4：验证与交付

- [x] `win-release-user` fresh configure，构建 Core、CFlow adapter 和 focused tests。
- [x] 运行相关 focused tests，再运行完整 Release CTest（52/52）。
- [x] `win-dev-user` 构建并运行新增测试；运行库只从 `CMakeUserPresets.json` 的 PATH 解析。
- [x] `win-analyze-user` 以 `/analyze /WX` 构建相关目标并运行聚焦测试。
- [x] 安装 Release 包并用独立 package consumer 验证 `TurboRaft::CFlowStateMachine` 导出。
- [x] `codegraph sync .`、`git diff --check`、完整 diff 与公开头 C++ 编译检查。
- [x] 更新 #23，记录 API、验证结果及剩余生产风险；确认后提交、推送并走 PR/merge。
