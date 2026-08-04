# TurboRaft DSL 使用指南

TurboRaft 提供三套小型文本语言（DSL）：Query（只读巡检）、
Protocol-debug（线协议帧描述与校验）、Replay（确定性仿真）。
语法细节见 [TEXT_SYNTAX.md](TEXT_SYNTAX.md)；本文说明"怎么用"：
工具、CMake 链接、C API 骨架与端到端示例。

## 1. 能力总览

| DSL | 能做什么 | 现成入口 |
|---|---|---|
| Query | 生成只读巡检 plan（status/members/progress）并分发执行 | `turboraft_console`（命令行）、`tr_text_query_execute(_service)` |
| Protocol-debug | 用可读文本描述一帧 wire 帧，并用真实编解码器校验 | `tr_text_protocol_debug_execute`（C API） |
| Replay | 确定性仿真：选举、提交、poll、故障注入、期望断言 | `turboraft_repl`（交互/脚本）、`tr_replay_driver_*`（C API） |

## 2. 构建与链接

构建（可选示例）：
```powershell
cmake --preset win-release-user -DBUILD_EXAMPLES=ON
cmake --build --preset win-release-user --target turboraft_dsl_embed_demo
```

应用里按需链接已安装的 CMake target：

```cmake
find_package(TurboRaft CONFIG REQUIRED)
target_link_libraries(app PRIVATE
    TurboRaft::TextSyntax        # Query parse + generic execute
    TurboRaft::TextQueryService  # Query 直接绑 tr_raft_service_t
    TurboRaft::TextProtocolDebug # Protocol-debug 帧校验
    TurboRaft::ReplayDriver      # Replay 完整驱动（tr_raft_core_*）
)
```

命令行工具（随构建生成、不随包安装）：
- `turboraft_console`：远程巡检，依赖运行中的控制面（`/raft/rpc`）。
- `turboraft_repl`：本地确定性仿真 REPL。

## 3. 命令行使用

### 3.1 Query：turboraft_console（连真实集群）

前置：控制面已绑定到 `/raft/rpc` 并注册 `raft.status/members/progress`。

```powershell
# 一次性查询
turboraft_console --endpoint http://127.0.0.1:8080/raft/rpc ^
  --query "show members where role == voter;"

# 交互模式：每行一条语句，exit/quit 退出
turboraft_console --endpoint http://127.0.0.1:8080/raft/rpc
turboraft> show status;
turboraft> show progress for node 2;
```

支持语句：`show status;`、`show members;`、
`show members where role == voter|learner;`、`show progress for node N;`。
注意：console 只发送 Query；protocol/replay 文本不会发给远端。

### 3.2 Replay：turboraft_repl（本地仿真）

```powershell
# 交互模式（3 节点集群）
turboraft_repl --nodes 3
repl> node 1
repl> tick 30
repl> status
repl> submit 7 1 1 1 0x0102a0ff
repl> poll 7 applied 40
repl> expect --node 2 --commit 1
repl> partition 1 2
repl> heal 1 2
repl> exit

# 批处理：replay DSL 脚本
turboraft_repl --nodes 3 --script demo.rpl
```

`demo.rpl`（replay DSL 语法）：
```text
node 1;
node 2;
node 3;
tick 30;
submit request 7 to node 1 client 1 sequence 1 payload 0x0102a0ff;
poll request 7 until applied timeout 40 ticks;
tick 5;
expect node 2 commit_index >= 1;
```
完整交互命令见 REPL 内 `help`。命令行分析由 TurboUtils `turbo_cmd`
（`turbo_parser.h`）完成；为避免其对错误输入直接退出进程，REPL 会先预校验
命令名、参数个数与整数值，批处理请用 `run <file>` 或 `--script`。

## 4. C API 使用

### 4.1 Query（解析 + 通用执行）

```c
#include <turboraft/text_syntax.h>
#include <turboraft/text_query_executor.h>

static int on_status(void *ctx, const tr_text_query_command_t *cmd) {
    (void)ctx; (void)cmd;
    return TURBO_OK; /* 输出/上报 */
}
/* on_members / on_progress 同形 */

const char text[] = "show status; show members where role == voter;";
tr_text_query_plan_t plan;
tr_text_diagnostic_t diag;
tr_text_query_executor_ops_t ops = {
    .status = on_status, .members = on_members, .progress = on_progress,
};
int rc = tr_text_query_parse(text, strlen(text), NULL, &plan, &diag);
if (rc == TURBO_OK) {
    rc = tr_text_query_execute(&plan, &ops, NULL);
}
```

若应用持有 `tr_raft_service_t`，可直接用
`tr_text_query_execute_service(service, &plan, &sink, ctx)`，由适配器调用
`tr_raft_service_status/config/progress` 并做 role 过滤。

### 4.2 Protocol-debug（帧描述 + 真实编解码校验）

```c
#include <turboraft/text_protocol_executor.h>

static int on_frame(void *ctx,
                    const tr_text_protocol_frame_t *frame,
                    const tr_text_protocol_debug_decoded_frame_t *decoded) {
    (void)ctx; (void)frame;
    /* decoded->payload.raft / .snapshot_chunk / .snapshot_ack */
    return TURBO_OK;
}

const char text[] =
    "frame version 3 kind raft { from = 1; to = 2; term = 7;"
    " message = append_request; payload = 0x0102a0ff; }";
tr_text_protocol_debug_plan_t plan;
tr_text_diagnostic_t diag;
tr_text_protocol_debug_executor_ops_t ops = { .frame = on_frame };
int rc = tr_text_protocol_debug_parse(text, strlen(text), NULL, &plan, &diag);
if (rc == TURBO_OK) {
    rc = tr_text_protocol_debug_execute(&plan, &ops, NULL);
}
```

### 4.3 Replay（完整驱动：全部动作）

```c
#include <turboraft/text_replay_core_driver.h>

/* 1) 创建 3 节点驱动（voters = {1,2,3}，配置见 examples/dsl_embed_demo.c） */
tr_replay_driver_t *driver = NULL;
tr_replay_driver_config_t cfg = { .nodes = nodes, .node_count = 3 };
int rc = tr_replay_driver_create(&cfg, &driver);

/* 2) 解析并运行整份 replay 脚本 */
tr_text_replay_plan_t plan;
tr_text_diagnostic_t diag;
rc = tr_text_replay_parse(script, strlen(script), NULL, &plan, &diag);
if (rc == TURBO_OK) {
    rc = tr_replay_driver_run(driver, &plan);
}

/* 3) 增量执行单条动作（交互/驱动场景） */
tr_text_replay_action_t a = {0};
a.kind = TR_TEXT_REPLAY_SUBMIT;
a.request_id = 7; a.node_id = leader; a.client_id = 1; a.sequence = 1;
a.payload_hex.data = "0x0102a0ff"; a.payload_hex.len = 10;
rc = tr_replay_driver_step(driver, &a);

/* 4) 查询状态与网络计数 */
tr_raft_status_t status;
tr_replay_driver_status(driver, 1, &status);
tr_replay_driver_counters_t counters;
tr_replay_driver_counters(driver, &counters);

tr_replay_driver_destroy(driver);
```

驱动语义要点：
- `tick N` 按 1 tick 逐单位推进（单位内 tick 所有节点并投递到期消息）。
- `drop/delay/duplicate` 作用于下一个同名在途消息；`partition/heal` 控制有向链路。
- `submit` 目标节点必须是 leader；`poll` 内部推进时间至 `timeout_ticks`。
- `expect` 失败返回 `TURBO_EPROTO`。

## 5. 完整示例

仓库提供可编译运行的 `examples/dsl_embed_demo.c`
（`turboraft_dsl_embed_demo`）：它先用 Query DSL 解析三条巡检命令并分发，
再创建 3 节点 Replay 驱动，选举 leader、submit、poll 到 applied、断言
全部节点 commit >= 1，最后打印各节点状态与网络计数。运行：

```powershell
build/msvc-release/bin/turboraft_dsl_embed_demo
```

## 6. 边界与限制

- `turboraft_console` 只支持 Query；protocol/replay 文本不会发给远端节点。
- `turboraft_repl` 是本地进程内仿真，不连网络、不连控制面。
- 通用适配器 `tr_text_replay_execute()` 只实现 submit/poll/tick，其余动作
  返回 `TURBO_ENOTSUP`；完整动作请用 `TurboRaft::ReplayDriver`。
- 输入与容量：输入 ≤ `TR_TEXT_MAX_INPUT_BYTES`（1MB）、语句 ≤
  `TR_TEXT_MAX_STATEMENTS`（64）；submit payload ≤ `TR_RAFT_MAX_ENTRY_BYTES`、
  protocol payload ≤ `TR_RAFT_WIRE_MAX_FRAME_SIZE`，超限在解析期即报
  `TURBO_ENOSPC`。
- 配置类 DSL 不在范围内。
