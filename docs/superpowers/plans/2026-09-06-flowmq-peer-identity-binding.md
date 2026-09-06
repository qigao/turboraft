# FlowMQ 证书与 Raft Peer 身份绑定实施计划

> 执行方式：按 `executing-plans` 在当前会话逐项实施；每个行为先写可观察失败测试，再做最小实现。

## 目标

关闭 TLS 已认证证书与 FlowMQ HELLO identity 彼此独立的 HIGH 风险：TLS ROUTER 必须只接受配置中明确授权的 `(SHA-256 certificate fingerprint, HELLO identity)` 元组，并让 TurboRaft 将该拒绝作为只读状态指标暴露。普通 TCP 和未配置策略的 FlowMQ socket 保持现有行为。

## 架构决策

### 背景与状态归属

- CNet connection 是已验证 peer certificate 的唯一事实源；FlowMQ HELLO frame 是远端声明 identity 的唯一事实源。
- FlowMQ socket 同时拥有 connection 和 HELLO 解码状态，能够在 identity 进入 ROUTER peer table 前原子校验二者。
- TurboRaft 拥有 Raft node-id、HELLO identity 和允许证书指纹的部署配置，但接收消息时不再拥有 connection/certificate 上下文。

### 候选方案

1. TurboRaft 收到 ROUTER envelope 后校验：不可选，因为公开 receive 结果没有 connection 或证书上下文，校验发生得太晚。
2. 修改 wire protocol，把证书指纹写入 HELLO：不可选，因为远端可伪造自报指纹，且会改变协议格式。
3. FlowMQ 在 TLS ROUTER 的 HELLO 接收边界查询 CNet verified fingerprint，并用不可变 map 精确匹配：选择此方案。

### 权衡

- 性能：每条连接仅在 HELLO 阶段查询一次证书并对最多 1024 条有界 binding 做精确匹配；数据热路径无新增工作。
- 复杂度：FlowMQ socket 新增一个拥有型 opaque policy 和一个饱和拒绝计数器；不新增线程、锁、fallback 或通用 utils。
- 可维护性：身份授权集中在拥有全部事实的 transport boundary；TurboRaft 只做配置适配。
- 接口：FlowMQ 新增两个 socket option；TurboRaft 扩展公开 peer config/status struct，0.1.x 消费方需要重新编译。
- 错误：配置组合在 bind/create 时 fail fast；运行时未授权 peer 仅关闭自身连接并增加计数，ROUTER 继续服务其他 peer。

### 迁移与回滚

- FlowMQ 未设置 policy 时保持兼容；设置 policy 时必须是 TLS ROUTER 且启用 client-certificate requirement。
- TurboRaft 的 TLS listener 改为强制 mTLS，并要求每个 peer 有 1..`TR_RAFT_FLOWMQ_MAX_CERTIFICATES_PER_PEER` 个 canonical fingerprint；普通 TCP 必须保持 NULL/0。
- 证书轮换通过同一 identity 同时配置旧、新指纹，部署完成后移除旧指纹并重启；本期不做热加载。
- 回滚时先回滚 TurboRaft 配置/API，再回滚 FlowMQ socket options；wire format 和持久化数据均未改变。

## Task 1：FlowMQ public socket policy API

**仓库：** `C:\projects\cpp\turbonet\flowmq-tls-identity-policy`

**文件：**

- 修改：`flowmq/include/flowmq_socket.h`
- 修改：`flowmq/tests/runtime/test_flowmq_socket.c`

- [x] 新增 `FLOWMQ_TLS_IDENTITY_POLICY = 1007` setter option，value 为完整 `flowmq_tls_identity_map_config_t`，调用期间同步复制。
- [x] 新增 `FLOWMQ_TLS_IDENTITY_REJECTIONS = 1008` getter option，返回 `uint64_t`。
- [x] 先写编译/行为测试并确认旧实现失败：非 ROUTER、非 TLS 或未要求 client certificate 的 policy bind 必须返回 `SALTS_EINVAL`；option 只能在 runtime 初始化前设置。

## Task 2：FlowMQ HELLO 身份认证

**文件：**

- 修改：`flowmq/src/runtime/flowmq_socket.c`
- 修改：`flowmq/tests/runtime/test_flowmq_socket.c`
- 修改：`README.md`

- [x] socket 拥有并在 close 时销毁 immutable identity map；setter 失败不得替换已有有效状态。
- [x] TLS ROUTER 收到合法 HELLO 后、复制 identity 前，通过 `cnet_tls_peer_certificate_sha256` 取得 64 位小写十六进制摘要，构造成 canonical `sha256:` 文本并精确校验。
- [x] 未授权或证书查询失败只 retire 当前 peer；未授权计数使用饱和加法，避免 `uint64_t` 回绕。
- [x] 真实 TLS ROUTER/DEALER 测试覆盖：匹配元组可传输、证书不匹配拒绝、HELLO identity 不匹配拒绝、拒绝后合法 peer 仍可传输、配置字符串已复制、counter size/error contract。
- [x] 运行 FlowMQ focused test、全量 Release CTest、`git diff --check`。

## Task 3：安装 FlowMQ Release 包

**配置来源：** `CMakeUserPresets.json`

- [x] 使用 `win-release-user` configure/build/test 和 `install-win-release-user`，安装到 preset 推导的 `C:\projects\cpp\external\pkgs\flowmq\release`。
- [x] 不复制 DLL；测试运行时仅使用 preset PATH。若本地旧 DLL 与安装包冲突，只删除已核实位于当前项目 build tree 的过期副本。
- [x] 验证安装头文件和 package 导出包含新 option/API 依赖。

## Task 4：TurboRaft 配置适配与 fail-fast

**仓库：** `C:\projects\cpp\turbonet\turboraft-backup-chaos-handoff`

**文件：**

- 修改：`include/turboraft/raft_flowmq_peer_service.h`
- 修改：`src/transport/raft_flowmq_peer_service.c`
- 修改：`tests/core/test_raft_flowmq_peer_service.c`

- [x] 定义 `TR_RAFT_FLOWMQ_MAX_CERTIFICATES_PER_PEER = 4U`，peer config 新增 borrowed fingerprint array/count，service create 内同步编译并由 FlowMQ 复制。
- [x] TLS listener 强制 `require_client_certificate == 1`，每个 peer 必须配置 1..4 个 canonical fingerprint；普通 TCP 要求 NULL/0，不提供兼容 fallback。
- [x] 使用有界 fixed bindings 数组，容量计算为 `(TR_RAFT_MAX_VOTERS - 1) * 4`，检查累计数量与所有输入。
- [x] 把 policy 设置在 bind 前完成；任何失败都沿现有 cleanup 路径释放已创建 socket/context。
- [x] status 新增 `tls_identity_rejections`，通过 FlowMQ getter 获取；读取不推进业务状态。

## Task 5：TurboRaft 真实 mTLS 回归与文档

**文件：**

- 修改：`tests/core/test_raft_flowmq_peer_service.c`
- 修改：`docs/FLOWMQ_PEER_TRANSPORT.md`
- 新增：`docs/FLOWMQ_TLS_IDENTITY_BINDING.md`

- [x] 从固定 PEM fixture 手工记录 canonical SHA-256 期望值；正向 mTLS 测试显式配置 node-1 fingerprint。
- [x] 新增同一 CA 下有效 client certificate、但策略 fingerprint 不匹配的测试：消息不得到达 callback，`tls_identity_rejections` 必须增加。
- [x] 新增 create-time 表格测试：缺指纹、格式错误、超过上限、plain TCP 携带指纹、TLS 未开启 client auth 均 fail fast。
- [x] 文档说明所有权、生命周期、错误语义、轮换、兼容性、迁移与回滚。
- [x] 使用 `win-release-user` 运行 focused target/test、全量 Release CTest 和 `git diff --check`。

## Task 6：审查与交付

- [x] 同步两个 CodeGraph 索引，运行 affected/impact 检查并人工复核真实实现与调用点。
- [x] 扫描无归属 TODO/FIXME/HACK、占位实现和意外生成文件。
- [x] 分别提交 FlowMQ 与 TurboRaft，push feature branch，并创建互相引用 Issue #2 / Issue #15 的 PR；TurboRaft PR 标注对 FlowMQ 新版本的依赖。

## 验收证据

- FlowMQ：真实 TLS 测试证明正确 cert+identity 被接收，任一维度不匹配被拒绝且 router 保持可用。
- TurboRaft：真实 mTLS delivery 保持通过，伪造 identity 的同 CA client 无法触发 Raft callback，拒绝计数可查询。
- 两仓库 Release configure/build/CTest 全绿；不依赖工作目录 DLL 拷贝；无 wire 或持久化格式变化。
