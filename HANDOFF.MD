# 依赖恢复与配置边界

## vcpkg-cache 发布锁

- `cmake/dependencies/*.lock.json` 是 `github-release` 描述，不是 `locked` 的输入。
  直接将描述传给 `locked` 会因不支持的 source/feed 失败。
- 使用 `cmake/native_dependencies.py prepare` 调用所选 checkout 的共享
  `resolve-release`，在构建目录生成 URL/SHA-256 锁及 blobs；平台任务再用
  `restore` 调用共享 `locked`，并使用回执返回的 root。
- 库按 target triplet 恢复；re2c/Lemon 按显式 `QIGAO_HOST_RID` 恢复。
  Lemon 的可执行文件与 `lempar.c` 必须属于同一 package/version/host。
- 宿主工具已从仓库工具目录迁出；恢复入口在 `cmake/`，发布 ABI 检查在
  `packaging/nuget/abi/`。不恢复旧工具源码或独立 consumer 验证工程。

## Wire 生成代码

- 当前 wire 版本为 6；批量 Raft 消息和 snapshot 消息统一维护在
  `schema/turboraft_wire.schema`，只创建一个 DataBind binding。
- 生成代码由各 preset 的 `build/<preset>/generated/wire/` 独立持有，
  不再写入或保留源码目录中的生成副本。CMake 显式依赖 schema 和生成器。
- 所选 SaltsUtils 0.1.4 SDK 的 `salts-idlc`（3.0）需要 `--binary-codec`
  才会生成 wire helpers；直接启动时也需要匹配的 SaltsBase DLL 搜索路径。
  配置已有的 SDK 根目录及其 bin 路径，不使用历史生成器或生成源码兜底。
- 已用上述 SDK 成功生成合并后的头文件及 C 源码。现有 wire codec 测试
  扩展为八条 entry 的满批次往返，并检查当前固定字节布局；该 CTest 尚未执行；当前配置阻塞见下文。

## 当前验证与外部阻塞（2026-11-17）

- 按用户要求删除 FlowMQ 支持：实现、公开头文件、CMake target/组件/依赖、
  SDK 恢复入口及锁、CI 源码构建/包缓存/运行时路径、NuGet 依赖、专属测试、
  TLS fixture 和文档一并移除。不保留兼容别名或旧配置入口。
- `TurboRaft::Multicore` 保持传输无关的 owner 工厂、线程归属、有限队列、
  背压及关闭契约。专属网络复制用例删除，调度/并发/取消/WAL 回归保留。
- `NodeConfig` 只加载节点、调度、成员和存储配置；成员不再要求解析到已删除
  的 transport owner。网络配置/认证属于宿主工厂。旧网络字段按 unknown-field
  契约失败；示例仅接受单 voter、无 learner 的本地组，在打开 WAL 前拒绝远端成员。
  迁移与回滚边界见 [设计说明](docs/DESIGN.md#configuration-version-1)。
- `python -B tests/ops/test_native_dependencies.py`：7 项行为测试通过。
- `python -B packaging/nuget/abi/public_callback_contract.py --sdk-root .
  --contract docs/PUBLIC_CALLBACK_CONTRACTS.json`：55 个公开回调、22 个语义族通过。
- 本轮 `actionlint.exe -shellcheck= -pyflakes=`、修改的 CMake 文件
  `cmake-format --check`、`cmake --list-presets=all` 通过；相关 JSON/XML/Python
  语法及 README/docs 的本地文件链接检查通过（零缺失目标）。
- 根 CMake 按所选拆分 SDK 查找 `SaltsBase`、`CMeta`、`SaltsUtils`、`SaltsCNet`，
  使用加载后的 `*_VERSION` / `*_ROOT`、`EXACT` 和必需查找；不隐式读取 `.env`。
  安装配置记录实际解析版本，消费方通过自己的根目录寻找同版本拆分 SDK。
- CMeta 包导出 `CMeta::CMeta`、`CMeta::CSTL`、`CMeta::CFlow`；Core、NativeIO、
  Coroutine、TinyTest 仍属于 SaltsBase 的 `Salts::` target。
- 本地环境现已指定 CHttp SDK。保留用户原有的 `Chttp_DIR` 搜索输入后，
  `cmake --fresh --preset llvm-mingw-x64-release-user -DChttp_DIR=<原选定目录>`
  配置/生成通过；`cmake --build --preset llvm-mingw-x64-release-user` 已实际执行，
  在 Core 编译处因所选 SDK 未提供 `cmeta_error.h` 失败。CTest 未执行。
  此 preset 的配置结果不等于启用测试的完整 graph 通过；新增 C 回归尚未运行。
- CHttp、测试用 TurboDB 使用标准 `CMAKE_PREFIX_PATH` 或 `<Package>_DIR`，
  不额外要求 `.env` 定义它们的版本/根目录。不要猜测其他 checkout 的 SDK 路径、
  回退旧包或关闭模块/测试绕过失败。
- 当前 NuGet/source CI 输入仍属于旧单体 SDK 代际；需要匹配拆分 SDK 的生产者
  输入后才能验证完整 graph。workflow 声明不等于 CI 通过。
- 之前真实 `prepare` 在发布元数据请求处返回 HTTP 401，未完成下载/恢复。
  修正父环境中的 GitHub Packages/Release 读取凭据后，按 README 的 prepare/restore
  流程重试；单测和宿主工具检测不代表完整依赖恢复成功。

## xxHash 查找失败：共享已恢复目录被裁剪

- 触发：`.env` 指向另一个项目的已恢复 vcpkg-cache payload；CMake 在查找
  `xxHash` 时失败。该 triplet 的 `vcpkg/status` 将 xxhash 0.8.3 标为
  `purge ok not-installed`，`include/xxhash.h` 和 CMake 包目录均缺失。
  配置的 triplet 和 CMake prefix 本身一致，不应修改包名或搜索其他安装。
- 同一 v0.1.3 发布的原始 archive 仍在本地；SHA-256 与已有共享 resolve-release
  生成锁一致（`622117bdd82ade44a8421f924dbe235c78c5ba1c7f0d0f317447ab0299036189`）。
  原始包中存在 xxHash 的头文件、DLL/import library、Debug/Release 配置。
  事实是已恢复目录与发布 payload 不同；未追溯是哪次操作裁剪了目录。
- 复用该已解析锁和验证后的原始 blob，通过所选 vcpkg-cache 的共享 `locked`
  在本项目 `build/dependencies/vcpkg-cache-0.1.3-qigao-x64-windows-llvm-mingw-622117bdd82a/`
  恢复独立 payload，要求 xxhash 头文件及 `xxHashConfig.cmake` 存在。
  未覆盖原共享目录或安装另一个版本，未运行 vcpkg manifest install。
- 只更新本地 `.env` 的 `VCPKG_INSTALLED_DIR`，保留其他设置、凭据和换行；
  入口记录在 `build/vcpkg-cache.env`，回执在独立恢复目录。
- 配置继续前进后发现生成器路径引用未定义 `_turboraft_tbe_root_name`；
  改为事实源 `SALTS_UTILS_ROOT`。实际 fresh configure 现已通过，wire/Lemon
  生成已执行。后续 Core 编译缺少 `cmeta_error.h` 是独立的拆分 SDK 接口问题。
- 后续使用项目独立的已恢复 prefix，避免其他项目对共享 vcpkg installed tree
  运行安装/删除时影响这里的依赖。复用以锁、摘要和必需文件校验为准，不能只看回执。

## SHA-256：迁移到 salts-core 的 Crypto

- 触发：快照发送器编译找不到旧 `cmeta_crypto.h`。所选 SaltsCNet 0.1.12
  SDK 已导出 salts-core 的 `Salts::Crypto` 和 `<crypto/sha256.h>`；无需更换 SDK。
- 快照收发、数据流、WAL 的 SHA-256 调用统一使用 `salts_crypto_sha256*`，
  对应四个静态库显式私有链接 `Salts::Crypto`。安装配置在请求上述组件时也
  必须加载 SaltsCNet，因为静态库的私有链接依赖仍需在最终链接时解析。
- 新流式上下文由接收器内嵌或 WAL 调用栈独占持有，不复制已初始化状态，
  不再使用旧 create/destroy；每次新传输调用 init，完成时调用 final。
  `SALTS_CRYPTO_*` 与 Salts Core 错误码属于不同域，不能直接对外返回 Crypto
  失败值；初始化/单次摘要失败转为 `SALTS_EIO`，现有传输/摘要校验错误保留。
- 已加载 `.env` 中相关非密钥字段，执行
  `cmake --preset llvm-mingw-x64-release-user` 配置/生成通过；
  `cmake --build --preset llvm-mingw-x64-release-user --target turboraft_snapshot_sender turboraft_snapshot_receiver turboraft_data_stream`
  三个正式目标编译及静态库生成通过。
- 完整 `cmake --build --preset llvm-mingw-x64-release-user` 在
  `src/runtime/raft_multicore.c:3` 缺少 `ring_buffer.h` 处失败；单独构建
  `turboraft_wal_storage` 在 `raft_wal_storage_internal.h:6` 缺少
  `cmeta_fs.h` 处失败。这两处是独立的旧 SDK API 引用，本次未修改。
  当前 preset 的 `BUILD_TESTS=OFF`，CTest 未运行；不能据此宣称完整构建、
  WAL 编译或 SHA-256 运行时回归通过。

## 旧 Core 头文件和符号迁移（2026-11-17 后续）

- 以当前已选择的 SaltsBase 0.1.8、SaltsUtils 0.1.4、CMeta 0.1.7 和
  SaltsCNet 0.1.12 SDK 公开头文件为事实源，不复制缺失的旧头文件或添加兼容别名。
- 文件系统改用 `<file/fs.h>`、`fs_*`、`file_t`；线程同步改用
  `<threads/threads.h>` 和 `sync_*`；进程管理改用 `<core/process.h>` 和
  `process_*`；时钟、随机数、字符串和格式化头文件归 `core/`。
  队列、buffer、coroutine executor 分别使用 `<queue/ring_buffer.h>`、
  `<memory/buffer.h>`、`<salts/coro_executor.h>`。
- 源码、公开头文件、正式测试、示例和 benchmark 共 27 个文件同步迁移；
  62 个替换后的 Core 符号均能在所选 SDK 中找到。真正的 CMeta 元数据符号保留。
  `fs_replace_durable` 的发布状态与旧契约一致；`sync_thread_join` 仍消费线程句柄。
  Multicore 的两个队列始终在 owner mutex 下操作，未切换队列实现或并发模型。
- 加载相关非密钥 `.env` 字段后，
  `cmake --build --preset llvm-mingw-x64-release-user --target turboraft_multicore turboraft_wal_storage turboraft_cnet turboraft_cflow_state_machine turboraft_node_config turboraft_text_syntax`
  六个正式目标构建通过，前一节的 `ring_buffer.h` / `cmeta_fs.h` 阻塞已消除。
- 完整 `cmake --build --preset llvm-mingw-x64-release-user` 仍失败：当前
  CHttp 0.1.1 已移除 `crpc_server_config`、独立 CRPC server 和手工路由注册，
  现有 ControlPlane 尚未迁移到 Application/Controller；不能仅改 include 修复。
  该迁移涉及公开配置结构和 HTTP owner 借用接口，已按 AGENTS.md 请求用户确认。
- 使用既有 CHttp 选定目录执行
  `cmake --preset native-release-user -DChttp_DIR=<原选定目录>`，在
  `tests/core/CMakeLists.txt:177` 必需查找 TurboDB 时失败。未提供本次匹配的
  `TurboDB_DIR`/prefix，已向用户请求；未猜测替代安装、关闭测试或绕过 ORM fixture。
  CTest、示例及 benchmark 运行均未执行，不能宣称完整构建或运行时回归通过。


## Application、TurboDB 与依赖事实源（2026-11-17 后续）

- 用户已授权 ControlPlane 迁移和 TurboDB 0.1.1 接入，并要求遵守其最新修改：
  依赖由 vcpkg-cache 与已加载 `.env` 选择，不随意增加字段或恢复旧实现。
  CMake 使用既有 `CHTTP_ROOT` / `TURBODB_ROOT`；本轮增加的重复 `*_DIR`
  preset 配置已移除。平台 preset 的 triplet 改用父环境选择。
- 工具使用必需查找，删除消费方重复的路径、目标、哈希检查和旧 CMake policy。
  保留用户对安装配置的简化及对 `cmake/native_dependencies.py` 的删除。
  其旧 ops 测试和注册已删除，CI action 直接调用 vcpkg-cache 共享
  `resolve-release` / `download` / `locked`。YAML 和 PowerShell 语法解析通过；
  远程 CI 未运行。
- WAL 删除已废弃的 `TR_RAFT_WAL_MAX_SEGMENTS`、`max_segments` 别名及始终为空的
  recovery `snapshot_data`；调用点迁移至 `max_live_segments` / `snapshot_source`。
  恢复结构布局变化，使用方需重编译；磁盘 WAL 格式未变。
- ControlPlane 使用 Application、Controller 和生成的 DataBind Service 绑定，
  RPC ABI 为 request IN / response OUT / error OUT。移除旧 CRPC server 和
  HTTP engine accessor；生命周期及错误协议变化见 `docs/CONTROL_PLANE.md`。
- TurboDB 官方 v0.1.1 nupkg 摘要和大小核对通过，但其 Windows MSVC DLL 命名与
  当前 LLVM-MinGW SDK 不匹配。此前从发布精确提交
  `d04f5e918bee641bd4e119692dd19d2bd63de8b0` 构建完整 producer graph，通过其
  registry / plugin / coexistence 三项 ORM 测试后安装到独立目录。当前
  `.env` 的 `TURBODB_ROOT` 指向该源构建结果，不是官方 MSVC 资产；未改名 DLL
  或切换编译器。未继续扩展源码构建链，未采用本地 SaltsUtils 修补版 SDK。
- 验证：加载所需非密钥 `.env` 字段后，执行
  `cmake --fresh --preset native-release-user`、
  `cmake --build --preset native-release-user` 均通过。
  `ctest --preset native-release-user --output-on-failure` 为 60 项、57 通过、
  3 失败，耗时 88.18 秒。WAL、快照、数据流、Multicore 和进程 chaos 回归通过。
- HIGH：`turboraft.control_plane` 创建失败。所选 SaltsUtils 0.1.4 拒绝空
  `StatusRequest` 的根参数归属，诊断为 request 不属于 Service；生成的空 C
  record 大小也为零。需上游修正空 record 的存储和空根参数 admission。
  原始错误日志保留，未伪造请求字段或跳过绑定编译器。
- HIGH：`turboraft.cflow_orm_sqlite_recovery` / `turboraft.orm_sqlite_crash`
  仍失败，错误为 `load driver Plugin: load_failed`。Windows 安全加载诊断确认：
  仅 PATH 不能加载插件，先加载同一 `.env` vcpkg prefix 的 `libsqlite3.dll`
  后可安全加载。此实验未加入正式测试或生产代码；应解决上游包布局或明确的
  可信 DLL 目录策略，未放宽加载器策略。
- 本次创建的两个 detached worktree 已移除，原上游 checkout 和用户已有
  worktree 保留。示例、benchmark、其他平台与远程 CI 未验证；剩余两项的
  运行时迁移尚未完成，不能宣称全部通过。

## SaltsUtils 0.1.5 同步（2026-11-17）

- `.env` 只更新既有 `SALTS_UTILS_VERSION` / `SALTS_UTILS_ROOT`，
  `.env.example` 同步到 0.1.5。正式 LLVM-MinGW TAR 大小为 6533120 字节，
  SHA-256 为 `314ab3009235c1e9a15c62f460f4edc0aa66c47eab63658cb58ea5a6b91549de`。
  发布清单、SHA256SUMS、资产 API 与成功生产者 CI `44043062583` 已交叉核对，
  原始 SDK 恢复到带版本和摘要的独立目录，回执位于
  `build/dependencies/salts-utils-0.1.5-inputs/restore-receipt.json`。
- 上游 0.1.5 修正空 record 的 C 存储和 Service 根参数归属；
  ControlPlane 保留零 wire 字段的 `StatusRequest`。
- 保留用户的 `raft/` / `schema/` 布局。schema 子目录通过 OBJECT target
  持有生成源码、头文件和 include 目录，raft 链接该目标；不依赖兄弟目录
  中不可见的局部变量。同步修正 node schema、私有测试头及示例源码路径。
- HIGH：fresh configure 暴露旧 TurboDB SDK 精确依赖 SaltsUtils 0.1.4。
  TurboDB 本地依赖选择同步到 0.1.5 后，移除消费者的直接 GmSSL 查找又暴露
  SaltsCNet 0.1.12 的包导出缺口：`Salts::Crypto` 引用未加载的
  `GmSSL::GmSSL`。此外其公开 API 尚无 MySQL native password 所需的 SHA-1。
  必须在上游修复并取得匹配 SDK；未修改已校验 SDK 或恢复消费者的直接查找。
  本轮配置失败后未执行 build / CTest，不能将上轮测试结果视为本轮通过。

## 2026-11-18 TurboRaft 上游 SDK 同步

- Windows LLVM-MinGW 使用 SaltsCNet 0.1.16、SaltsUtils 0.1.6、CHttp 0.1.2。
  CHttp 发布清单选定 CMeta 0.1.8、SaltsBase 0.1.8 和 vcpkg-cache 0.1.3。
  GitHub 发布资产与本地 CHttp SDK 的 SHA-256 均为
  `2caded4adc931b57acb81a169a8743b3fc895f76e8e817daaaafde30b331b87c`。
  `.env` 使用原有版本和根目录字段，CMake 从指定根精确查找 CHttp。
- 测试依赖从 TurboDB 的 v0.1.2 标签 `1cf30023ddd6bb327e8b3fd0e84fecaae1a05550`
  构建。其 Studio 需要 Scintilla/Lexilla，TurboRaft 需要 xxHash。以本地选定的
  vcpkg-cache triplet 副本为基础，通过 vcpkg 安装该标签的两个 Studio overlay
  ports 后，两个工程使用同一个安装前缀。SQLite 同为 3.50.4 的不同构建曾产生
  不同 DLL；最终两个工程的 CMake cache 指向同一 package，测试宿主所用 DLL
  与该包的 SHA-256 相同。TurboDB 六项 SQLite 相关正式测试通过。
- CHttp 只接受一个 JSON-RPC result 投影。把七个状态字段放在同一个生成的
  record，再把该 record 投影为 result，保持空请求、原字段和类型化错误。
  Windows ORM 测试从当前 triplet 将 SQLite DLL 放在宿主可执行文件目录，
  供受限搜索路径的插件加载器使用。ControlPlane 和两个 ORM 测试均通过。
- `raft/` 迁移后的配置模板、schema 和 ABI 清单从项目根安装。生成的 wire
  和 control schema 对象分别包含在 Core 和 ControlPlane 静态库中。
- Windows 临时文件名会复用；遗留 WAL sidecar 可能导致全新用例 open 返回
  `SALTS_EPROTO`。各 WAL 用例现在使用单独的临时目录，并在 teardown 清理。
  `turboraft.wal_storage` 连续十次通过。原有共享临时目录中的文件已保留。
- 最终验证使用 `cmake --fresh --preset native-release-user`、完整 build、
  `ctest --preset native-release-user --output-on-failure` 和
  `cmake --build --preset install-native-release-user`。60/60 通过（90.19 秒），
  含多进程 chaos。原始日志在 `build/upstream-sync/`。
