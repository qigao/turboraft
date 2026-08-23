# TurboRaft TurboSTL Natural API Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Finish TurboRaft's migration from legacy TurboUtils container/string aliases to the installed TurboSTL natural API, with explicit package dependencies and reproducible Windows validation.

**Architecture:** Keep TurboRaft's public API unchanged. Internal log and transport storage use raw `vec_t`/`deque_t` handles, one internal adapter maps `stl_status` into TurboUtils error codes, and each CMake target links the package that owns the symbols it calls.

**Tech Stack:** C11, TurboUtils Core/STL, TurboParser DataBind/TBE compiler, CMake Presets, Ninja, TinyTest/CTest.

**Spec:** `Agents.md` and the current user request to apply the TurboSTL natural API migration consistently.

## Global Constraints

- Preserve the existing 73 tracked modifications and untracked `src/turboraft_stl_status.h`.
- Do not change TurboRaft public protocol, persistence, ownership, capacity, or error behavior.
- Remove machine-specific `CMakeUserPresets.json` from version control and ignore future local copies.
- Use installed package targets; do not recreate compatibility aliases.
- Validate against the local TurboUtils natural-API install before declaring completion.

---

### Task 1: Establish the migration build baseline

**Files:**
- Read: `CMakePresets.json`
- Read: `CMakeUserPresets.json`
- Read: `CMakeLists.txt`

**Interfaces:**
- Consumes: installed `TurboUtilsConfig.cmake`, `TurboParserConfig.cmake`, and vcpkg dependencies.
- Produces: configure/build evidence for the current dirty migration.

- [ ] **Step 1: List configure, build, and test presets**

```powershell
cmake --list-presets
cmake --build --list-presets
ctest --list-presets
```

- [ ] **Step 2: Fresh-configure the current tree against the natural-API TurboUtils install**

```powershell
cmd /c "call ""C:\Program Files\Microsoft Visual Studio\2022\Professional\Common7\Tools\VsDevCmd.bat"" -arch=x64 -host_arch=x64 && cmake --fresh --preset win-release-user -DTURBO_UTILS_ROOT=C:/projects/cpp/turbonet/turbo-utils/.worktrees/turbostl-natural-api/build/Msvc-Release/install-test"
```

- [ ] **Step 3: Build the direct STL consumers and record any compile/link failure**

```powershell
cmake --build --preset win-release-user --target turboraft_core turboraft_coronet turboraft_flowmq
```

### Task 2: Make TurboSTL ownership explicit in CMake

**Files:**
- Modify: `CMakeLists.txt`

**Interfaces:**
- Consumes: `TurboUtils::STL` from the installed TurboUtils package.
- Produces: correct private/link-only dependencies for `turboraft_core`, `turboraft_coronet`, and `turboraft_flowmq`.

- [ ] **Step 1: Add `TurboUtils::STL` to each target that directly calls `vec_*` or `deque_*`**

```cmake
target_link_libraries(turboraft_core
  PUBLIC TurboUtils::Core
  PRIVATE TurboUtils::STL TurboParser::DataBind)
```

- [ ] **Step 2: Reconfigure and rebuild the three targets**

```powershell
cmake --preset release-win-msvc-ninja -DVCPKG_MANIFEST_MODE=OFF -DTURBO_UTILS_ROOT=C:/projects/cpp/turbonet/turbo-utils/.worktrees/turbostl-natural-api/build/Msvc-Release/install-test -DTURBOPARSER_ROOT=C:/projects/cpp/external/pkgs/turboparser -DTURBO_NET_ROOT=C:/projects/cpp/external/pkgs/turbonet -DTURBO_HTTP_ROOT=C:/projects/cpp/external/pkgs/turbohttp -DFLOWMQ_ROOT=C:/projects/cpp/external/pkgs/flowmq -DCMAKE_PREFIX_PATH=C:/projects/cpp/turbonet/turboraft/vcpkg_installed/x64-windows -DPKG_CONFIG_EXECUTABLE=C:/projects/cpp/turbonet/turboraft/vcpkg_installed/x64-windows/tools/pkgconf/pkgconf.exe
cmake --build --preset build-release-windows --target turboraft_core turboraft_coronet turboraft_flowmq
```

### Task 3: Remove tracked user presets and synchronize repository guidance

**Files:**
- Delete: `CMakeUserPresets.json`
- Modify: `.gitignore`
- Modify: `presets/ConfigurePresets.json`
- Modify: `Agents.md`
- Modify: `docs/NATIVE_CORE.md`

**Interfaces:**
- Consumes: shared release configure/build/test presets and environment/cache overrides for machine-specific paths.
- Produces: a repository-safe preset contract and current TurboSTL usage guidance.

- [ ] **Step 1: Expose `release-win-msvc-ninja` and `release-linux-ninja` as shared entry points**
- [ ] **Step 2: Ignore and delete `CMakeUserPresets.json`**
- [ ] **Step 3: Replace legacy container guidance with `<turbostl/typed.h>` and `Vec(Type, name)` / `Deque(Type, name)` declarations**
- [ ] **Step 4: Document `vec_t` ownership and the Core/STL target split**

### Task 4: Remove generated-file-only whitespace noise

**Files:**
- Modify: `src/wire/generated/turboraft_wire_tbe.c`
- Modify: `src/wire/generated/turboraft_wire_tbe.h`
- Modify: `src/wire/generated/turboraft_wire_v3_tbe.c`
- Modify: `src/wire/generated/turboraft_wire_v3_tbe.h`

**Interfaces:**
- Consumes: checked-in generated TBE sources.
- Produces: no semantic generated-code diff unrelated to the API migration.

- [ ] **Step 1: Remove only the added blank lines shown by `git diff -w`**
- [ ] **Step 2: Confirm `git diff --ignore-all-space` reports no generated-code changes**

### Task 5: Verify source, package, and tests

**Files:**
- Test: `tests/core/*`
- Test: `tests/text_syntax/*`
- Test: `tests/package/*`

**Interfaces:**
- Consumes: configured release build and installed TurboRaft/TurboUtils packages.
- Produces: build, CTest, install-consumer, legacy-audit, and diff-check evidence.

- [ ] **Step 1: Run legacy symbol audits and `git diff --check`**
- [ ] **Step 2: Build the complete Release tree**
- [ ] **Step 3: Run focused core/transport/text tests, then the full CTest preset**
- [ ] **Step 4: Install to an isolated build prefix and build `tests/package`**
- [ ] **Step 5: Review final status without committing or pushing unless explicitly requested**
