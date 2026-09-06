# Configurable chaos seed range implementation plan

> **For Codex:** Required execution skill: `superpowers:executing-plans`. Follow this plan task by task with `superpowers:test-driven-development`.

**Goal:** Make the deterministic multi-process chaos test accept a bounded seed range, reject malformed or overflowing ranges before launching children, and print a copyable single-seed reproduction hint while preserving the default `1..4` campaign.

**Architecture:** Keep seed selection inside the existing chaos test executable because it owns the deterministic scheduler and is the sole consumer of the setting. Parse the two environment strings through a small pure helper so TinyTest can cover defaults and invalid boundaries without mutating process-global environment. Keep production Raft libraries, wire protocol, storage format, and public API unchanged.

**Tech stack:** C11, Salts error codes, TinyTest, CMake/CTest presets.

---

## Task 1: Specify the bounded range contract with failing TinyTest cases

**Files:**
- Modify: `tests/chaos/test_raft_multiprocess_chaos.c`

1. Replace the fixed-count name with explicit defaults and a hard campaign limit:

   ```c
   #define TR_CHAOS_DEFAULT_FIRST_SEED 1U
   #define TR_CHAOS_DEFAULT_SEED_COUNT 4U
   #define TR_CHAOS_MAX_SEED_COUNT 16U
   ```

2. Add a `tr_chaos_seed_range_t` test-side value type and TinyTest cases that call the wished-for pure parser:

   ```c
   typedef struct tr_chaos_seed_range {
       uint32_t first;
       uint32_t count;
   } tr_chaos_seed_range_t;

   check_equal(tr_chaos_parse_seed_range(NULL, NULL, &range), SALTS_OK);
   check_equal(range.first, UINT32_C(1));
   check_equal(range.count, UINT32_C(4));

   check_equal(tr_chaos_parse_seed_range("41", "2", &range), SALTS_OK);
   check_equal(range.first, UINT32_C(41));
   check_equal(range.count, UINT32_C(2));
   ```

3. Add separate behavior cases for malformed numeric text, zero/out-of-bound count, and an overflowing final seed. Hand-derived invalid inputs are `"0"`, `"1x"`, `"17"`, and first `"4294967295"` with count `"2"`.

4. Build only `turboraft_multiprocess_chaos_tests` through the `win-release-user` build preset and confirm compilation fails because `tr_chaos_parse_seed_range` does not exist yet. This is the required RED observation.

## Task 2: Implement fail-fast parsing and use it in the campaign

**Files:**
- Modify: `tests/chaos/test_raft_multiprocess_chaos.c`

1. Implement one strict decimal parser and one range parser. Accept only non-empty ASCII digits; reject signs, whitespace, trailing data, zero values, count values above 16, `strtoul` range failures, and `first + count - 1` overflow. Return `SALTS_EINVAL` for malformed/zero input and `SALTS_ERANGE` for numeric or interval overflow.

2. Read the two environment variables once in the chaos spec's `before_all()`
   preflight and cache the validated range:

   ```c
   tr_chaos_parse_seed_range(getenv("TURBORAFT_CHAOS_FIRST_SEED"),
                             getenv("TURBORAFT_CHAOS_SEED_COUNT"),
                             &range)
   ```

   Check the result before either child-process test creates a workspace or
   launches a process. Invalid configuration must fail the overall TinyTest
   spec while both process tests remain side-effect free.

3. Iterate by count rather than a `seed <= end` condition, deriving each seed from the already-validated range. Before every run print:

   ```text
   chaos seed=<n> started reproduce_env=TURBORAFT_CHAOS_FIRST_SEED=<n>,TURBORAFT_CHAOS_SEED_COUNT=1
   ```

   Retain the existing completion line and detailed failure-stage diagnostics.

4. Rebuild the focused target and run the parser-only TinyTest cases with a filter. Confirm all new cases pass (GREEN), then run the complete chaos executable through CTest and confirm the unchanged default still executes four seeds.

## Task 3: Document bounded campaigns and align the CTest budget

**Files:**
- Modify: `tests/core/CMakeLists.txt`
- Modify: `docs/CHAOS_TESTING.md`

1. Change only the existing chaos test `TIMEOUT` from 180 to 600 seconds. The 600-second budget provides more than 2x the observed 278.28-second linear projection for 16 Debug/ASan seeds. Do not add CMake helpers, fallback lookup, duplicated dependency configuration, or new targets.

2. Document the defaults, valid range, fail-fast behavior, PowerShell commands for a multi-seed range, and a single-seed reproduction. Explain that campaigns larger than 16 seeds must be split into bounded ranges and that the seed-start line is the reproduction source of truth.

3. Reconfigure with `cmake --fresh --preset win-release-user`, rebuild, and run the focused CTest entry with `--output-on-failure`.

## Task 4: Regression verification and delivery

**Files:**
- Verify all changed files and repository state.

1. Run a direct invalid-environment check with `TURBORAFT_CHAOS_SEED_COUNT=0`; confirm the executable exits non-zero before printing any `chaos seed=... started` line.

2. Run the full Windows Release test preset and confirm zero failures.

3. Configure/build and run the focused test under `win-dev-user` (MSVC ASan) using the documented preset entry.

4. Inspect `git diff --check`, `git diff --stat`, and `git status --short`. Confirm `.codegraph/` is not staged.

5. Request independent code review, address any validated findings with another RED/GREEN cycle, rerun affected verification, then commit, push, and open a PR linked to production-readiness issue `#18` without closing the umbrella issue.

## Compatibility and rollback

- Public API, ABI, Raft protocol, WAL format, and dependency graph do not change.
- With both environment variables absent, user-visible execution remains seeds `1..4`.
- Invalid configuration changes behavior from implicit fixed seeds to an explicit test failure; this is intentional fail-fast test-runner behavior.
- Rollback is a single test/docs/CMake commit and does not require data migration.
