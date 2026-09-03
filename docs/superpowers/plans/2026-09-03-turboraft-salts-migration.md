# TurboRaft Salts migration plan

> **For Codex:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Move every first-party dependency and adapter to Salts, SaltsUtils,
CSTL, CNet, CHTTP/CRPC, and current FlowMQ packages.

**Architecture:** Keep consensus transport-neutral. Use caller-driven CNet and
FlowMQ adapters with bounded copied admission. Keep administration behind a
thread-safe status provider consumed by the CHTTP/CRPC server owner.

## Completed work

- [x] Exact Debug/Release package roots and package exports.
- [x] Core, WAL, schema generation, CSTL, DataBind, and TinyTest migration.
- [x] Transport-neutral framing and direct CNet peer adapter.
- [x] Caller-driven FlowMQ ROUTER/DEALER peer service.
- [x] Transport-neutral SnapshotManager integration.
- [x] CHTTP/CRPC status server and CHTTP console.
- [x] Obsolete public ABI, sources, tests, and documentation removed.
- [x] Release build and complete CTest suite verified.

## Final verification

- Search source, build metadata, tests, tools, and current documentation for
  retired package and API identifiers.
- Fresh-configure Debug and Release from their profile-specific package roots.
- Build, run all tests, install both profiles, and configure the installed
  package consumer.
- Measure relevant Release benchmarks before making throughput or latency
  claims.
