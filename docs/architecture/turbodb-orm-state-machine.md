# TurboDB ORM database boundary

All TurboRaft application database access goes through **TurboDB ORM 2.3.1 or
newer**, linked as `Orm::C`. The integration boundary is SQL execution and
transactions. Driver selection, connection configuration and SQL dialects remain
inside TurboDB and the application bootstrap; TurboRaft does not maintain
backend-specific adapters or a database-server CI matrix. ORM does not supply
a schema-less document/key-value backend. Raft WAL files remain Raft consensus storage, separate from the
application database.

## Ownership and transaction protocol

Core owns consensus ordering; ApplyRuntime owns bounded committed entries and
exact application acknowledgement. CFlow owns deterministic state transitions.
The application owns its relational schema, `orm_runtime_t`, explicitly loaded
driver, connection and persistence worker. A database connection has one progress
owner at a time; a blocking database operation must not run on the CFlow serial
executor. Multiple Raft groups may use separate connection owners, but entries
within each group still apply in committed index order.

The application decodes each bounded command into typed domain data, then uses
one ORM transaction to persist:

1. the business rows;
2. the exact entry identity (index, term, command ID and payload);
3. the durable applied marker for that Raft group.

Only a proven successful transaction permits `APPLIED` settlement and Core's
applied-index advance. The journal's bounded binary identity column is part of
a relational schema; it is not a second unstructured storage API. Schema DDL and
migrations remain application-owned. Applications sharing a database across
groups must scope marker and journal keys by group ID.

Use `orm_transaction_begin`, query execution in that transaction, and
`orm_transaction_commit`. On a known pre-commit failure, roll back. An
`ORM_STATUS_COMMIT_UNKNOWN` result is not proof of rollback: stop application
progress, reconnect, and compare the durable marker and full identity with the
exact retained/WAL entry. A different identity is a conflict; the index alone
never authorizes acknowledgement. A confirmed absent transaction permits retry;
a confirmed committed identity permits acknowledgement without repeating the
business mutation. A rollback/cleanup failure also requires explicit recovery.

The repository's application-owned reference is
[`test_raft_cflow_orm_recovery.c`](../../tests/core/test_raft_cflow_orm_recovery.c).
It uses the public `orm_runtime_load_driver` / `orm_runtime_connect` APIs and
portable `?N` parameter binding from TurboDB 2.3.1. SQLite is the fixture's local
SQL implementation, not an additional database API in Raft. The fixture
uses signed SQL `BIGINT` indexes and rejects values above `INT64_MAX`; this is
not a change to Core's unsigned index domain.

## Why application-owned ORM

The former Redis journal adapter duplicated a database-specific persistence
path and exposed a separate public component. Retaining it would preserve a
non-SQL database boundary that this project no longer supports. Moving SQL or
schema management into Core would instead couple consensus to application data.
The chosen boundary keeps the existing generic state-machine SPI and provides
one SQL transaction/recovery contract through ORM.
There is no new universal SQL schema or parallel ORM wrapper in TurboRaft.

This removes `TurboRaft::TurboDbRedisStateMachine`, its public header/functions,
the `TURBORAFT_ENABLE_TURBODB_REDIS_STATE_MACHINE` switch and Redis CI/tests.
Callers migrate to the existing application SPI plus `Orm::C`; they own any
data migration. No existing Redis data is deleted or converted. Rollback means
using the prior TurboRaft release and its matching TurboDB deployment; there is
no runtime database fallback. Wire, WAL and snapshot formats are unchanged.

## Configuration and qualification

Set `TURBODB_ROOT` to the intended SDK. CMake requires version 2.3.1+, `Orm::C`
and the installed SQLite test driver. `TURBORAFT_ENABLE_ORM_SQLITE_FIXTURES`
enables the SQL recovery/crash tests. Missing requested SDKs or the test driver
fail explicitly.

Windows local validation, from a Visual Studio developer environment:

```powershell
cmake --preset win-orm-user
cmake --build --preset win-orm-user
ctest --preset win-orm-user
```

CI uses `ci-linux-orm-user`, builds the complete configured graph, and runs the
two ORM CTest entries. The recovery case checks rollback, commit-unknown
reconciliation, conflicting identity rejection and absence of duplicate effects.
The process-crash fixture verifies termination before and after commit. Tests
own temporary database files, create their own schema and remove it afterwards.
CTest supplies the SQLite driver path from the installed TurboDB SDK.

CI restores exactly `TurboDB.Native` 2.3.1, while Salts and SaltsUtils continue
to resolve their latest published stable SDKs. References:
[TurboDB 2.3.1 release](https://github.com/qigao/turbodb/releases/tag/v2.3.1),
[public ORM API](https://github.com/qigao/turbodb/blob/v2.3.1/orm/include/orm/orm.h),
[driver runtime](https://github.com/qigao/turbodb/blob/v2.3.1/orm/include/orm/orm_runtime.h).
