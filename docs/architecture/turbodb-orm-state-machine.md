# TurboDB ORM database boundary

All TurboRaft application database access goes through **the selected TurboDB ORM SDK**, linked as `Orm::C`. The integration boundary is SQL execution and
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
portable `?N` parameter binding. SQLite is the fixture's local
SQL implementation, not an additional database API in Raft. The fixture
uses signed SQL `BIGINT` indexes and rejects values above `INT64_MAX`; this is
not a change to Core's unsigned index domain.

## Why application-owned ORM

Putting SQL or schema management into Core would couple consensus to
application data. Keeping the generic state-machine SPI leaves domain schemas,
drivers and transactions with the application and provides one SQL
transaction/recovery contract through ORM. TurboRaft does not expose a separate
database-specific state-machine component or a universal SQL schema.

## Configuration and qualification

Expose the intended matching TurboDB SDK through `CMAKE_PREFIX_PATH` or
`TurboDB_DIR`. With `BUILD_TESTS=ON`, CMake requires the package, `Orm::C` and
the installed SQLite driver used by the formal
recovery/crash fixtures. The normal test graph includes both tests; there is no
ORM-specific feature switch or source profile. Dependency lookup, linking and
driver loading failures must be fixed at the selected SDK input.

Windows local validation, from a Visual Studio developer environment with the
[required SDK inputs](../../README.md#upstream-dependency-versions):

```powershell
cmake --fresh --preset msvc-release-user
cmake --build --preset msvc-release-user
ctest --preset msvc-release-user -R "^turboraft\.(cflow_orm_sqlite_recovery|orm_sqlite_crash)$" --output-on-failure
```

Linux Release uses `gcc-release-user`. The
[current CI workflow](../../.github/workflows/ci.yml) builds the complete graph
and runs the full CTest suite, including these entries. The recovery fixture
checks rollback, commit-unknown reconciliation, conflicting identity rejection
and absence of duplicate effects. The process-crash fixture checks termination
before and after commit. Tests own their temporary database files and schema;
CTest supplies the SQLite driver path from the installed TurboDB SDK.

The native CI matrix builds the selected TurboDB tag against the same Salts
SDKs as TurboRaft and supplies the installed SQLite driver to these tests.
See the [native dependency inputs](../SUPPORTED_PLATFORMS.md#native-dependency-inputs).
