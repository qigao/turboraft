# Pre-1.0 compatibility policy

TurboRaft is currently a pre-1.0 C SDK. This document defines what may change
before 1.0, what must already remain stable, and the objective criteria for
declaring a stable 1.0 API/ABI.

## Compatibility levels

TurboRaft uses three different compatibility classes. They are not
interchangeable.

### Durable and wire compatibility

Raft wire data, WAL records, snapshots, persisted membership/configuration,
backup metadata, and peer handshake contracts are durable facts. Pre-1.0 does
**not** permit silently reinterpreting an incompatible durable fact.

A change to one of these contracts must do one of the following:

- remain readable by the new implementation;
- carry an explicit version and migration path;
- reject the unsupported state before mutation; or
- document an explicit upgrade/rollback boundary.

A source-level API change never justifies silent durable-state corruption.

### v0.2.0 downgrade boundary

Rollback from current TurboRaft to the released v0.2.0 binary is conditional,
not unconditional. v0.2.0 predates the durable live-range manifest and discovers
WAL segments only inside its configured legacy scan window.

Before starting v0.2.0 on current durable state, operators must run:

```text
turboraft_compat_check downgrade \
  --target v0.2.0 \
  --path-prefix <raft-wal-prefix> \
  --legacy-max-segments <v0.2.0 max_segments> \
  --legacy-segment-bytes <v0.2.0 segment_bytes> \
  --legacy-max-transaction-bytes <v0.2.0 max_transaction_bytes> \
  --legacy-max-log-entries <v0.2.0 max_log_entries> \
  --legacy-max-snapshot-bytes <v0.2.0 max_snapshot_bytes>
```

The preflight is read-only. It does not open `tr_raft_wal_storage_t`, does not
take the WAL lock, and does not create, rewrite, truncate, rename, or delete any
manifest, WAL, snapshot, or lock file.

For target v0.2.0:

- `legacy-max-segments` must be in `1..65535` and must equal the deployed
  v0.2.0 replay setting;
- an authoritative current manifest range must fit entirely inside that legacy
  scan window;
- the old-visible segment namespace must be one contiguous range and must equal
  the manifest's authoritative range exactly; stale pre/post-range segment files
  reject rollback;
- every visible segment must use WAL format v1, carry the expected sequence and
  header checksum, and fit the deployed v0.2.0 `segment_bytes`;
- unknown required manifest or segment versions fail as unsupported;
- the other legacy replay limits are recorded in the machine-readable preflight
  result and are exercised by the real v0.2.0 verifier in hosted qualification.

A successful preflight is necessary but not sufficient evidence by itself: the
supported release procedure also requires the same v0.2.0 replay limits used by
qualification. Hosted compatibility CI invokes the real released v0.2.0
verifier only after a successful preflight.

Compatibility qualification deliberately uses **two dependency epochs**. The
real v0.2.0 producer/verifier runs with the released v0.2.0 dependency graph
(Salts.Native 1.8.3 / SaltsUtils.Native 4.1.3), while the current
producer/preflight runs with the current published Salts.Native /
SaltsUtils.Native graph. This proves that the durable/wire boundary does not
depend on reusing the old dependency epoch.

FlowMQ and CHttp are not part of the `storage-dev` WAL/snapshot parser used by
this downgrade preflight. They remain separately qualified by the current
package and live-peer gates. An in-place process rollback therefore means
restarting the v0.2.0 binary with its **own released dependency graph**; it does
not mean loading old and current FlowMQ/CHttp/Salts binaries into one process,
and no cross-version binary ABI promise is implied for those external
libraries.

If preflight rejects current state, directly starting v0.2.0 is outside the
supported rollback contract. Restore a backup created at a v0.2.0-compatible
boundary or rejoin the node from a healthy compatible peer instead.

### Public source API

Within one `0.y.z` minor line, patch releases should not intentionally break
public source API. Additive functions, constants, flags, targets, and
components are preferred.

A new `0.(y+1).0` minor release may make a source-breaking change before
1.0 when all of the following are true:

- the change is intentional and release-noted;
- an objective migration path is documented;
- durable/wire compatibility follows the stricter rule above;
- installed-package qualification is updated in the same change.

There is no promise that every 0.x public struct has a permanent binary layout
across minor releases.

### Stable 1.0 ABI

After a 1.0 baseline is declared, public C type layouts, calling conventions,
exported/linkable symbols, ownership rules, error semantics, installed CMake
targets, and component dependency contracts are part of the compatibility
surface.

## Public type policy

### Opaque handles are the preferred stateful ABI

Stateful objects should use incomplete public handle types and
create/destroy-style APIs. Current examples include:

- `tr_raft_core_t`
- `tr_raft_service_t`
- `tr_raft_runtime_t`
- `tr_raft_wal_storage_t`
- `tr_raft_transport_session_t`
- `tr_raft_cnet_peer_t`
- `tr_raft_flowmq_peer_service_t`
- snapshot sender/receiver/manager handles
- `tr_raft_control_plane_t`
- `tr_raft_wire_codec_t`

Implementation layout for an opaque handle is not public ABI.

### Caller-sized structs are fixed-size contracts

Many current config, status, message, callback-table, and plan types are
caller-allocated concrete structs. Examples include `tr_raft_core_config_t`,
`tr_raft_ready_t`, `tr_raft_status_t`, `tr_raft_storage_t`,
`tr_raft_transport_t`, snapshot source/sink structs,
transport config/status structs, and text parser plans.

For a concrete public struct that does **not** begin with an explicit
version/size contract:

- appending a field is not assumed ABI-compatible;
- reordering, resizing, or changing field type is ABI-breaking;
- adding a callback to a callback table changes its ABI;
- reserved padding may only be repurposed when its zero/default behavior was
  defined in advance;
- exact layout must be frozen before that type enters the 1.0 baseline, or the
  type must be migrated to an opaque/versioned design before 1.0.

`tr_raft_runtime_t` has been migrated to an opaque create/destroy handle.
Its implementation layout is no longer part of the public ABI.

### Version + size structs

A public struct may be tail-extensible only when the contract explicitly
contains version/size metadata and implementations validate the supplied size.
`tr_raft_control_audit_config_t` and its event format are the current model.

For such a type:

- existing field offsets never change;
- a caller with an older recognized size remains valid;
- newly added tail fields must have defined behavior when absent/zero;
- unknown required versions fail before mutation;
- tests must cover the oldest supported size and the current size.

A `size` field that is ignored by the implementation does not create ABI
compatibility by itself.

### Runtime migration

Runtime's former caller-sized `tr_raft_runtime_t` layout and
`tr_raft_runtime_init()` entry point are intentionally removed in the next
pre-1.0 minor line. Callers migrate from stack/static allocation to
`tr_raft_runtime_create()` / `tr_raft_runtime_destroy()`. Runtime continues
to borrow the configured Core and callback contexts; destroying Runtime does
not destroy those borrowed objects. The source break is intentional and does
not change wire, WAL, snapshot, Ready, storage, transport, or apply semantics.

## Enums, flags, and constants

- Existing public numeric values are never renumbered after the 1.0 baseline.
- Existing meanings are never reused for a different behavior.
- Additive enum values must define how older callers handle unknown values.
- Bit flags are preferred when independent additive capabilities are expected.
- Values serialized on wire or disk follow the durable/wire rules regardless
  of source-level enum policy.

## Callback and ownership contracts

Callback ownership is ABI behavior.

Every public callback contract must define:

- which thread/owner invokes it;
- whether input pointers are borrowed and for how long;
- whether the callback may retain data;
- whether the callback may re-enter TurboRaft;
- whether failure is retryable, permanent, or durability-uncertain;
- who releases any returned or transferred resource.

Changing one of these facts is a compatibility change even when the C
function signature is unchanged.

The existing single-owner Core/Service rule, Ready ownership, storage
transaction boundary, reliable-local-queue transport admission rule, and
snapshot source/sink lifetime rules are therefore compatibility surface.

The current public callback surface is also retained in machine-readable form
at `share/turboraft/abi/PUBLIC_CALLBACK_CONTRACTS.json` in packaged SDKs.
Callbacks are grouped into semantic families so repeated callback-table fields
share one owner/borrowing/re-entry/failure contract without duplicating prose.
Native SDK qualification rediscovers callback typedefs and function-pointer
fields from the **installed public headers** and requires exact manifest
coverage. A newly installed callback therefore fails qualification until it is
explicitly assigned a contract family.

This manifest records the current pre-1.0 surface. It is compatibility evidence,
not a claim that every current callback signature is already frozen as the 1.0
ABI.

## Error compatibility

TurboRaft returns Salts status codes. After the 1.0 baseline:

- a documented error condition must not silently become success;
- a permanent error must not silently become retryable, or vice versa;
- durability-unknown outcomes must remain distinguishable from
  not-published/not-committed outcomes;
- new error cases may be added only when callers can safely treat an unknown
  non-success as failure.

Exact internal failure stages may be extended additively when the public
status type allows it without changing an unversioned struct layout.

## Installed CMake/package contract

TurboRaft currently ships static library targets. Therefore ABI qualification
is not limited to shared-library exports. The installed contract includes:

- public header layouts and declarations;
- linkable C symbol names and signatures;
- static archive membership sufficient for supported consumers;
- transitive public link dependencies;
- installed target names such as `TurboRaft::Core` and
  `TurboRaft::Service`;
- component names accepted by `find_package(TurboRaft COMPONENTS ...)`;
- the rule that an optional component only requires its own external roots
  (for example, Core does not require `CHTTP_ROOT`).

Once a target/component enters the 1.0 baseline it is not silently renamed or
removed in a patch/minor release. An additive component is allowed.

## Symbol visibility and binary inventory

Every release-qualified packaged SDK captures a machine-readable public ABI
inventory at:

`share/turboraft/abi/public-abi.json`

The inventory contains:

- SHA-256 for every installed `include/turboraft/*.h` public header;
- public `tr_*` function declarations discovered from those headers;
- every public `typedef enum` and its current enumerator numeric values;
- all defined `tr_*` symbols found in installed static archives;
- the declared/defined intersection used as the current public-link symbol
  inventory;
- declared symbols without a link definition and archive symbols without a
  public declaration as diagnostics.

Linux inventory uses `nm -g --defined-only`. Windows inventory uses MSVC
`dumpbin /linkermember:1`. Enum values are parsed from the installed public
headers using a deliberately small constant-expression evaluator; unsupported
expressions fail the capture instead of being guessed. The native SDK pack gate
requires Linux and Windows public headers, declarations, enum numeric maps, and
public-link symbol inventories to match.

Because current targets are static libraries, archive-global implementation
symbols that are not declared in installed public headers are diagnostic
information, not automatically public ABI. A linkable public symbol is the
intersection of the installed declaration inventory and archive-defined
symbols.

This 0.x inventory is retained evidence, **not** a declaration that the current
surface is the permanent 1.0 baseline. When a 1.0 release candidate is
declared, the same comparison tool must be given the checked-in release
baseline; removal of a baseline public symbol or public enum value, or
renumbering of a baseline enum value, is then a release blocker. Additive enum
values remain possible where the enum contract permits unknown values.

If shared libraries are introduced later, their exported symbol table becomes
an additional gate, not a replacement for header/layout qualification.

## Test-only surfaces

Fault injection, sequence accelerators, and private I/O providers under
`src/` are test-only implementation seams.

They must not:

- be installed as public headers;
- appear in the stable target interface;
- become required to consume a packaged SDK;
- be treated as a supported production recovery API.

A test-only symbol accidentally entering the installed 1.0 surface is a
release blocker.

## CMake component policy

Before the 1.0 baseline, the component inventory must be reviewed explicitly.
For each installed component we record:

- target name;
- required public dependency roots;
- whether it is always present or profile/feature dependent;
- minimal installed consumer used as qualification evidence.

At minimum, existing qualification must continue to prove independent Core,
CFlowStateMachine, FlowMQ, and ControlPlane consumption where those components
are shipped.

## 1.0 promotion checklist

TurboRaft may declare a stable 1.0 C ABI only when all of the following are
true:

- [ ] every installed public header is classified as opaque, frozen-layout, or
      explicit version+size;
- [x] every current public callback is covered by the packaged callback
      contract manifest with owner, borrowing, retention, re-entry, failure,
      and release semantics;
- [x] `tr_raft_runtime_t` uses an opaque create/destroy handle;
- [x] current packaged SDKs capture public enum numeric inventories;
- [x] Linux and Windows packaged SDKs produce a retained symbol inventory;
- [ ] ABI/header comparison runs against a declared release-candidate baseline;
- [ ] installed CMake target/component inventory and minimal consumers are
      retained as qualification evidence;
- [ ] no private fault/test header is installed;
- [ ] wire/WAL/snapshot/configuration upgrade and rollback boundaries are
      qualified separately;
- [ ] release notes name the compatibility baseline and supported upgrade
      policy.

Until this checklist is complete, TurboRaft remains pre-1.0 and may make
explicitly documented minor-release API changes under this policy.
