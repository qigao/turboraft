# Multi-process chaos testing

`turboraft.multiprocess_chaos` runs three independent TurboRaft child
processes. Each child is now a Multi-Group host containing three independent
Raft groups:

~~~text
process node 1                 process node 2                 process node 3
  G100 Service + WAL             G100 Service + WAL             G100 Service + WAL
  G101 Service + WAL             G101 Service + WAL             G101 Service + WAL
  G102 Service + WAL             G102 Service + WAL             G102 Service + WAL
~~~

The group IDs used by the deterministic acceptance fixture are G100, G101, and
G102. Group identity lives in the simulated transport envelope; each
`tr_raft_service_t` remains group-local and does not know its hosting
`group_id`.

Each local group owns a distinct WAL prefix. A node base such as
`node-2.db` therefore produces independent durable facts under prefixes such
as:

~~~text
node-2.db.g100
node-2.db.g101
node-2.db.g102
~~~

The parent process owns the simulated network queue and routes bounded
stdin/stdout IPC frames by both physical node and Raft group. This keeps fault
decisions deterministic and independent of operating-system packet timing.

## Existing deterministic fault campaign

The historical seed campaign remains focused on G100 so prior safety evidence
is preserved. It covers:

- packet loss, duplication, delay, and reorder;
- a minority partition;
- termination of the observed leader;
- restart from the same per-group WAL prefix;
- online WAL backup handoff on an elected leader and follower;
- an injected WAL reopen failure after prepare/close;
- stable recovery periods before and after faults.

For every child response the runner checks term and commit monotonicity,
`applied <= commit <= last_log`, one leader per observed term, and identical
application hashes at equal applied indexes.

## Multi-Group acceptance scenario

The R0 Multi-Group acceptance case exercises all three groups in the same three
physical child processes.

The deterministic election-timeout fixture drives:

~~~text
G100 leader = node 1
G101 leader = node 2
G102 leader = node 3
~~~

The scenario then verifies:

1. all three groups independently propose, commit, apply, and converge;
2. G100 traffic can remain deliberately blocked in the shared simulated
   network while G101 and G102 continue to commit and converge;
3. a G100 WAL backup handoff on node 2 does not fault G101 or G102 hosted by
   the same process;
4. terminating and restarting physical node 2 reopens three independent WAL
   prefixes and all groups reconverge.

Focused CI also verifies the same scenario with three real OS child processes
and real TurboRaft Core, Service, and WAL implementations. That focused harness
uses a test-only group-aware wire shim to keep the executable independent from
the full DataBind/private-dependency closure. The production v6 group-aware
wire contract is verified separately by the #26 gate. The repository's final
`turboraft.multiprocess_chaos` CTest remains the authoritative combined test
with the real wire codec.

The focused three-process evidence is:

~~~text
run 35501109360
head c53bd888d088145f1585c4b2aa2fe509733fef9d
result SUCCESS
~~~

A separate combined snapshot/scheduler executable also streams G100 through
the same bounded per-peer group scheduler while G101/G102 protocol traffic is
scheduled between every snapshot chunk. The receiver completes digest
verification and commits the snapshot:

~~~text
run 35501381815
head b0882af6839145223965d16d318283d03fa2522f
result SUCCESS
~~~

## Running the real chaos test

Run the test with the normal preset:

~~~powershell
ctest --preset win-release-user -R turboraft.multiprocess_chaos --output-on-failure
~~~

With no additional environment configuration, the legacy deterministic fault
campaign executes seeds 1 through 4. A bounded campaign can select a different
contiguous range:

~~~powershell
$env:TURBORAFT_CHAOS_FIRST_SEED = "17"
$env:TURBORAFT_CHAOS_SEED_COUNT = "8"
try {
  ctest --preset win-release-user -R "^turboraft\.multiprocess_chaos$" --output-on-failure
} finally {
  Remove-Item Env:TURBORAFT_CHAOS_FIRST_SEED -ErrorAction SilentlyContinue
  Remove-Item Env:TURBORAFT_CHAOS_SEED_COUNT -ErrorAction SilentlyContinue
}
~~~

`TURBORAFT_CHAOS_FIRST_SEED` accepts decimal values from 1 through
`4294967295`. `TURBORAFT_CHAOS_SEED_COUNT` accepts 1 through 16, and the
final seed must remain within that same 32-bit range. Empty, signed,
non-decimal, zero, oversized, or overflowing values fail before the runner
creates a chaos workspace or starts child nodes.

The runner prints a `reproduce_env` line before each seed. Use those two
values to reproduce only that seed:

~~~powershell
$env:TURBORAFT_CHAOS_FIRST_SEED = "23"
$env:TURBORAFT_CHAOS_SEED_COUNT = "1"
ctest --preset win-release-user -R "^turboraft\.multiprocess_chaos$" --output-on-failure
~~~

## Extended reliability campaign

`.github/workflows/extended-reliability.yml` turns the bounded deterministic
fixture into a retained campaign rather than changing the fixture semantics.

The profiles are:

- pull-request validation: seeds 1 through 4 and one real
  `turboraft.flowmq_peer_service` pass;
- scheduled weekly qualification: 16 contiguous seeds plus four repeated
  real FlowMQ/mTLS peer-service passes;
- manual qualification: optional `first_seed`, `seed_count` (1..16), and
  `flowmq_repetitions` (1..16).

When a scheduled/manual first seed is not supplied, the workflow derives a
deterministic nonzero range from the workflow run number. The exact first/last
seed, count, commit SHA, run number, and a single-seed reproduction command are
written to `campaign-metadata.txt`.

The multi-process test already covers, per seed:

- packet loss/duplication/reorder/delay;
- partition and heal;
- leader process termination and restart from the same WAL;
- leader and follower online WAL backup handoff;
- durable election/log/application invariants and final convergence.

The repeated `turboraft.flowmq_peer_service` phase retains the production
FlowMQ/mTLS path, including the authenticated peer boundary and snapshot/data
transport scenarios, alongside the deterministic simulated-network campaign.

Every run uploads a 30-day `extended-reliability-<run_id>` artifact containing:

- campaign metadata;
- verbose multi-process chaos output;
- extracted `reproduce_env` / seed-stage-round lines;
- verbose FlowMQ peer-service output;
- CTest temporary diagnostics.

Both campaign phases are allowed to complete independently even if one fails;
the final evaluation fails the workflow if either phase failed. This preserves
evidence for both fault domains in the same run.

A failing deterministic seed should be promoted to a permanent regression by
first reproducing it with `TURBORAFT_CHAOS_SEED_COUNT=1`, then adding either a
focused regression or a documented retained seed to the normal bounded
qualification set. Do not increase the hard 16-seed cap merely to mask or
dilute a discovered failure.

The deterministic parent-owned network does not replace the CNet/FlowMQ mTLS
tests. Those tests validate the physical peer transport; this harness validates
Multi-Group process isolation, per-group durable recovery, and deterministic
cross-group fault isolation.
