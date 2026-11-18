# Multi-process chaos testing

`turboraft.multiprocess_chaos` runs three independent TurboRaft child
processes. Each child is a Multi-Group host containing three independent
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

The seeded fault campaign focuses on G100. It covers:

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

The Multi-Group acceptance case exercises all three groups in the same three
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

The formal [multiprocess fixture](../tests/chaos/test_raft_multiprocess_chaos.c)
uses real child processes and the production Core, Service, WAL and wire codec.
It also checks a quiescent backup copied to a fresh process, rejection of an
incomplete copy, and restored-node catch-up while the remaining quorum
continues. Scenario coverage describes the fixture, not a passing run of the
current checkout.

## Running the real chaos test

After loading the required compiler and SDK inputs, build the complete graph
and select the test through CTest:

~~~powershell
cmake --fresh --preset msvc-release-user
cmake --build --preset msvc-release-user
ctest --preset msvc-release-user -R "^turboraft\.multiprocess_chaos$" --output-on-failure
~~~

With no additional environment configuration, the deterministic fault
campaign executes seeds 1 through 4. A bounded campaign can select a different
contiguous range:

~~~powershell
$env:TURBORAFT_CHAOS_FIRST_SEED = "17"
$env:TURBORAFT_CHAOS_SEED_COUNT = "8"
try {
  ctest --preset msvc-release-user -R "^turboraft\.multiprocess_chaos$" --output-on-failure
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
ctest --preset msvc-release-user -R "^turboraft\.multiprocess_chaos$" --output-on-failure
~~~

The deterministic parent-owned network does not replace the CNet adapter and
peer-handshake tests. This harness validates
Multi-Group process isolation, per-group durable recovery, and deterministic
cross-group fault isolation.

## Longer campaigns and evidence

There is no separate scheduled extended-chaos workflow. Run a bounded seed
range through the same CTest entry. The companion
`turboraft.snapshot_group_isolation` test exercises the bounded group queue and
snapshot sender/receiver, checking sibling-group progress between chunks.
Run it through the same configured preset:

~~~powershell
ctest --preset msvc-release-user -R "^turboraft\.snapshot_group_isolation$" --output-on-failure
~~~

Retain exact source and dependency revisions, seed range, runner identity,
configure/build output, CTest logs and reproduction commands with campaign
results. Neither a longer seed campaign nor the companion test replaces the
transport admission tests or a full CTest run. Physical TLS authentication must
be validated with the host's actual CNet connections and deployment policy.

### Failure-to-regression policy

A deterministic campaign failure is not considered resolved merely because a
later run passes.

1. Read the retained `chaos.log` failure line and capture its exact
   `seed`, `stage`, and `round` plus the campaign commit/dependency SHAs.
2. Reproduce only that seed with
   `TURBORAFT_CHAOS_FIRST_SEED=<seed>` and
   `TURBORAFT_CHAOS_SEED_COUNT=1`.
3. Reduce the failure to the narrowest stable contract test when practical.
   The resulting focused regression must run in normal hosted CI; retaining
   only the seed in a longer local campaign is insufficient.
4. Link the failure evidence and regression PR from the owning reliability
   issue before treating the seed as closed.
5. If the failure is in the companion snapshot scenario, preserve a focused
   `snapshot_group_isolation` (or narrower sender/receiver/scheduler)
   regression rather than inventing a seed for a non-seeded test.
