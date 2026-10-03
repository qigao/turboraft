# Performance SLO and regression policy

TurboRaft performance qualification separates **stable release guards** from
**hosted-runner evidence**. A metric is not promoted to a hard CI threshold
merely because one benchmark run produced a favorable number.

The retained evidence is produced by
`.github/workflows/durable-fsync-benchmark.yml` using the installed
`storage-dev` SDK and the production Salts filesystem path.

## Release-blocking guards

The following guards are intentionally normalized or protocol-derived. They
have remained materially more stable than absolute hosted latency.

### Durable batching efficiency

For the same benchmark execution, the direct WAL durability boundary must
amortize one fsync across bounded proposal batches:

| batch | minimum same-run speedup vs batch 1 |
| ---: | ---: |
| 4 | 2.0x |
| 8 | 4.0x |
| 16 | 8.0x |

The retained runs that motivated these limits show approximately 4x, 8x and
14x per-operation improvement for batches 4/8/16. The release guards retain a
large margin rather than encoding the observed best-case values.

This is a **relative same-run guard**. It detects loss of batching
amortization without assuming a fixed storage-device latency for a GitHub
hosted VM.

### Snapshot application-wire efficiency

For the fixed 4 MiB / 64 KiB-chunk catch-up fixture:

- total TurboRaft application-wire bytes must be identical across the five
  repeated executions;
- maximum wire amplification must be **<= 1.01x** payload bytes.

The retained baseline is 1.004433x, with exact total wire bytes repeated across
all five runs. The 1.01x ceiling leaves more than twice the observed protocol
overhead while still catching accidental framing or ACK amplification.

The byte count covers TurboRaft frames plus the four-byte transport length
prefix. It intentionally does not include TLS, TCP, IP or link-layer bytes.

## Retained evidence, not hard hosted thresholds

### Durable fsync latency

The workflow retains raw commit samples plus p50/p95/p99/max for batch sizes
1/4/8/16. Single-fsync center latency is useful operational evidence, but
absolute latency depends on the hosted storage/VM generation.

Across retained hosted runs, center and p95 moved materially and p99/max moved
even more. Therefore TurboRaft currently has **no absolute hosted-runner fsync
latency gate**. A release must retain the distribution artifact and provenance;
failure to produce the evidence is a gate, the absolute p95/p99 value is not.

### ReadIndex latency

The benchmark retains five independent executions for:

- idle ReadIndex;
- ReadIndex while one real durable proposal/fsync/commit is serialized on the
  same Service owner.

Within one job the distribution can be stable, while absolute values have
shifted substantially between hosted runner instances. TurboRaft therefore has
**no absolute hosted ReadIndex latency gate** and does not gate p99.

### Snapshot throughput and local storage resource cost

Snapshot store/install/catch-up throughput, WAL rotation/checkpoint cost,
CPU time, RSS, file-descriptor count and live-WAL bytes remain retained release
evidence. They are useful for trend analysis and incident comparison but are
not yet stable enough across hosted runner classes for absolute pass/fail
thresholds.

## Promotion rule for new hard thresholds

A new absolute CI threshold requires evidence that:

1. the timing/resource boundary is unambiguous and production-relevant;
2. repeated runs retain raw samples and exact dependency/environment
   provenance;
3. variance is characterized across multiple independent workflow runs, not
   only repetitions inside one VM;
4. the threshold has enough headroom to distinguish a product regression from
   normal runner variation;
5. a failure gives an actionable engineering response.

Until all five conditions hold, the metric remains evidence-only.

## Current policy summary

| Metric | Qualification |
| --- | --- |
| WAL batch 4/8/16 efficiency | hard same-run guard: >=2x / >=4x / >=8x |
| Snapshot application-wire amplification | hard protocol guard: <=1.01x |
| Snapshot repeated total wire bytes | hard deterministic guard: identical |
| Fsync p50/p95/p99/max | retained evidence only |
| ReadIndex idle/write-load latency | retained evidence only |
| Snapshot throughput | retained evidence only |
| Rotation/compaction latency | retained evidence only |
| CPU/RSS/FD/live-WAL resource metrics | retained evidence only |
