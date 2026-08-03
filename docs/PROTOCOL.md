# TurboRaft Peer Protocol Draft

Status: pre-implementation draft. Version 1 is not frozen until the Phase 1 codec tests pass.

## 1. Transport boundary

Production peers communicate over mutually authenticated TLS streams provided by CoroNet. TCP preserves byte order but not application message boundaries, so each message is framed. The decoder must support split headers, split payloads, and multiple frames in one receive buffer.

Peer RPC is independent from TurboHTTP JSON-RPC. This prevents HTTP parsing, retries, proxies, and public routing rules from becoming part of consensus safety.

## 2. Connection handshake

The first frame in each direction is `HELLO`.

`HELLO` carries:

- Wire major and minor version range.
- Cluster UUID.
- Sender node UUID.
- Sender process incarnation UUID.
- Config epoch known by the sender.
- Supported feature bits.
- Maximum accepted frame and snapshot-chunk sizes.

The receiver rejects cluster mismatch, zero/reused identity, unsupported major version, certificate identity mismatch, and size negotiation below protocol minimums. No Raft RPC is processed before successful handshake.

The process incarnation distinguishes a restarted peer from a delayed connection owned by an earlier process. It is not a Raft identity and is never stored in membership.

### Implemented handshake profile

The native CoroNet adapter performs handshake before constructing a connected
Raft session. Its V1 pre-session packet is a four-byte network-order length
followed by an 84-byte fixed record. The record carries magic and format
version, HELLO or HELLO_ACK type, wire major/minor ranges, feature bits, cluster
ID, node ID, process incarnation, configuration epoch, and accepted frame and
snapshot-chunk limits. Reserved bytes must be zero.

Both sides send HELLO, negotiate the highest common version and the minimum
resource limits, then exchange HELLO_ACK. An ACK must exactly match the
negotiated values and the identity from HELLO. The TLS-authenticated node ID is
an explicit input to negotiation and must equal the claimed HELLO node ID. No
connected `tr_raft_coronet_session_t` can be created without a completed
handshake result.

## 3. Frame header

All integers use network byte order. The fixed V1 header is 48 bytes.

| Offset | Size | Field | Rule |
|---:|---:|---|---|
| 0 | 4 | magic | ASCII `TRFT` |
| 4 | 2 | major_version | Must equal supported major version. |
| 6 | 2 | message_type | Known type or explicitly skippable extension. |
| 8 | 4 | flags | Unknown mandatory flags fail decoding. |
| 12 | 4 | header_length | At least 48 and bounded. |
| 16 | 8 | correlation_id | Non-zero for request/response pairs. |
| 24 | 8 | term | Sender term when applicable, otherwise zero. |
| 32 | 4 | payload_length | Checked against negotiated and local limits. |
| 36 | 4 | header_crc32c | Header checksum with this field zeroed. |
| 40 | 4 | payload_crc32c | Payload checksum, zero only for empty payload. |
| 44 | 4 | reserved | Must be zero in V1. |

Header CRC is validated before payload allocation. Payload CRC is validated before message decoding. A checksum failure closes the connection and increments a protocol-error metric; it is never passed to the core.

## 4. Message types

| Type | Direction | Purpose |
|---|---|---|
| `HELLO` | both | Identity and feature negotiation. |
| `HELLO_ACK` | both | Negotiated limits and acceptance. |
| `PRE_VOTE_REQ` | candidate to voters | Check whether an election could succeed without incrementing term. |
| `PRE_VOTE_RESP` | voter to candidate | Pre-vote outcome and current term. |
| `REQUEST_VOTE_REQ` | candidate to voters | Request a vote for a real term. |
| `REQUEST_VOTE_RESP` | voter to candidate | Vote outcome and current term. |
| `APPEND_REQ` | leader to follower | Heartbeat, log replication, and leader commit index. |
| `APPEND_RESP` | follower to leader | Match result and conflict hint. |
| `SNAPSHOT_BEGIN` | leader to follower | Snapshot metadata and transfer setup. |
| `SNAPSHOT_CHUNK` | leader to follower | Bounded ordered snapshot bytes. |
| `SNAPSHOT_ACK` | follower to leader | Durable progress or rejection. |
| `TIMEOUT_NOW` | leader to transferee | Request expedited leadership transfer. |
| `READ_INDEX_REQ` | follower to leader | Forward a linearizable read barrier. |
| `READ_INDEX_RESP` | leader to follower | Return safe commit index after quorum confirmation. |
| `ERROR` | both | Typed connection- or request-scoped rejection. |

## 5. Payload encoding rules

- Payloads are deterministic, length-delimited, and independently bounded.
- Repeated log entries carry an explicit count and total byte length.
- Every log entry carries index, term, type, and command length.
- Strings are not used for node IDs, terms, indices, or enum values.
- Unknown optional fields are skippable by length.
- Duplicate singular fields, integer overflow, invalid enum values, trailing bytes, and non-canonical encodings fail decoding.
- Decoders first validate structure and sizes, then allocate or copy owned data.
- Borrowed network receive memory does not survive a coroutine suspension or owner-loop post.

The Phase 0 codec spike must decide whether TurboUtils TBE can enforce these rules without leaking generator types into the public ABI. If it cannot, the repository will provide one reviewed codec implementation behind `tr_codec_v1_t`. Lemon is not a binary codec and is not a candidate.

## 6. Append request semantics

`APPEND_REQ` carries leader UUID, previous log index and term, leader commit index, and zero or more entries. Empty entries represent a heartbeat.

`APPEND_RESP` carries success, follower match index, follower durable index, and optional conflict term plus first index for that term. The leader may use the conflict hint to skip a mismatching term, but correctness cannot depend on that optimization.

A follower responds success only after required entries and hard-state changes are durable. The leader never advances a peer's match index beyond acknowledged durable data.

## 7. Snapshot transfer semantics

- Snapshot transfer is resumable at chunk boundaries within one process incarnation.
- `SNAPSHOT_BEGIN` identifies last included index/term, membership configuration, total bytes, chunk size, and whole snapshot digest.
- Each chunk identifies offset and length and carries its own frame CRC32C.
- The receiver writes to a private temporary file and reports progress only after the configured durable boundary.
- Completion requires exact size, whole digest verification, file sync, atomic rename, parent-directory sync, and state-machine restore.
- A newer term, newer snapshot, shutdown, or identity mismatch cancels the old transfer.
- Snapshot traffic has separate flow-control limits so heartbeats remain schedulable.

## 8. Correlation, retry, and duplicates

Correlation IDs are local transport identifiers generated from secure random bytes or a process-unique monotonic sequence with overflow handling. They are not log indices.

Raft RPCs are retryable and duplicate delivery is expected. The core decides message relevance from term, index, role, and peer progress. The transport never interprets a timeout as rejection or commitment.

## 9. Flow control

Each connection has configured limits for frame bytes, decoded message bytes, append entries, inflight requests, inflight bytes, receive backlog, and snapshot bytes. Exceeding a hard protocol limit rejects the frame. Exceeding a transient window applies backpressure and does not allocate an unbounded queue.

Priority order is:

1. Vote and term-changing responses.
2. Heartbeats and append acknowledgements.
3. Normal append payloads.
4. Snapshot chunks.

Priority does not permit reordering entries within one follower's log stream.

## 10. Protocol errors

Connection-fatal errors include invalid magic, version mismatch, cluster or certificate identity mismatch, malformed lengths, checksum failure, and forbidden flags.

Request-scoped errors include stale term, unknown peer under the current configuration, snapshot superseded, busy flow-control window, and unsupported optional feature.

Protocol errors carry stable numeric codes. Diagnostic text is bounded, optional, and never used for control flow.

## 11. Versioning

- Major version changes may alter framing or required semantics and cannot negotiate silently.
- Minor versions may add optional length-delimited fields or feature bits.
- A sender uses a feature only after handshake negotiation.
- WAL and snapshot versions do not derive from the wire version.
- Rolling upgrade tests cover every supported adjacent version pair.

## 12. Security limits

The decoder treats every peer frame as untrusted even after TLS authentication. It checks multiplication and addition before calculating entry-array and payload sizes. Compressed peer payloads are disabled in V1 to avoid decompression bombs and non-deterministic resource expansion.
