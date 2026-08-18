# Rolling upgrades and rollback

TurboRaft keeps handshake major `1`, minor `0`, and the fixed 84-byte record.
Wire behavior is selected only from the negotiated feature intersection.

## Capability matrix

| Capability | Feature bit | Negotiated behavior |
| --- | ---: | --- |
| Snapshot configuration | `1 << 0` | Required by every accepted peer |
| Raft batch v3 | `1 << 1` | Raft frames use v3 and may carry up to `TR_RAFT_MAX_APPEND_ENTRIES` entries |
| Snapshot v4 | `1 << 2` | Snapshot chunk and acknowledgement frames use v4 |

A peer advertising only bit 0 is a legacy peer. Its Raft frames use v2,
AppendEntries batches are emitted as ordered single-entry frames, and snapshot
transfer is rejected with `TURBO_EPROTONOSUPPORT`.

## Compatibility matrix

`Legacy` below means a peer that advertises only snapshot ConfState support. It
does not name a product release; release engineering must map each shipped
binary to its advertised feature bits.

| Boundary | Legacy with current | Current with current | Rollback condition |
| --- | --- | --- | --- |
| Handshake | Major 1/minor 0 accepted | Major 1/minor 0 accepted | Both peers must retain the fixed handshake format |
| Raft messages | Negotiates v2; batches split into ordered single-entry frames | Negotiates v3; bounded batches enabled | Retain v2 support and uncompacted log history |
| Snapshot transfer | Rejected before transfer | v4 chunk and acknowledgement frames | A legacy node that needs a snapshot must be upgraded again |
| WAL storage | Exact local format version required | Matching WAL and snapshot files open directly | Restore a backup written by the target binary version |
| RPC control plane | Administrative HTTP/JSON-RPC is outside peer negotiation | Same method compatibility aliases remain available | Rollback must preserve the deployed RPC client contract |

The executable evidence for the peer rows is `turboraft.wire_upgrade`. Storage
recovery evidence is in `turboraft.wal_storage`. A version pair is not
declared rolling-upgrade compatible unless both tests cover its advertised
feature and storage boundaries.

## Upgrade order

1. Deploy binaries that understand all three bits but continue accepting legacy peers.
2. Upgrade followers before leaders so a current leader can still send v2 to remaining legacy followers.
3. Confirm every active session negotiated bits 1 and 2 before relying on batched replication or snapshot transfer.
4. Upgrade leaders last.

## Rollback boundary

Rollback is safe while the cluster still permits legacy sessions. A rolled-back
node negotiates v2 and remains able to receive ordered log entries. It cannot
receive snapshots from a current node, so retain enough uncompacted log history
to cover the rollback window. If a lagging rolled-back node requires a snapshot,
upgrade it again before rejoining.

Inbound sessions reject a frame whose version differs from the negotiated
capability. This prevents an on-path or misconfigured peer from silently
downgrading v3 replication or injecting snapshot payloads into a legacy session.

WAL format compatibility is a separate rollback boundary from peer wire
negotiation. Take a consistent backup before starting a binary with a new local
format. Do not edit WAL headers to force a downgrade. Follow
[`RECOVERY.md`](RECOVERY.md) for backup and restore procedures.
