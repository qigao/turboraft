# WAL backup and recovery

TurboRaft keeps one durable fact source per Raft group:

- `<prefix>.NNNNNNNN.wal` contains ordered Raft transactions;
- `<prefix>.snapshot.<index>.<term>` contains an opaque FSM snapshot;
- `<prefix>.lock` prevents concurrent writers.

## Backup

Quiesce and close the storage instance before copying files. Copy every WAL
segment and every snapshot file sharing the prefix as one backup set. Record
the TurboRaft version, node ID, cluster ID, and backup time beside that set.

Copying an active prefix is unsupported: a snapshot file may have reached disk
before its referencing WAL transaction, or the final WAL frame may still be in
flight. For online backup, first coordinate a Service pause and storage close
at the owner-loop boundary.

## Startup validation

`tr_raft_wal_storage_open()` requires consecutive segment numbers and
transaction predecessor IDs. It validates segment headers, frame sizes,
transaction checksums, Raft index/term invariants, snapshot metadata, and the
snapshot payload checksum.

A partial final transaction is an uncommitted torn tail and is truncated.
Checksum mismatch, missing committed snapshot data, sequence gaps, or invalid
Raft state return `SALTS_EPROTO`; I/O failures return `SALTS_EIO`. Recovery
never skips a damaged committed record.

## FSM recovery order

1. Open and replay WAL storage.
2. If a snapshot exists, pass its opaque bytes to the application `restore`
   callback.
3. Create Core with the snapshot index/term as the compacted log base.
4. Load the contiguous suffix after that base.
5. Apply committed suffix entries in index order.

An FSM restore failure faults startup. Do not start from an empty FSM or apply
suffix entries on top of an unverified state.

## Restore or rebuild

Restore a complete matching backup set while the node is stopped. If no valid
backup exists, remove the failed node's local files only after removing or
isolating that node operationally, then let it rejoin from a healthy cluster
member through snapshot installation. Never copy another live node's local WAL
as a substitute for the Raft snapshot protocol.
