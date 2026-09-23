# FlowMQ TLS identity binding

## Decision context

TLS authenticates a certificate chain, while a FlowMQ DEALER independently
chooses the HELLO identity used by a ROUTER. CA trust alone therefore does not
prove that a certificate is authorized to claim a particular Raft peer
identity. Accepting the HELLO first would let any client certificate trusted by
the cluster CA impersonate another configured peer.

The relevant facts have distinct owners:

- CNet owns the live TLS connection and its verified peer certificate.
- FlowMQ owns HELLO decoding and the ROUTER peer table.
- TurboRaft owns the configured relation between Raft node ID, FlowMQ identity,
  and acceptable deployment certificates.

## Options considered

1. Validate after `flowmq_recv()`. This is too late and cannot be correct because
   the receive envelope exposes the claimed routing identity but not the
   connection certificate.
2. Add a fingerprint field to the wire HELLO. This is self-reported data and can
   be forged; it would also require a wire protocol migration.
3. Validate the CNet certificate and HELLO tuple inside FlowMQ before storing the
   peer identity. This is the selected design because that boundary owns both
   authenticated inputs.

No fallback is provided. An invalid startup configuration fails before bind,
and an unauthorized live connection is closed before application data can be
delivered.

## Public configuration

Each `tr_raft_flowmq_peer_config_t` supplies:

- `identity`: the exact NUL-terminated HELLO identity expected from the peer;
- `client_certificate_sha256`: a borrowed array of canonical fingerprints;
- `client_certificate_sha256_count`: the number of array entries.

A canonical value contains `sha256:` followed by exactly 64 lowercase
hexadecimal characters. For example:

```c
#include <turboraft/raft_flowmq_peer_service.h>
#include <stdio.h>

static const char *const node_2_certificates[] = {
    "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    "sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"};

int main(void) {
  tr_raft_flowmq_peer_config_t node_2 = {0};

  node_2.node_id = 2;
  node_2.identity = "node-2";
  node_2.client_certificate_sha256 = node_2_certificates;
  node_2.client_certificate_sha256_count =
      sizeof(node_2_certificates) / sizeof(node_2_certificates[0]);
  node_2.endpoint = "tls://127.0.0.1:9002";
  printf("%s accepts %zu certificates\n", node_2.identity,
         node_2.client_certificate_sha256_count);
  return 0;
}
```

The example prints `node-2 accepts 2 certificates`. It leaves `handshake` and
outbound TLS credentials to application startup because they are
deployment-specific. Before calling
`tr_raft_flowmq_peer_service_create()`, the caller must set a completed
handshake, configure the listener CA/certificate/key, and set
`require_client_certificate = 1`.

For a TLS listener, every configured peer requires between one and
`TR_RAFT_FLOWMQ_MAX_CERTIFICATES_PER_PEER` fingerprints. Missing bindings or
disabled client authentication return `SALTS_EINVAL`; exceeding the per-peer
limit returns `SALTS_ERANGE`; malformed or conflicting bindings return the
FlowMQ policy validation error, normally `SALTS_EINVAL`. For a plaintext
listener, every fingerprint pointer/count pair must be `NULL`/zero or creation
returns `SALTS_EINVAL`.

All pointer and string inputs are borrowed only during service creation. The
adapter expands them into a bounded stack array and FlowMQ synchronously copies
the complete immutable policy before returning. No caller storage is retained.
The maximum expansion is
`(TR_RAFT_MAX_VOTERS - 1) * TR_RAFT_FLOWMQ_MAX_CERTIFICATES_PER_PEER`, currently
120 bindings. FlowMQ owns the copied map until the ROUTER closes.

## Runtime behavior and observability

On the owner thread, FlowMQ validates a peer in this order:

1. complete TLS chain, hostname/client-auth, and connection establishment;
2. decode and structurally validate HELLO;
3. query CNet for the verified certificate SHA-256 digest;
4. exact-match the canonical fingerprint and claimed identity;
5. only then store the identity and accept SETTINGS/DATA.

A failed tuple closes only that peer. It does not poison the ROUTER or set
TurboRaft's `last_error`. The saturating rejection count is exposed as
`tr_raft_flowmq_peer_service_status_t.tls_identity_rejections`; status reads do
not poll sockets or advance protocol state. `stop()` snapshots the counter
before closing the ROUTER, so the value remains available until service
destruction.

The match is O(number of configured certificate bindings) once per connection.
No lookup, allocation, copy, or branch is added to the DATA hot path. All state
remains single-owner; no mutex or worker is introduced.

## Certificate rotation

Rotation uses an explicit overlap window:

1. add the new fingerprint next to the old fingerprint for the same identity;
2. restart listeners so the immutable policy contains both values;
3. deploy the new peer certificate;
4. remove the old fingerprint and restart listeners again.

Hot reload is intentionally unsupported. A restart makes the policy generation
and lifetime unambiguous, while the two-entry overlap prevents an availability
gap.

## Compatibility, migration, and rollback

This change adds fields to public TurboRaft configuration and status structs.
TurboRaft 0.2.0 requires FlowMQ 1.1.0 or newer; the FlowMQ side is delivered by qigao/flowmq PR #11. TurboRaft is pre-1.0 and these
structs have no ABI size/version member, so all C consumers must recompile
against the new header. Existing plaintext
configurations remain source-compatible when structs are zero-initialized.
Existing TLS configurations must add per-peer fingerprints and enable required
client authentication; otherwise creation now fails fast.

There is no FMQ/6 wire-format or persistent-data change. Rollback therefore
requires only coordinated binaries/configuration: roll back TurboRaft consumers
first, then FlowMQ. WAL, snapshots, and Raft log contents need no migration.
