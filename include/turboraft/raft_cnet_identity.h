#ifndef TURBORAFT_RAFT_CNET_IDENTITY_H
#define TURBORAFT_RAFT_CNET_IDENTITY_H

#include <turboraft/raft_peer_handshake.h>
#include <cnet/cnet.h>

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* CNet exports a verified peer leaf certificate as 64 LOWERCASE hex bytes.
 * Policy declarations use the exact same canonical representation with no
 * "sha256:" prefix and one trailing NUL. The caller's tables are immutable
 * borrowed storage; no registry or dynamic provider lifetime is introduced. */
#define TR_RAFT_CNET_MAX_CERTIFICATES_PER_PEER 4U

typedef struct tr_raft_cnet_peer_identity {
    tr_raft_node_id_t node_id;
    const char *const *certificate_sha256;
    size_t certificate_count;
} tr_raft_cnet_peer_identity_t;

typedef struct tr_raft_cnet_identity_policy {
    tr_raft_node_id_t local_node_id;
    const tr_raft_cnet_peer_identity_t *peers;
    size_t peer_count;
} tr_raft_cnet_identity_policy_t;

/* Reject duplicate node IDs, noncanonical/duplicated fingerprints across all
 * nodes, unknown/zero local or peer IDs and over-capacity policy. This is a
 * PURE preflight; it neither loads credentials nor authenticates a connection.
 */
int tr_raft_cnet_identity_policy_validate(
    const tr_raft_cnet_identity_policy_t *policy);

/* Deterministic selection only: passing a fingerprint string here does NOT
 * authenticate any network connection. The pure match is for policy
 * validation, offline diagnostics and negative tests. */
int tr_raft_cnet_identity_match_fingerprint(
    const tr_raft_cnet_identity_policy_t *policy,
    const char *certificate_sha256,
    tr_raft_node_id_t *out_node_id);

/* Authorizes one live CNet TLS connection using CNet's verified peer leaf.
 * No plaintext, unauthenticated TLS or caller-claimed Node ID fallback.
 * Returns the underlying CNet error for non-TLS, incomplete or absent cert;
 * unknown/ambiguous identity returns SALTS_EPROTO. Output is zero on error.
 * Must run on the connection's single CNet owner while it is live.
 */
int tr_raft_cnet_identity_admit_tls(
    const tr_raft_cnet_identity_policy_t *policy,
    cnet_client *client,
    cnet_connection connection,
    tr_raft_node_id_t *out_node_id);

/* Authenticate THIS TLS connection before negotiating a HELLO that MUST have
 * been decoded from this same connection's receive stream by the caller.
 * The claimed Node ID is then matched by tr_raft_handshake_negotiate() against
 * CNet-authenticated identity; an untrusted HELLO cannot self-authorize.
 * No result is published on identity failure. No packet/epoch fallback.
 */
int tr_raft_cnet_identity_negotiate_tls(
    const tr_raft_cnet_identity_policy_t *policy,
    cnet_client *client,
    cnet_connection connection,
    const tr_raft_handshake_config_t *local,
    const tr_raft_handshake_message_t *remote_hello,
    tr_raft_handshake_message_t *out_local_ack,
    tr_raft_handshake_result_t *out_result);

#ifdef __cplusplus
}
#endif

#endif /* TURBORAFT_RAFT_CNET_IDENTITY_H */
