#include <turboraft/raft_cnet_identity.h>

#include <cmeta_error.h>

#include <string.h>

enum { TR_RAFT_CNET_CERT_SHA256_HEX_LENGTH = 64U };

static int tr_raft_cnet_cert_canonical(const char *digest)
{
    size_t i;

    if (digest == NULL) return 0;
    /* A policy entry is a caller-owned, NUL-terminated C string. CNet itself
     * produces exactly this lowercase SHA-256 representation. */
    for (i = 0U; i < TR_RAFT_CNET_CERT_SHA256_HEX_LENGTH; ++i) {
        const unsigned char c = (unsigned char)digest[i];
        if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')))
            return 0;
    }
    return digest[TR_RAFT_CNET_CERT_SHA256_HEX_LENGTH] == '\0';
}

int tr_raft_cnet_identity_policy_validate(
    const tr_raft_cnet_identity_policy_t *policy)
{
    size_t i;
    size_t j;
    size_t k;
    size_t n;

    if (policy == NULL || policy->local_node_id == 0U ||
        policy->peers == NULL || policy->peer_count == 0U ||
        policy->peer_count >= TR_RAFT_MAX_MEMBERS)
        return SALTS_EINVAL;

    for (i = 0U; i < policy->peer_count; ++i) {
        const tr_raft_cnet_peer_identity_t *peer = &policy->peers[i];
        if (peer->node_id == 0U || peer->node_id == policy->local_node_id ||
            peer->certificate_sha256 == NULL ||
            peer->certificate_count == 0U ||
            peer->certificate_count > TR_RAFT_CNET_MAX_CERTIFICATES_PER_PEER)
            return SALTS_EINVAL;

        for (j = 0U; j < peer->certificate_count; ++j) {
            const char *fingerprint = peer->certificate_sha256[j];
            if (!tr_raft_cnet_cert_canonical(fingerprint))
                return SALTS_EINVAL;

            /* One verified certificate may authorize exactly one node and
             * one entry, even across rotation lists and distinct peer IDs.
             * Reject the entire policy rather than picking a first match. */
            for (k = 0U; k <= i; ++k) {
                const tr_raft_cnet_peer_identity_t *earlier =
                    &policy->peers[k];
                size_t count = k == i ? j : earlier->certificate_count;
                if (k < i && earlier->node_id == peer->node_id)
                    return SALTS_EINVAL;
                for (n = 0U; n < count; ++n) {
                    if (memcmp(fingerprint, earlier->certificate_sha256[n],
                               TR_RAFT_CNET_CERT_SHA256_HEX_LENGTH) == 0)
                        return SALTS_EINVAL;
                }
            }
        }
    }
    return SALTS_OK;
}

int tr_raft_cnet_identity_match_fingerprint(
    const tr_raft_cnet_identity_policy_t *policy,
    const char *certificate_sha256,
    tr_raft_node_id_t *out_node_id)
{
    size_t i;
    size_t j;

    if (out_node_id == NULL) return SALTS_EINVAL;
    *out_node_id = 0U;
    if (!tr_raft_cnet_cert_canonical(certificate_sha256) ||
        tr_raft_cnet_identity_policy_validate(policy) != SALTS_OK)
        return SALTS_EINVAL;

    for (i = 0U; i < policy->peer_count; ++i) {
        const tr_raft_cnet_peer_identity_t *peer = &policy->peers[i];
        for (j = 0U; j < peer->certificate_count; ++j) {
            if (memcmp(peer->certificate_sha256[j], certificate_sha256,
                       TR_RAFT_CNET_CERT_SHA256_HEX_LENGTH) == 0) {
                *out_node_id = peer->node_id;
                return SALTS_OK;
            }
        }
    }
    return SALTS_EPROTO;
}

int tr_raft_cnet_identity_admit_tls(
    const tr_raft_cnet_identity_policy_t *policy,
    cnet_client *client,
    cnet_connection connection,
    tr_raft_node_id_t *out_node_id)
{
    char digest[CNET_TLS_PEER_CERTIFICATE_SHA256_CAPACITY] = {0};
    int result;

    if (out_node_id == NULL) return SALTS_EINVAL;
    *out_node_id = 0U;
    if (client == NULL ||
        tr_raft_cnet_identity_policy_validate(policy) != SALTS_OK)
        return SALTS_EINVAL;

    /* CNet is the ONLY authority for verified TLS credentials. A supplied
     * fingerprint and an unauthenticated peer HELLO are not authorization. */
    result = cnet_tls_peer_certificate_sha256(client, connection, digest);
    if (result != SALTS_OK) return result;

    return tr_raft_cnet_identity_match_fingerprint(
        policy, digest, out_node_id);
}

int tr_raft_cnet_identity_negotiate_tls(
    const tr_raft_cnet_identity_policy_t *policy,
    cnet_client *client,
    cnet_connection connection,
    const tr_raft_handshake_config_t *local,
    const tr_raft_handshake_message_t *remote_hello,
    tr_raft_handshake_message_t *out_local_ack,
    tr_raft_handshake_result_t *out_result)
{
    tr_raft_node_id_t authenticated_node_id = 0U;
    int result;

    if (out_local_ack == NULL || out_result == NULL) return SALTS_EINVAL;
    memset(out_local_ack, 0, sizeof(*out_local_ack));
    memset(out_result, 0, sizeof(*out_result));

    if (policy == NULL || local == NULL || remote_hello == NULL ||
        local->local_node_id != policy->local_node_id)
        return SALTS_EINVAL;

    result = tr_raft_cnet_identity_admit_tls(
        policy, client, connection, &authenticated_node_id);
    if (result != SALTS_OK) return result;

    /* Caller passes HELLO from this exact CNet connection's receive path.
     * The Raft negotiator rejects any claimed node/cluster mismatch and
     * owns all wire limits. We do not duplicate protocol negotiation here. */
    return tr_raft_handshake_negotiate(
        local, authenticated_node_id, remote_hello,
        out_local_ack, out_result);
}
