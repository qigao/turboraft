#include <turboraft/raft_cnet_identity.h>

#include <cmeta_error.h>
#include <tinytest.h>

#include <string.h>

/* SHA-256 fingerprints for the checked-in test-only certificates. */
#define CERT_NODE1 "eb571a92b33237897216c79501066b3e77391047eaeb6be084526c4769657549"
#define CERT_NODE2 "44e8fe3ce37ede1c2a1b5d36efa345cb662887d4250d17a000ebea2e834aed95"
#define CERT_ROTATED "d696b3ab8d0596e3e8e8abcecc4ca87856eda0f5055f31a7dfb6c213c2c2b1f3"

static tr_raft_cnet_identity_policy_t test_identity_policy(
    tr_raft_cnet_peer_identity_t peers[2],
    const char *node1[2],
    const char *node2[1])
{
    tr_raft_cnet_identity_policy_t policy;

    node1[0] = CERT_NODE1;
    node1[1] = CERT_ROTATED;
    node2[0] = CERT_NODE2;
    peers[0] = (tr_raft_cnet_peer_identity_t){1U, node1, 2U};
    peers[1] = (tr_raft_cnet_peer_identity_t){2U, node2, 1U};
    policy = (tr_raft_cnet_identity_policy_t){3U, peers, 2U};
    return policy;
}

spec("ACE 2.3 CNet verified TLS certificate identity policy")
{
    it("selects one exact node and supports explicit certificate rotation")
    {
        tr_raft_cnet_peer_identity_t peers[2];
        const char *node1[2];
        const char *node2[1];
        tr_raft_cnet_identity_policy_t policy =
            test_identity_policy(peers, node1, node2);
        tr_raft_node_id_t node_id = 99U;

        check_equal(tr_raft_cnet_identity_policy_validate(&policy), SALTS_OK);
        check_equal(tr_raft_cnet_identity_match_fingerprint(
            &policy, CERT_NODE1, &node_id), SALTS_OK);
        check_equal(node_id, UINT64_C(1));
        check_equal(tr_raft_cnet_identity_match_fingerprint(
            &policy, CERT_ROTATED, &node_id), SALTS_OK);
        check_equal(node_id, UINT64_C(1));
        check_equal(tr_raft_cnet_identity_match_fingerprint(
            &policy, CERT_NODE2, &node_id), SALTS_OK);
        check_equal(node_id, UINT64_C(2));
    }

    it("fails closed for unknown identity and invalid certificate text")
    {
        tr_raft_cnet_peer_identity_t peers[2];
        const char *node1[2];
        const char *node2[1];
        tr_raft_cnet_identity_policy_t policy =
            test_identity_policy(peers, node1, node2);
        tr_raft_node_id_t node_id = 99U;
        char unknown[65];
        char uppercase[65];

        memset(unknown, 'a', 64U);
        unknown[64] = '\0';
        memcpy(uppercase, CERT_NODE1, sizeof(uppercase));
        uppercase[0] = 'E';

        check_equal(tr_raft_cnet_identity_match_fingerprint(
            &policy, unknown, &node_id), SALTS_EPROTO);
        check_equal(node_id, UINT64_C(0));
        node_id = 99U;
        check_equal(tr_raft_cnet_identity_match_fingerprint(
            &policy, uppercase, &node_id), SALTS_EINVAL);
        check_equal(node_id, UINT64_C(0));
        check_equal(tr_raft_cnet_identity_match_fingerprint(
            &policy, "sha256:" CERT_NODE1, &node_id), SALTS_EINVAL);
        check_equal(tr_raft_cnet_identity_match_fingerprint(
            &policy, CERT_NODE1, NULL), SALTS_EINVAL);
    }

    it("rejects duplicate IDs and reused certificates before TLS admission")
    {
        tr_raft_cnet_peer_identity_t peers[2];
        const char *node1[2];
        const char *node2[1];
        tr_raft_cnet_identity_policy_t policy =
            test_identity_policy(peers, node1, node2);
        tr_raft_node_id_t node_id = 17U;

        peers[1].node_id = 1U;
        check_equal(tr_raft_cnet_identity_policy_validate(&policy), SALTS_EINVAL);
        peers[1].node_id = 2U;

        node2[0] = CERT_NODE1;
        check_equal(tr_raft_cnet_identity_policy_validate(&policy), SALTS_EINVAL);
        node2[0] = CERT_NODE2;

        node1[1] = CERT_NODE1;
        check_equal(tr_raft_cnet_identity_policy_validate(&policy), SALTS_EINVAL);
        node1[1] = CERT_ROTATED;

        peers[0].node_id = policy.local_node_id;
        check_equal(tr_raft_cnet_identity_policy_validate(&policy), SALTS_EINVAL);
        peers[0].node_id = 1U;

        policy.peer_count = TR_RAFT_MAX_MEMBERS;
        check_equal(tr_raft_cnet_identity_policy_validate(&policy), SALTS_EINVAL);
        policy.peer_count = 2U;

        policy.local_node_id = 0U;
        check_equal(tr_raft_cnet_identity_policy_validate(&policy), SALTS_EINVAL);
        policy.local_node_id = 3U;

        check_equal(tr_raft_cnet_identity_match_fingerprint(
            &policy, CERT_NODE1, &node_id), SALTS_OK);
        check_equal(node_id, UINT64_C(1));
    }

    it("never treats a caller-supplied fingerprint as a live TLS credential")
    {
        tr_raft_cnet_peer_identity_t peers[2];
        const char *node1[2];
        const char *node2[1];
        tr_raft_cnet_identity_policy_t policy =
            test_identity_policy(peers, node1, node2);
        tr_raft_node_id_t node_id = 11U;
        tr_raft_handshake_config_t local = {0};
        tr_raft_handshake_message_t hello = {0};
        tr_raft_handshake_message_t ack = {0};
        tr_raft_handshake_result_t result = {0};

        check_equal(tr_raft_cnet_identity_admit_tls(
            &policy, NULL, (cnet_connection){0}, &node_id), SALTS_EINVAL);
        check_equal(node_id, UINT64_C(0));
        local.local_node_id = 9U;
        check_equal(tr_raft_cnet_identity_negotiate_tls(
            &policy, NULL, (cnet_connection){0}, &local,
            &hello, &ack, &result), SALTS_EINVAL);
        check_equal(result.complete, 0);
    }
}
