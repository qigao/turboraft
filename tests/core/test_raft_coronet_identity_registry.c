#include <turboraft/raft_coronet_transport.h>

#include <tinytest.h>
#include <turbo_error.h>

#include <string.h>

static tr_raft_coronet_identity_entry_t identity_entry(char hex_digit,
                                                       tr_raft_node_id_t node_id)
{
    tr_raft_coronet_identity_entry_t entry;

    memset(&entry, 0, sizeof(entry));
    memcpy(entry.certificate_sha256, "sha256:", 7U);
    memset(entry.certificate_sha256 + 7U, hex_digit, 64U);
    entry.node_id = node_id;
    return entry;
}

spec("raft CoroNet certificate identity registry")
{
    it("copies, sorts, and resolves certificate rotation entries")
    {
        tr_raft_coronet_identity_entry_t entries[3];
        tr_raft_coronet_identity_registry_t *registry = NULL;
        tr_raft_node_id_t node_id = 0U;
        char second_fingerprint[CORO_TLS_PEER_CERT_SHA256_CAPACITY];

        entries[0] = identity_entry('b', 2U);
        entries[1] = identity_entry('a', 1U);
        entries[2] = identity_entry('c', 2U);
        memcpy(second_fingerprint, entries[0].certificate_sha256,
               sizeof(second_fingerprint));
        check_int_eq(tr_raft_coronet_identity_registry_create(
                         entries, 3U, &registry),
                     TURBO_OK);

        memset(entries, 0, sizeof(entries));
        check_int_eq(tr_raft_coronet_identity_registry_resolve(
                         registry, second_fingerprint, &node_id),
                     TURBO_OK);
        check_int_eq(node_id, 2);
        check_int_eq(tr_raft_coronet_identity_registry_resolve(
                         registry,
                         "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
                         &node_id),
                     TURBO_OK);
        check_int_eq(node_id, 2);
        tr_raft_coronet_identity_registry_destroy(registry);
    }

    it("rejects malformed, duplicate, and unknown identities")
    {
        tr_raft_coronet_identity_entry_t entries[2];
        tr_raft_coronet_identity_registry_t *registry = NULL;
        tr_raft_node_id_t node_id = 99U;

        entries[0] = identity_entry('a', 1U);
        entries[1] = identity_entry('a', 2U);
        check_int_eq(tr_raft_coronet_identity_registry_create(
                         entries, 2U, &registry),
                     TURBO_EINVAL);
        check_null(registry);

        entries[1] = identity_entry('b', 2U);
        check_int_eq(tr_raft_coronet_identity_registry_create(
                         entries, 2U, &registry),
                     TURBO_OK);
        check_int_eq(tr_raft_coronet_identity_registry_resolve(
                         registry,
                         "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
                         &node_id),
                     TURBO_EPERM);
        check_int_eq(node_id, 0);
        check_int_eq(tr_raft_coronet_identity_registry_resolve(
                         registry,
                         "sha256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                         &node_id),
                     TURBO_EPROTO);
        check_int_eq(node_id, 0);
        tr_raft_coronet_identity_registry_destroy(registry);
    }
}
