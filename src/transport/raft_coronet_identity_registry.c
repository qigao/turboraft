#include <turboraft/raft_coronet_transport.h>

#include <turbo_error.h>

#include <stdlib.h>
#include <string.h>

struct tr_raft_coronet_identity_registry {
    size_t entry_count;
    tr_raft_coronet_identity_entry_t entries[];
};

static int tr_raft_coronet_fingerprint_validate(const char *fingerprint)
{
    static const char prefix[] = "sha256:";
    size_t index;

    if (fingerprint == NULL ||
        memcmp(fingerprint, prefix, sizeof(prefix) - 1U) != 0) {
        return TURBO_EPROTO;
    }
    for (index = sizeof(prefix) - 1U;
         index < CORO_TLS_PEER_CERT_SHA256_CAPACITY - 1U; ++index) {
        char value = fingerprint[index];
        if (!((value >= '0' && value <= '9') ||
              (value >= 'a' && value <= 'f'))) {
            return TURBO_EPROTO;
        }
    }
    return fingerprint[CORO_TLS_PEER_CERT_SHA256_CAPACITY - 1U] == '\0'
               ? TURBO_OK
               : TURBO_EPROTO;
}

static int tr_raft_coronet_identity_entry_compare(const void *left,
                                                   const void *right)
{
    const tr_raft_coronet_identity_entry_t *left_entry =
        (const tr_raft_coronet_identity_entry_t *) left;
    const tr_raft_coronet_identity_entry_t *right_entry =
        (const tr_raft_coronet_identity_entry_t *) right;

    return strcmp(left_entry->certificate_sha256,
                  right_entry->certificate_sha256);
}

int tr_raft_coronet_identity_registry_create(
    const tr_raft_coronet_identity_entry_t *entries,
    size_t entry_count,
    tr_raft_coronet_identity_registry_t **out_registry)
{
    tr_raft_coronet_identity_registry_t *registry;
    size_t allocation_size;
    size_t index;

    if (out_registry == NULL) {
        return TURBO_EINVAL;
    }
    *out_registry = NULL;
    if (entries == NULL || entry_count == 0U ||
        entry_count > TR_RAFT_CORONET_MAX_CERT_IDENTITIES) {
        return TURBO_EINVAL;
    }
    for (index = 0U; index < entry_count; ++index) {
        if (entries[index].node_id == 0U ||
            tr_raft_coronet_fingerprint_validate(
                entries[index].certificate_sha256) != TURBO_OK) {
            return TURBO_EINVAL;
        }
    }
    if (entry_count >
        (SIZE_MAX - sizeof(*registry)) / sizeof(registry->entries[0])) {
        return TURBO_EINVAL;
    }

    allocation_size = sizeof(*registry) +
                      entry_count * sizeof(registry->entries[0]);
    registry = (tr_raft_coronet_identity_registry_t *) calloc(
        1U, allocation_size);
    if (registry == NULL) {
        return TURBO_ENOMEM;
    }
    registry->entry_count = entry_count;
    memcpy(registry->entries, entries,
           entry_count * sizeof(registry->entries[0]));
    qsort(registry->entries, registry->entry_count,
          sizeof(registry->entries[0]),
          tr_raft_coronet_identity_entry_compare);
    for (index = 1U; index < registry->entry_count; ++index) {
        if (strcmp(registry->entries[index - 1U].certificate_sha256,
                   registry->entries[index].certificate_sha256) == 0) {
            free(registry);
            return TURBO_EINVAL;
        }
    }

    *out_registry = registry;
    return TURBO_OK;
}

void tr_raft_coronet_identity_registry_destroy(
    tr_raft_coronet_identity_registry_t *registry)
{
    free(registry);
}

int tr_raft_coronet_identity_registry_resolve(
    void *context,
    const char *verified_certificate_sha256,
    tr_raft_node_id_t *out_peer_node_id)
{
    const tr_raft_coronet_identity_registry_t *registry =
        (const tr_raft_coronet_identity_registry_t *) context;
    size_t left = 0U;
    size_t right;

    if (out_peer_node_id == NULL) {
        return TURBO_EINVAL;
    }
    *out_peer_node_id = 0U;
    if (registry == NULL ||
        tr_raft_coronet_fingerprint_validate(
            verified_certificate_sha256) != TURBO_OK) {
        return TURBO_EPROTO;
    }

    right = registry->entry_count;
    while (left < right) {
        size_t middle = left + (right - left) / 2U;
        int comparison = strcmp(
            verified_certificate_sha256,
            registry->entries[middle].certificate_sha256);

        if (comparison == 0) {
            *out_peer_node_id = registry->entries[middle].node_id;
            return TURBO_OK;
        }
        if (comparison < 0) {
            right = middle;
        } else {
            left = middle + 1U;
        }
    }
    return TURBO_EPERM;
}
