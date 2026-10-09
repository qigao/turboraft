#include <turboraft/raft_cnet_peer_directory.h>

#include <cmeta_error.h>
#include <salts/thread.h>

#include <string.h>

/* Pure CNet placement decision: no manager credit, queue admission, or
 * fallback. The same Node ID selects the same final Owner on every listener.
 * No dynamic pressure-based route change is allowed for a live Raft session. */
static int tr_directory_strict_owner(tr_raft_node_id_t node_id,
                                     uint32_t count,
                                     size_t *out_owner)
{
    cnet_owner_placement_hint eligible[TR_RAFT_MULTICORE_MAX_OWNERS] = {{0}};
    cnet_owner_placement_input input = {0};
    size_t i;

    if (count == 0U || count > TR_RAFT_MULTICORE_MAX_OWNERS ||
        node_id == 0U || out_owner == NULL)
        return SALTS_EINVAL;
    for (i = 0U; i < count; ++i)
        eligible[i].eligible = true;

    input.size = sizeof(input);
    input.version = CNET_OWNER_PLACEMENT_VERSION;
    input.kind = CNET_OWNER_PLACE_STRICT_KEY;
    input.owners = eligible;
    input.owner_count = count;
    input.key_known = true;
    input.key_hash = node_id;
    return cnet_owner_placement_choose(&input, out_owner);
}

static int tr_directory_peer_allowed(
    const tr_raft_cnet_identity_policy_t *identity,
    tr_raft_node_id_t node_id)
{
    size_t i;
    for (i = 0U; i < identity->peer_count; ++i) {
        if (identity->peers[i].node_id == node_id)
            return 1;
    }
    return 0;
}

static int tr_directory_owner(
    const tr_raft_cnet_peer_directory_t *directory)
{
    if (directory == NULL || !directory->active)
        return SALTS_EINVAL;
    return directory->owner_thread == cmeta_thread_current_token()
               ? SALTS_OK : SALTS_EPERM;
}

int tr_raft_cnet_peer_directory_init(
    tr_raft_cnet_peer_directory_t *directory,
    const tr_raft_cnet_peer_directory_config_t *config)
{
    size_t i, j;

    if (directory == NULL) return SALTS_EINVAL;
    if (directory->active) return SALTS_EALREADY;
    if (config == NULL ||
        config->version != TR_RAFT_CNET_PEER_DIRECTORY_VERSION ||
        config->local_node_id == 0U ||
        config->owner_count == 0U ||
        config->owner_count > TR_RAFT_MULTICORE_MAX_OWNERS ||
        config->owner_index >= config->owner_count ||
        config->identities == NULL ||
        config->identities->local_node_id != config->local_node_id ||
        tr_raft_cnet_identity_policy_validate(config->identities) != SALTS_OK ||
        config->entries == NULL || config->entry_count == 0U ||
        config->entry_count >= TR_RAFT_MAX_MEMBERS ||
        config->entry_count > config->identities->peer_count ||
        config->groups == NULL || config->group_count == 0U ||
        config->group_count > TR_RAFT_MULTICORE_MAX_GROUPS)
        return SALTS_EINVAL;

    for (i = 0U; i < config->group_count; ++i) {
        if (config->groups[i] == 0U) return SALTS_EINVAL;
        for (j = 0U; j < i; ++j)
            if (config->groups[i] == config->groups[j])
                return SALTS_EINVAL;
    }

    for (i = 0U; i < config->entry_count; ++i) {
        const tr_raft_cnet_peer_directory_entry_t *entry =
            &config->entries[i];
        tr_raft_cnet_managed_peer_status_t status = {0};
        size_t owner = SIZE_MAX;
        int result;

        if (entry->node_id == 0U ||
            entry->node_id == config->local_node_id ||
            entry->peer == NULL ||
            !tr_directory_peer_allowed(config->identities, entry->node_id))
            return SALTS_EINVAL;

        result = tr_directory_strict_owner(
            entry->node_id, config->owner_count, &owner);
        if (result != SALTS_OK || owner != config->owner_index)
            return SALTS_EINVAL;

        /* CNetManagedDial snapshot also verifies that these borrowed
         * ManagedPeers belong to the current CNet progress Owner. */
        result = tr_raft_cnet_managed_peer_get_status(entry->peer, &status);
        if (result != SALTS_OK) return result;
        if (status.peer_node_id != entry->node_id || status.stopped)
            return SALTS_EINVAL;
        for (j = 0U; j < i; ++j) {
            if (entry->node_id == config->entries[j].node_id ||
                entry->peer == config->entries[j].peer)
                return SALTS_EINVAL;
        }
    }

    directory->config = *config;
    directory->owner_thread = cmeta_thread_current_token();
    directory->active = 1;
    return SALTS_OK;
}

int tr_raft_cnet_peer_directory_lookup(
    tr_raft_cnet_peer_directory_t *directory,
    tr_raft_node_id_t node_id,
    tr_raft_cnet_managed_peer_t **out_peer)
{
    size_t i;
    int result;

    if (out_peer == NULL) return SALTS_EINVAL;
    *out_peer = NULL;
    result = tr_directory_owner(directory);
    if (result != SALTS_OK) return result;
    if (node_id == 0U || node_id == directory->config.local_node_id)
        return SALTS_EINVAL;

    for (i = 0U; i < directory->config.entry_count; ++i) {
        if (directory->config.entries[i].node_id == node_id) {
            *out_peer = directory->config.entries[i].peer;
            return SALTS_OK;
        }
    }
    /* A known but foreign-owner Node ID requires a real bounded handoff to
     * that Owner, never a local Manager fallback or borrowed peer pointer. */
    if (tr_directory_peer_allowed(directory->config.identities, node_id)) {
        size_t owner = SIZE_MAX;
        result = tr_directory_strict_owner(
            node_id, directory->config.owner_count, &owner);
        if (result != SALTS_OK) return result;
        if (owner != directory->config.owner_index) return SALTS_EPERM;
    }
    return SALTS_ENOENT;
}

static int tr_directory_payload_nodes(
    const tr_raft_transport_payload_t *payload,
    tr_raft_node_id_t *from,
    tr_raft_node_id_t *to)
{
    switch (payload->kind) {
    case TR_RAFT_WIRE_PAYLOAD_RAFT:
        *from = payload->data.raft.from;
        *to = payload->data.raft.to;
        return SALTS_OK;
    case TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_CHUNK:
        *from = payload->data.snapshot_chunk.from;
        *to = payload->data.snapshot_chunk.to;
        return SALTS_OK;
    case TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_ACK:
        *from = payload->data.snapshot_ack.from;
        *to = payload->data.snapshot_ack.to;
        return SALTS_OK;
    case TR_RAFT_WIRE_PAYLOAD_DATA_CHUNK:
        *from = payload->data.data_chunk.from;
        *to = payload->data.data_chunk.to;
        return SALTS_OK;
    case TR_RAFT_WIRE_PAYLOAD_DATA_ACK:
        *from = payload->data.data_ack.from;
        *to = payload->data.data_ack.to;
        return SALTS_OK;
    default:
        return SALTS_EPROTO;
    }
}

int tr_raft_cnet_peer_directory_send(
    tr_raft_cnet_peer_directory_t *directory,
    const tr_raft_transport_payload_t *payload)
{
    tr_raft_node_id_t from = 0U, to = 0U;
    tr_raft_cnet_managed_peer_t *peer = NULL;
    size_t i;
    int result;

    result = tr_directory_owner(directory);
    if (result != SALTS_OK) return result;
    if (payload == NULL || payload->group_id == 0U)
        return SALTS_EINVAL;

    result = tr_directory_payload_nodes(payload, &from, &to);
    if (result != SALTS_OK) return result;
    if (from != directory->config.local_node_id ||
        to == 0U || to == from)
        return SALTS_EPROTO;

    for (i = 0U; i < directory->config.group_count; ++i) {
        if (directory->config.groups[i] == payload->group_id)
            break;
    }
    if (i == directory->config.group_count)
        return SALTS_ENOENT;

    result = tr_raft_cnet_peer_directory_lookup(directory, to, &peer);
    if (result != SALTS_OK) return result;
    return tr_raft_cnet_managed_peer_send(peer, payload);
}

int tr_raft_cnet_peer_directory_receive(
    void *ingress_context, const tr_raft_transport_payload_t *payload)
{
    tr_raft_cnet_directory_ingress_t *ingress =
        (tr_raft_cnet_directory_ingress_t *)ingress_context;
    tr_raft_cnet_channel_status_t status = {0};
    tr_raft_node_id_t from = 0U;
    tr_raft_node_id_t to = 0U;
    tr_raft_cnet_managed_peer_t *authorized_peer = NULL;
    size_t i;
    int result;

    if (ingress == NULL || ingress->directory == NULL ||
        ingress->channel == NULL || ingress->on_payload == NULL ||
        payload == NULL || payload->group_id == 0U)
        return SALTS_EINVAL;

    result = tr_directory_owner(ingress->directory);
    if (result != SALTS_OK) return result;

    /* Only this live TLS channel's CNet-verified identity is authoritative.
     * An arbitrary remote HELLO or a raw caller-provided Node ID cannot
     * authorize dispatch. This is the exact same owner as CNet progress. */
    result = tr_raft_cnet_channel_get_status(ingress->channel, &status);
    if (result != SALTS_OK) return result;
    if (status.phase != TR_RAFT_CNET_CHANNEL_ACTIVE)
        return SALTS_EBUSY;
    if (status.authenticated_peer_node_id == 0U)
        return SALTS_EPROTO;

    result = tr_directory_payload_nodes(payload, &from, &to);
    if (result != SALTS_OK) return result;
    if (from != status.authenticated_peer_node_id ||
        to != ingress->directory->config.local_node_id)
        return SALTS_EPROTO;

    for (i = 0U; i < ingress->directory->config.group_count; ++i) {
        if (ingress->directory->config.groups[i] == payload->group_id)
            break;
    }
    if (i == ingress->directory->config.group_count)
        return SALTS_ENOENT;

    result = tr_raft_cnet_peer_directory_lookup(
        ingress->directory, from, &authorized_peer);
    if (result != SALTS_OK) return result;
    if (authorized_peer == NULL) return SALTS_ENOENT;

    /* Borrowed payload is valid during this callback only; do not mutate
     * another Raft group's Service outside its owner. Host explicitly
     * chooses a bounded mailbox adapter if CNet and Raft owners differ. */
    return ingress->on_payload(ingress->context, payload);
}

static int tr_directory_group_enqueue(void *context,
                                       const tr_raft_message_t *message)
{
    tr_raft_cnet_directory_group_binding_t *binding =
        (tr_raft_cnet_directory_group_binding_t *)context;
    tr_raft_transport_payload_t payload = {0};

    if (binding == NULL || binding->directory == NULL ||
        binding->group_id == 0U || message == NULL)
        return SALTS_EINVAL;

    payload.group_id = binding->group_id;
    payload.kind = TR_RAFT_WIRE_PAYLOAD_RAFT;
    payload.data.raft = *message;
    return tr_raft_cnet_peer_directory_send(binding->directory, &payload);
}

int tr_raft_cnet_peer_directory_transport_bind(
    tr_raft_cnet_directory_group_binding_t *binding,
    tr_raft_transport_t *transport)
{
    size_t i;
    int result;

    if (binding == NULL || binding->directory == NULL ||
        binding->group_id == 0U || transport == NULL)
        return SALTS_EINVAL;
    result = tr_directory_owner(binding->directory);
    if (result != SALTS_OK) return result;
    if (transport->enqueue != NULL)
        return SALTS_EALREADY;

    /* Fail before publishing a borrowed Service callback if the Group was
     * never admitted into this CNet Owner's immutable routing contract. */
    for (i = 0U; i < binding->directory->config.group_count; ++i)
        if (binding->directory->config.groups[i] == binding->group_id)
            break;
    if (i == binding->directory->config.group_count)
        return SALTS_ENOENT;

    transport->context = binding;
    transport->enqueue = tr_directory_group_enqueue;
    return SALTS_OK;
}

int tr_raft_cnet_peer_directory_destroy(
    tr_raft_cnet_peer_directory_t *directory)
{
    int result = tr_directory_owner(directory);
    if (result != SALTS_OK) return result;
    /* Borrowed table and peers; no owned runtime/network state is released.
     * The host must already have stopped every borrowing Raft Service. */
    memset(directory, 0, sizeof(*directory));
    return SALTS_OK;
}
