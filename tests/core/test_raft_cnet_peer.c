#include <turboraft/raft_cnet_peer.h>

#include <salts_error.h>
#include <tinytest.h>

#include <string.h>

static int ignore_payload(void *context,
                          const tr_raft_transport_payload_t *payload)
{
    (void)context;
    (void)payload;
    return SALTS_OK;
}

static tr_raft_cnet_peer_t *make_peer(
    cnet_client *client,
    size_t total_capacity,
    size_t max_groups,
    size_t per_group_capacity)
{
    tr_raft_cnet_peer_config_t config;
    tr_raft_handshake_result_t handshake;
    tr_raft_cnet_peer_t *peer = NULL;
    size_t index;

    memset(&config, 0, sizeof(config));
    memset(&handshake, 0, sizeof(handshake));
    config.client = client;
    for (index = 0U; index < sizeof(config.transport.cluster_id.bytes);
         ++index) {
        config.transport.cluster_id.bytes[index] = (uint8_t)(31U + index);
    }
    config.transport.local_node_id = 1U;
    config.transport.peer_node_id = 2U;
    config.transport.first_outbound_message_id = 1U;
    handshake.complete = 1;
    handshake.cluster_id = config.transport.cluster_id;
    handshake.local_node_id = 1U;
    handshake.peer_node_id = 2U;
    handshake.peer_process_incarnation.bytes[0] = 1U;
    handshake.feature_bits = 0U;
    handshake.wire_major = TR_RAFT_HANDSHAKE_WIRE_MAJOR;
    handshake.wire_minor = TR_RAFT_HANDSHAKE_WIRE_MINOR;
    handshake.max_frame_size = TR_RAFT_WIRE_MAX_FRAME_SIZE;
    handshake.max_snapshot_chunk_size =
        TR_RAFT_WIRE_MAX_SNAPSHOT_CHUNK_BYTES;
    config.transport.handshake = &handshake;
    config.transport.on_payload = ignore_payload;
    config.outbound_limits.total_item_capacity = total_capacity;
    config.outbound_limits.total_data_bytes = 4096U;
    config.outbound_limits.max_active_groups = max_groups;
    config.outbound_limits.per_group_item_capacity = per_group_capacity;
    config.outbound_limits.per_group_data_bytes = 2048U;
    check_equal(tr_raft_cnet_peer_create(&config, &peer), SALTS_OK);
    return peer;
}

spec("Raft CNet peer adapter")
{
    it("exposes callbacks and a bounded grouped queue")
    {
        cnet_client client;
        cnet_observer observer;
        tr_raft_cnet_peer_t *peer;
        tr_raft_cnet_peer_status_t status;
        tr_raft_message_t message;

        memset(&client, 0, sizeof(client));
        memset(&message, 0, sizeof(message));
        peer = make_peer(&client, 4U, 2U, 2U);
        observer = tr_raft_cnet_peer_observer(peer);
        check_not_null(observer.on_state);
        check_not_null(observer.on_receive);
        check_not_null(observer.on_send);
        check(observer.user == peer);

        message.type = TR_RAFT_MSG_HEARTBEAT_REQUEST;
        message.from = 1U;
        message.to = 2U;
        message.term = 3U;
        check_equal(tr_raft_cnet_peer_enqueue_group(peer, 10U, &message),
                    SALTS_OK);
        message.term = 4U;
        check_equal(tr_raft_cnet_peer_enqueue_group(peer, 10U, &message),
                    SALTS_OK);
        message.term = 5U;
        check_equal(tr_raft_cnet_peer_enqueue_group(peer, 10U, &message),
                    SALTS_ENOSPC);
        check_equal(tr_raft_cnet_peer_enqueue_group(peer, 20U, &message),
                    SALTS_OK);
        check_equal(tr_raft_cnet_peer_enqueue_group(peer, 30U, &message),
                    SALTS_ENOSPC);
        check_equal(tr_raft_cnet_peer_enqueue_group(peer, 0U, &message),
                    SALTS_EINVAL);
        check_equal(tr_raft_cnet_peer_step(peer), SALTS_EBUSY);
        check_equal(tr_raft_cnet_peer_get_status(peer, &status), SALTS_OK);
        check_equal(status.queued_payload_count, 3U);
        check_equal(status.active_group_count, 2U);
        check_equal(status.outbound_limits.total_item_capacity, 4U);
        check_equal(status.outbound_limits.max_active_groups, 2U);
        check_equal(status.outbound_limits.per_group_item_capacity, 2U);
        {
            tr_raft_transport_group_queue_status_t group_status;

            check_equal(tr_raft_cnet_peer_get_group_status(
                            peer, 10U, &group_status),
                        SALTS_OK);
            check_equal(group_status.group_id, 10U);
            check_equal(group_status.queued_payload_count, 2U);
            check_equal(group_status.queued_data_bytes, 0U);
            check_equal(tr_raft_cnet_peer_get_group_status(
                            peer, 30U, &group_status),
                        SALTS_ENOENT);
        }
        check_equal(tr_raft_cnet_peer_stop(peer), SALTS_OK);
        check_equal(tr_raft_cnet_peer_enqueue_group(peer, 1U, &message),
                    SALTS_EPIPE);
        check_equal(tr_raft_cnet_peer_destroy(peer), SALTS_OK);
    }
}
