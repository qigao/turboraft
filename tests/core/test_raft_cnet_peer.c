#include <turboraft/raft_cnet_peer.h>

#include <salts_error.h>
#include <tinytest.h>

#include <string.h>

static int ignore_message(void *context, const tr_raft_message_t *message)
{
    (void)context;
    (void)message;
    return SALTS_OK;
}

static tr_raft_cnet_peer_t *make_peer(cnet_client *client, size_t capacity)
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
    handshake.feature_bits = TR_RAFT_HANDSHAKE_FEATURE_CURRENT;
    handshake.wire_major = TR_RAFT_HANDSHAKE_WIRE_MAJOR;
    handshake.wire_minor = TR_RAFT_HANDSHAKE_WIRE_MINOR;
    handshake.max_frame_size = TR_RAFT_WIRE_MAX_FRAME_SIZE;
    handshake.max_snapshot_chunk_size =
        TR_RAFT_WIRE_MAX_SNAPSHOT_CHUNK_BYTES;
    config.transport.handshake = &handshake;
    config.transport.on_message = ignore_message;
    config.outbound_queue_capacity = capacity;
    check_equal(tr_raft_cnet_peer_create(&config, &peer), SALTS_OK);
    return peer;
}

spec("Raft CNet peer adapter")
{
    it("exposes callbacks and a bounded copied queue")
    {
        cnet_client client;
        cnet_observer observer;
        tr_raft_cnet_peer_t *peer;
        tr_raft_cnet_peer_status_t status;
        tr_raft_message_t message;

        memset(&client, 0, sizeof(client));
        memset(&message, 0, sizeof(message));
        peer = make_peer(&client, 1U);
        observer = tr_raft_cnet_peer_observer(peer);
        check_not_null(observer.on_state);
        check_not_null(observer.on_receive);
        check_not_null(observer.on_send);
        check(observer.user == peer);

        message.type = TR_RAFT_MSG_HEARTBEAT_REQUEST;
        message.from = 1U;
        message.to = 2U;
        message.term = 3U;
        check_equal(tr_raft_cnet_peer_enqueue(peer, &message), SALTS_OK);
        check_equal(tr_raft_cnet_peer_enqueue(peer, &message), SALTS_ENOSPC);
        check_equal(tr_raft_cnet_peer_step(peer), SALTS_EBUSY);
        check_equal(tr_raft_cnet_peer_get_status(peer, &status), SALTS_OK);
        check_equal(status.queued_payload_count, 1U);
        check_equal(tr_raft_cnet_peer_stop(peer), SALTS_OK);
        check_equal(tr_raft_cnet_peer_enqueue(peer, &message), SALTS_EPIPE);
        check_equal(tr_raft_cnet_peer_destroy(peer), SALTS_OK);
    }
}
