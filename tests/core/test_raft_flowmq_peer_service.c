#include <turboraft/raft_flowmq_peer_service.h>

#include <salts_error.h>
#include <tinytest.h>

#include <string.h>

static int ignore_message(void *context, const tr_raft_message_t *message)
{
    (void)context;
    (void)message;
    return SALTS_OK;
}

static tr_raft_handshake_config_t protocol_config(void)
{
    tr_raft_handshake_config_t config;
    size_t index;

    memset(&config, 0, sizeof(config));
    for (index = 0U; index < sizeof(config.cluster_id.bytes); ++index) {
        config.cluster_id.bytes[index] = (uint8_t)(11U + index);
    }
    config.local_node_id = 1U;
    config.process_incarnation.bytes[0] = 1U;
    config.config_epoch = 1U;
    config.feature_bits = TR_RAFT_HANDSHAKE_FEATURE_CURRENT;
    config.wire_major_min = TR_RAFT_HANDSHAKE_WIRE_MAJOR;
    config.wire_major_max = TR_RAFT_HANDSHAKE_WIRE_MAJOR;
    config.wire_minor_min = TR_RAFT_HANDSHAKE_WIRE_MINOR;
    config.wire_minor_max = TR_RAFT_HANDSHAKE_WIRE_MINOR;
    config.max_frame_size = TR_RAFT_WIRE_MAX_FRAME_SIZE;
    config.max_snapshot_chunk_size = TR_RAFT_WIRE_MAX_SNAPSHOT_CHUNK_BYTES;
    return config;
}

static tr_raft_flowmq_peer_service_t *make_service(size_t capacity)
{
    tr_raft_flowmq_peer_service_config_t config;
    tr_raft_flowmq_peer_config_t peer;
    tr_raft_handshake_result_t handshake;
    tr_raft_flowmq_peer_service_t *service = NULL;

    memset(&config, 0, sizeof(config));
    memset(&peer, 0, sizeof(peer));
    memset(&handshake, 0, sizeof(handshake));
    config.protocol = protocol_config();
    peer.node_id = 2U;
    handshake.complete = 1;
    handshake.cluster_id = config.protocol.cluster_id;
    handshake.local_node_id = 1U;
    handshake.peer_node_id = 2U;
    handshake.peer_process_incarnation.bytes[0] = 1U;
    handshake.feature_bits = TR_RAFT_HANDSHAKE_FEATURE_CURRENT;
    handshake.wire_major = TR_RAFT_HANDSHAKE_WIRE_MAJOR;
    handshake.wire_minor = TR_RAFT_HANDSHAKE_WIRE_MINOR;
    handshake.max_frame_size = TR_RAFT_WIRE_MAX_FRAME_SIZE;
    handshake.max_snapshot_chunk_size =
        TR_RAFT_WIRE_MAX_SNAPSHOT_CHUNK_BYTES;
    peer.handshake = &handshake;
    peer.identity = "node-2";
    peer.endpoint = "tcp://127.0.0.1:57992";
    config.bind_endpoint = "tcp://127.0.0.1:0";
    config.local_identity = "node-1";
    config.peers = &peer;
    config.peer_count = 1U;
    config.outbound_queue_capacity = capacity;
    config.max_send_batch_items =
        TR_RAFT_FLOWMQ_RECOMMENDED_SEND_BATCH_ITEMS;
    config.max_receive_batch_items =
        TR_RAFT_FLOWMQ_RECOMMENDED_RECEIVE_BATCH_ITEMS;
    config.max_inflight_data_bytes =
        TR_RAFT_FLOWMQ_RECOMMENDED_INFLIGHT_DATA_BYTES;
    config.send_hwm_messages = capacity;
    config.receive_hwm_messages = capacity;
    config.send_hwm_bytes = 8U * 1024U * 1024U;
    config.receive_hwm_bytes = 8U * 1024U * 1024U;
    config.reconnect_initial_ms = 100U;
    config.reconnect_max_ms = 1000U;
    config.on_message = ignore_message;
    check_equal(tr_raft_flowmq_peer_service_create(&config, &service),
                SALTS_OK);
    return service;
}

static tr_raft_message_t heartbeat(void)
{
    tr_raft_message_t message;

    memset(&message, 0, sizeof(message));
    message.type = TR_RAFT_MSG_HEARTBEAT_REQUEST;
    message.from = 1U;
    message.to = 2U;
    message.term = 4U;
    return message;
}

spec("Raft FlowMQ caller-driven service")
{
    it("copies messages into a bounded peer FIFO")
    {
        tr_raft_flowmq_peer_service_t *service = make_service(2U);
        tr_raft_flowmq_peer_service_status_t status;
        tr_raft_message_t message = heartbeat();

        check_equal(tr_raft_flowmq_peer_service_enqueue(service, &message),
                    SALTS_OK);
        check_equal(tr_raft_flowmq_peer_service_enqueue(service, &message),
                    SALTS_OK);
        check_equal(tr_raft_flowmq_peer_service_enqueue(service, &message),
                    SALTS_ENOSPC);
        check_equal(tr_raft_flowmq_peer_service_get_status(service, &status),
                    SALTS_OK);
        check_equal(status.queued_payload_count, 2U);
        check_equal(status.outbound_queue_capacity, 2U);
        check_equal(tr_raft_flowmq_peer_service_destroy(service), SALTS_OK);
    }

    it("has explicit start stop ownership")
    {
        tr_raft_flowmq_peer_service_t *service = make_service(4U);
        tr_raft_flowmq_peer_service_step_result_t step;
        tr_raft_message_t message = heartbeat();

        check_equal(tr_raft_flowmq_peer_service_start(service), SALTS_OK);
        check_equal(tr_raft_flowmq_peer_service_destroy(service), SALTS_EBUSY);
        check_equal(tr_raft_flowmq_peer_service_enqueue(service, &message),
                    SALTS_OK);
        memset(&step, 0, sizeof(step));
        check_equal(tr_raft_flowmq_peer_service_step(service, &step), SALTS_OK);
        check_equal(tr_raft_flowmq_peer_service_stop(service), SALTS_OK);
        check_equal(tr_raft_flowmq_peer_service_enqueue(service, &message),
                    SALTS_EPIPE);
        check_equal(tr_raft_flowmq_peer_service_destroy(service), SALTS_OK);
    }
}
