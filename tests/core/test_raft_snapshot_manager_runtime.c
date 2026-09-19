#include <turboraft/raft_snapshot_manager.h>

#include <turboraft/raft_snapshot_receiver.h>

#include <tinytest.h>
#include <salts_error.h>

#include <string.h>

typedef struct runtime_manager_capture {
    tr_raft_transport_payload_t payloads[4];
    size_t payload_count;
    size_t provider_count;
    size_t install_count;
    size_t complete_count;
    tr_raft_node_id_t completed_peer;
    tr_raft_index_t completed_index;
} runtime_manager_capture_t;

static int runtime_manager_enqueue(
    void *context,
    const tr_raft_transport_payload_t *payload)
{
    runtime_manager_capture_t *capture =
        (runtime_manager_capture_t *) context;

    if (capture == NULL || payload == NULL ||
        capture->payload_count >= 4U) {
        return SALTS_ENOSPC;
    }
    capture->payloads[capture->payload_count++] = *payload;
    return SALTS_OK;
}

static int runtime_manager_provider(
    void *context,
    tr_raft_index_t required_index,
    tr_raft_term_t required_term,
    tr_raft_snapshot_point_t *out_point,
    uint8_t *buffer,
    size_t capacity,
    size_t *out_size)
{
    static const uint8_t snapshot[] = {0x72U, 0x61U, 0x66U, 0x74U};
    runtime_manager_capture_t *capture =
        (runtime_manager_capture_t *) context;

    if (capture == NULL || out_point == NULL || buffer == NULL ||
        out_size == NULL || required_index != 10U || required_term != 7U ||
        capacity < sizeof(snapshot)) {
        return SALTS_EINVAL;
    }
    memset(out_point, 0, sizeof(*out_point));
    out_point->index = required_index;
    out_point->term = required_term;
    out_point->configuration.phase = TR_RAFT_CONF_FINAL;
    out_point->configuration.transition_id = 9U;
    out_point->configuration.member_count = 2U;
    out_point->configuration.members[0].node_id = 1U;
    out_point->configuration.members[0].roles =
        TR_RAFT_CONF_OLD_VOTER | TR_RAFT_CONF_NEW_VOTER;
    out_point->configuration.members[1].node_id = 2U;
    out_point->configuration.members[1].roles =
        TR_RAFT_CONF_OLD_VOTER | TR_RAFT_CONF_NEW_VOTER;
    memcpy(buffer, snapshot, sizeof(snapshot));
    *out_size = sizeof(snapshot);
    ++capture->provider_count;
    return SALTS_OK;
}

static int runtime_manager_install(
    void *context,
    tr_raft_term_t leader_term,
    tr_raft_index_t snapshot_index,
    tr_raft_term_t snapshot_term,
    const tr_raft_conf_t *configuration,
    const uint8_t *data,
    size_t size)
{
    static const uint8_t expected[] = {0x72U, 0x61U, 0x66U, 0x74U};
    runtime_manager_capture_t *capture =
        (runtime_manager_capture_t *) context;

    if (capture == NULL || leader_term != 8U || snapshot_index != 10U ||
        snapshot_term != 7U || configuration == NULL ||
        configuration->member_count != 2U || size != sizeof(expected) ||
        memcmp(data, expected, sizeof(expected)) != 0) {
        return SALTS_EPROTO;
    }
    ++capture->install_count;
    return SALTS_OK;
}

static int runtime_manager_complete(void *context,
                                    tr_raft_node_id_t peer_id,
                                    tr_raft_index_t snapshot_index)
{
    runtime_manager_capture_t *capture =
        (runtime_manager_capture_t *) context;

    if (capture == NULL) {
        return SALTS_EINVAL;
    }
    ++capture->complete_count;
    capture->completed_peer = peer_id;
    capture->completed_index = snapshot_index;
    return SALTS_OK;
}

spec("snapshot manager runtime bridge")
{
    it("deduplicates requests and reports completed installation")
    {
        const tr_raft_node_id_t peers[] = {2U};
        runtime_manager_capture_t capture;
        tr_raft_snapshot_manager_config_t manager_config;
        tr_raft_snapshot_receiver_config_t receiver_config;
        tr_raft_snapshot_manager_t *manager = NULL;
        tr_raft_snapshot_receiver_t *receiver = NULL;
        tr_raft_snapshot_request_t request;
        tr_raft_snapshot_receive_result_t receive_result;
        tr_raft_transport_payload_t ack_payload;

        memset(&capture, 0, sizeof(capture));
        memset(&manager_config, 0, sizeof(manager_config));
        manager_config.self_id = 1U;
        manager_config.group_id = 88U;
        manager_config.peer_node_ids = peers;
        manager_config.peer_count = 1U;
        manager_config.max_snapshot_bytes = 1024U;
        manager_config.snapshot_chunk_size =
            512U;
        manager_config.snapshot_max_inflight_chunks = 1U;
        manager_config.enqueue = runtime_manager_enqueue;
        manager_config.enqueue_context = &capture;
        manager_config.provider = runtime_manager_provider;
        manager_config.provider_context = &capture;
        manager_config.complete = runtime_manager_complete;
        manager_config.complete_context = &capture;
        check_equal(tr_raft_snapshot_manager_create(&manager_config,
                                                     &manager), SALTS_OK);

        memset(&request, 0, sizeof(request));
        request.peer_id = 2U;
        request.leader_term = 8U;
        request.snapshot_index = 10U;
        request.snapshot_term = 7U;
        check_equal(tr_raft_snapshot_manager_enqueue_request(manager,
                                                              &request),
                     SALTS_OK);
        check_equal(tr_raft_snapshot_manager_enqueue_request(manager,
                                                              &request),
                     SALTS_OK);
        check_equal(capture.provider_count, 1U);
        check_equal(capture.payload_count, 1U);

        memset(&receiver_config, 0, sizeof(receiver_config));
        receiver_config.self_id = 2U;
        receiver_config.max_snapshot_bytes = 1024U;
        receiver_config.install = runtime_manager_install;
        receiver_config.install_context = &capture;
        check_equal(tr_raft_snapshot_receiver_create(&receiver_config,
                                                      &receiver), SALTS_OK);
        check_equal(tr_raft_snapshot_receiver_handle(
                         receiver,
                         &capture.payloads[0].data.snapshot_chunk,
                         &receive_result), SALTS_OK);
        memset(&ack_payload, 0, sizeof(ack_payload));
        ack_payload.group_id = 88U;
        ack_payload.kind = TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_ACK;
        ack_payload.data.snapshot_ack = receive_result.ack;
        check_equal(tr_raft_snapshot_manager_handle_payload(manager,
                                                             &ack_payload),
                     SALTS_OK);
        check_equal(capture.install_count, 1U);
        check_equal(capture.complete_count, 1U);
        check_equal(capture.completed_peer, 2U);
        check_equal(capture.completed_index, 10U);

        tr_raft_snapshot_receiver_destroy(receiver);
        tr_raft_snapshot_manager_destroy(manager);
    }
}
