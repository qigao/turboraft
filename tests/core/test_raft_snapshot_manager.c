#include "raft_snapshot_manager.h"

#include <turboraft/raft_snapshot_receiver.h>

#include <tinytest.h>
#include <turbo_error.h>

#include <string.h>

static const tr_raft_conf_t manager_configuration = {
    TR_RAFT_CONF_FINAL, 0U, 2U,
    {
        {2U, TR_RAFT_CONF_OLD_VOTER | TR_RAFT_CONF_NEW_VOTER},
        {3U, TR_RAFT_CONF_OLD_VOTER | TR_RAFT_CONF_NEW_VOTER}
    }
};

typedef struct manager_payload_capture {
    tr_raft_coronet_payload_t payloads[3];
    size_t count;
} manager_payload_capture_t;

typedef struct manager_install_capture {
    const uint8_t *expected;
    size_t expected_size;
    size_t count;
} manager_install_capture_t;

static int manager_enqueue(
    void *context,
    const tr_raft_coronet_payload_t *payload)
{
    manager_payload_capture_t *capture =
        (manager_payload_capture_t *) context;

    if (capture == NULL || payload == NULL ||
        capture->count >= sizeof(capture->payloads) /
                              sizeof(capture->payloads[0])) {
        return TURBO_ENOSPC;
    }
    capture->payloads[capture->count++] = *payload;
    return TURBO_OK;
}

static int manager_install(
    void *context,
    tr_raft_term_t leader_term,
    tr_raft_index_t snapshot_index,
    tr_raft_term_t snapshot_term,
    const tr_raft_conf_t *configuration,
    const uint8_t *data,
    size_t size)
{
    manager_install_capture_t *capture =
        (manager_install_capture_t *) context;

    (void) leader_term;
    (void) snapshot_index;
    (void) snapshot_term;
    if (capture == NULL || data == NULL || size != capture->expected_size ||
        memcmp(data, capture->expected, size) != 0) {
        return TURBO_EPROTO;
    }
    ++capture->count;
    return TURBO_OK;
}

static int manager_receive_and_ack(
    tr_raft_snapshot_manager_t *manager,
    tr_raft_snapshot_receiver_t *receiver,
    const tr_raft_coronet_payload_t *chunk_payload)
{
    tr_raft_snapshot_receive_result_t result;
    tr_raft_coronet_payload_t ack_payload;
    int status;

    status = tr_raft_snapshot_receiver_handle(
        receiver, &chunk_payload->data.snapshot_chunk, &result);
    if (status != TURBO_OK) {
        return status;
    }
    memset(&ack_payload, 0, sizeof(ack_payload));
    ack_payload.kind = TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_ACK;
    ack_payload.data.snapshot_ack = result.ack;
    return tr_raft_snapshot_manager_handle_payload(manager, &ack_payload);
}

spec("raft snapshot manager")
{
    it("routes independent transfers by peer identity")
    {
        const tr_raft_node_id_t peer_ids[] = {2U, 3U};
        tr_raft_snapshot_manager_config_t manager_config;
        tr_raft_snapshot_receiver_config_t receiver_config;
        tr_raft_snapshot_manager_t *manager = NULL;
        tr_raft_snapshot_receiver_t *receiver_two = NULL;
        tr_raft_snapshot_receiver_t *receiver_three = NULL;
        tr_raft_snapshot_sender_status_t status;
        manager_payload_capture_t payloads;
        manager_install_capture_t installed_two;
        manager_install_capture_t installed_three;
        uint8_t snapshot_two[600];
        const uint8_t snapshot_three[] = {0x31U, 0x32U, 0x33U};
        size_t index;

        memset(&manager_config, 0, sizeof(manager_config));
        memset(&receiver_config, 0, sizeof(receiver_config));
        memset(&payloads, 0, sizeof(payloads));
        memset(&installed_two, 0, sizeof(installed_two));
        memset(&installed_three, 0, sizeof(installed_three));
        for (index = 0U; index < sizeof(snapshot_two); ++index) {
            snapshot_two[index] = (uint8_t) (index * 11U);
        }
        installed_two.expected = snapshot_two;
        installed_two.expected_size = sizeof(snapshot_two);
        installed_three.expected = snapshot_three;
        installed_three.expected_size = sizeof(snapshot_three);

        manager_config.self_id = 1U;
        manager_config.peer_node_ids = peer_ids;
        manager_config.peer_count = sizeof(peer_ids) / sizeof(peer_ids[0]);
        manager_config.max_snapshot_bytes = 1024U;
        manager_config.enqueue = manager_enqueue;
        manager_config.enqueue_context = &payloads;
        check_int_eq(tr_raft_snapshot_manager_create(&manager_config, &manager),
                     TURBO_OK);

        receiver_config.self_id = 2U;
        receiver_config.max_snapshot_bytes = 1024U;
        receiver_config.install = manager_install;
        receiver_config.install_context = &installed_two;
        check_int_eq(tr_raft_snapshot_receiver_create(
                         &receiver_config, &receiver_two), TURBO_OK);
        receiver_config.self_id = 3U;
        receiver_config.install_context = &installed_three;
        check_int_eq(tr_raft_snapshot_receiver_create(
                         &receiver_config, &receiver_three), TURBO_OK);

        check_int_eq(tr_raft_snapshot_manager_begin(
                         manager, 3U, 7U, 10U, 7U,
                         &manager_configuration,
                         snapshot_three, sizeof(snapshot_three)), TURBO_OK);
        check_int_eq(tr_raft_snapshot_manager_begin(
                         manager, 2U, 7U, 9U, 6U,
                         &manager_configuration,
                         snapshot_two, sizeof(snapshot_two)), TURBO_OK);
        check_size_eq(payloads.count, 2U);
        check_long_eq(payloads.payloads[0].data.snapshot_chunk.to, 3U);
        check_long_eq(payloads.payloads[1].data.snapshot_chunk.to, 2U);

        check_int_eq(manager_receive_and_ack(
                         manager, receiver_three, &payloads.payloads[0]),
                     TURBO_OK);
        check_int_eq(manager_receive_and_ack(
                         manager, receiver_two, &payloads.payloads[1]),
                     TURBO_OK);
        check_size_eq(payloads.count, 3U);
        check_long_eq(payloads.payloads[2].data.snapshot_chunk.to, 2U);
        check_long_eq(payloads.payloads[2].data.snapshot_chunk.snapshot_offset,
                      512U);
        check_int_eq(manager_receive_and_ack(
                         manager, receiver_two, &payloads.payloads[2]),
                     TURBO_OK);

        check_int_eq(tr_raft_snapshot_manager_get_status(manager, 2U, &status),
                     TURBO_OK);
        check(status.complete);
        check_int_eq(tr_raft_snapshot_manager_get_status(manager, 3U, &status),
                     TURBO_OK);
        check(status.complete);
        check_size_eq(installed_two.count, 1U);
        check_size_eq(installed_three.count, 1U);

        tr_raft_snapshot_receiver_destroy(receiver_three);
        tr_raft_snapshot_receiver_destroy(receiver_two);
        tr_raft_snapshot_manager_destroy(manager);
    }
}
