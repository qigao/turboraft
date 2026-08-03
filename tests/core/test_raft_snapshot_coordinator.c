#include "raft_snapshot_coordinator.h"

#include <turboraft/raft_snapshot_receiver.h>

#include <tinytest.h>
#include <turbo_error.h>

#include <stdbool.h>
#include <string.h>

typedef struct snapshot_emit_capture {
    tr_raft_snapshot_chunk_t chunks[3];
    size_t count;
    bool fail_next;
} snapshot_emit_capture_t;

typedef struct snapshot_install_capture {
    uint8_t data[600];
    size_t size;
    size_t count;
} snapshot_install_capture_t;

static const tr_raft_conf_t coordinator_configuration = {
    TR_RAFT_CONF_FINAL, 0U, 1U,
    {{2U, TR_RAFT_CONF_OLD_VOTER | TR_RAFT_CONF_NEW_VOTER}}
};

static int snapshot_capture_emit(
    void *context,
    const tr_raft_snapshot_chunk_t *chunk)
{
    snapshot_emit_capture_t *capture =
        (snapshot_emit_capture_t *) context;

    if (capture == NULL || chunk == NULL) {
        return TURBO_EINVAL;
    }
    if (capture->fail_next) {
        capture->fail_next = false;
        return TURBO_EPIPE;
    }
    if (capture->count >= sizeof(capture->chunks) / sizeof(capture->chunks[0])) {
        return TURBO_ENOSPC;
    }
    capture->chunks[capture->count++] = *chunk;
    return TURBO_OK;
}

static int snapshot_capture_install(
    void *context,
    tr_raft_term_t leader_term,
    tr_raft_index_t snapshot_index,
    tr_raft_term_t snapshot_term,
    const tr_raft_conf_t *configuration,
    const uint8_t *data,
    size_t size)
{
    snapshot_install_capture_t *capture =
        (snapshot_install_capture_t *) context;

    if (capture == NULL || leader_term != 7U || snapshot_index != 9U ||
        snapshot_term != 6U || configuration == NULL || data == NULL ||
        size != sizeof(capture->data)) {
        return TURBO_EPROTO;
    }
    memcpy(capture->data, data, size);
    capture->size = size;
    ++capture->count;
    return TURBO_OK;
}

spec("raft snapshot coordinator")
{
    it("drives two chunks and resumes an emit failure")
    {
        tr_raft_snapshot_coordinator_config_t coordinator_config;
        tr_raft_snapshot_receiver_config_t receiver_config;
        tr_raft_snapshot_coordinator_t *coordinator = NULL;
        tr_raft_snapshot_receiver_t *receiver = NULL;
        tr_raft_snapshot_receive_result_t receive_result;
        tr_raft_snapshot_sender_status_t status;
        snapshot_emit_capture_t emitted;
        snapshot_install_capture_t installed;
        uint8_t snapshot[600];
        size_t index;

        memset(&coordinator_config, 0, sizeof(coordinator_config));
        memset(&receiver_config, 0, sizeof(receiver_config));
        memset(&emitted, 0, sizeof(emitted));
        memset(&installed, 0, sizeof(installed));
        for (index = 0U; index < sizeof(snapshot); ++index) {
            snapshot[index] = (uint8_t) (index * 17U);
        }

        coordinator_config.self_id = 1U;
        coordinator_config.peer_id = 2U;
        coordinator_config.max_snapshot_bytes = 1024U;
        coordinator_config.emit = snapshot_capture_emit;
        coordinator_config.emit_context = &emitted;
        receiver_config.self_id = 2U;
        receiver_config.max_snapshot_bytes = 1024U;
        receiver_config.install = snapshot_capture_install;
        receiver_config.install_context = &installed;

        check_int_eq(tr_raft_snapshot_coordinator_create(
                         &coordinator_config, &coordinator), TURBO_OK);
        check_int_eq(tr_raft_snapshot_receiver_create(
                         &receiver_config, &receiver), TURBO_OK);
        check_int_eq(tr_raft_snapshot_coordinator_begin(
                         coordinator, 7U, 9U, 6U,
                         &coordinator_configuration,
                         snapshot, sizeof(snapshot)), TURBO_OK);
        check_size_eq(emitted.count, 1U);
        check_long_eq(emitted.chunks[0].snapshot_offset, 0U);
        check_size_eq(emitted.chunks[0].data_length, 512U);

        check_int_eq(tr_raft_snapshot_receiver_handle(
                         receiver, &emitted.chunks[0], &receive_result),
                     TURBO_OK);
        check(!receive_result.installed);
        emitted.fail_next = true;
        check_int_eq(tr_raft_snapshot_coordinator_handle_ack(
                         coordinator, &receive_result.ack), TURBO_EPIPE);
        check_size_eq(emitted.count, 1U);
        check_int_eq(tr_raft_snapshot_coordinator_resume(coordinator),
                     TURBO_OK);
        check_size_eq(emitted.count, 2U);
        check_long_eq(emitted.chunks[1].snapshot_offset, 512U);
        check_size_eq(emitted.chunks[1].data_length, 88U);
        check(emitted.chunks[1].done);

        check_int_eq(tr_raft_snapshot_receiver_handle(
                         receiver, &emitted.chunks[1], &receive_result),
                     TURBO_OK);
        check(receive_result.installed);
        check_int_eq(tr_raft_snapshot_coordinator_handle_ack(
                         coordinator, &receive_result.ack), TURBO_OK);
        check_int_eq(tr_raft_snapshot_coordinator_handle_ack(
                         coordinator, &receive_result.ack), TURBO_OK);
        check_int_eq(tr_raft_snapshot_coordinator_get_status(
                         coordinator, &status), TURBO_OK);
        check(status.complete);
        check_long_eq(status.acknowledged_offset, sizeof(snapshot));
        check_size_eq(installed.count, 1U);
        check_size_eq(installed.size, sizeof(snapshot));
        check_mem_eq(installed.data, snapshot, sizeof(snapshot));

        tr_raft_snapshot_receiver_destroy(receiver);
        tr_raft_snapshot_coordinator_destroy(coordinator);
    }
}
