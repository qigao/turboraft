#include "raft_snapshot_coordinator.h"

#include <turboraft/raft_snapshot_receiver.h>

#include <tinytest.h>
#include <turbo_error.h>

#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

typedef struct snapshot_emit_capture {
    tr_raft_snapshot_chunk_t chunks[6];
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

        check_equal(tr_raft_snapshot_coordinator_create(
                         &coordinator_config, &coordinator), TURBO_OK);
        check_equal(tr_raft_snapshot_receiver_create(
                         &receiver_config, &receiver), TURBO_OK);
        check_equal(tr_raft_snapshot_coordinator_begin(
                         coordinator, 7U, 9U, 6U,
                         &coordinator_configuration,
                         snapshot, sizeof(snapshot)), TURBO_OK);
        check_equal(emitted.count, 1U);
        check_equal(emitted.chunks[0].snapshot_offset, 0U);
        check_equal(emitted.chunks[0].data_length, 512U);

        check_equal(tr_raft_snapshot_receiver_handle(
                         receiver, &emitted.chunks[0], &receive_result),
                     TURBO_OK);
        check(!receive_result.installed);
        emitted.fail_next = true;
        check_equal(tr_raft_snapshot_coordinator_handle_ack(
                         coordinator, &receive_result.ack), TURBO_EPIPE);
        check_equal(emitted.count, 1U);
        check_equal(tr_raft_snapshot_coordinator_resume(coordinator),
                     TURBO_OK);
        check_equal(emitted.count, 2U);
        check_equal(emitted.chunks[1].snapshot_offset, 512U);
        check_equal(emitted.chunks[1].data_length, 88U);
        check(emitted.chunks[1].done);

        check_equal(tr_raft_snapshot_receiver_handle(
                         receiver, &emitted.chunks[1], &receive_result),
                     TURBO_OK);
        check(receive_result.installed);
        check_equal(tr_raft_snapshot_coordinator_handle_ack(
                         coordinator, &receive_result.ack), TURBO_OK);
        check_equal(tr_raft_snapshot_coordinator_handle_ack(
                         coordinator, &receive_result.ack), TURBO_OK);
        check_equal(tr_raft_snapshot_coordinator_get_status(
                         coordinator, &status), TURBO_OK);
        check(status.complete);
        check_equal(status.acknowledged_offset, sizeof(snapshot));
        check_equal(installed.count, 1U);
        check_equal(installed.size, sizeof(snapshot));
        check_equal(installed.data, snapshot, sizeof(snapshot));

        tr_raft_snapshot_receiver_destroy(receiver);
        tr_raft_snapshot_coordinator_destroy(coordinator);
    }

    it("fills and refills a four-chunk V5 window")
    {
        enum { SNAPSHOT_BYTES = 5U * TR_RAFT_WIRE_MAX_SNAPSHOT_CHUNK_BYTES };
        tr_raft_snapshot_coordinator_config_t config;
        tr_raft_snapshot_coordinator_t *coordinator = NULL;
        tr_raft_snapshot_ack_t ack;
        snapshot_emit_capture_t emitted;
        uint8_t *snapshot = malloc(SNAPSHOT_BYTES);

        check(snapshot != NULL);
        if (snapshot == NULL) {
            return;
        }
        memset(snapshot, 0x31, SNAPSHOT_BYTES);
        memset(&config, 0, sizeof(config));
        memset(&emitted, 0, sizeof(emitted));
        config.self_id = 1U;
        config.peer_id = 2U;
        config.max_snapshot_bytes = SNAPSHOT_BYTES;
        config.chunk_size = TR_RAFT_WIRE_MAX_SNAPSHOT_CHUNK_BYTES;
        config.max_inflight_chunks = TR_RAFT_SNAPSHOT_DEFAULT_INFLIGHT_CHUNKS;
        config.emit = snapshot_capture_emit;
        config.emit_context = &emitted;
        check_equal(tr_raft_snapshot_coordinator_create(&config, &coordinator),
                     TURBO_OK);
        check_equal(tr_raft_snapshot_coordinator_begin(
                         coordinator, 7U, 9U, 6U,
                         &coordinator_configuration, snapshot,
                         SNAPSHOT_BYTES), TURBO_OK);
        check_equal(emitted.count, 4U);

        memset(&ack, 0, sizeof(ack));
        ack.from = 2U;
        ack.to = 1U;
        ack.term = 7U;
        ack.snapshot_index = 9U;
        ack.snapshot_size = SNAPSHOT_BYTES;
        ack.next_offset = 2U * TR_RAFT_WIRE_MAX_SNAPSHOT_CHUNK_BYTES;
        ack.accepted = true;
        memcpy(ack.snapshot_digest, emitted.chunks[0].snapshot_digest,
               sizeof(ack.snapshot_digest));
        check_equal(tr_raft_snapshot_coordinator_handle_ack(coordinator, &ack),
                     TURBO_OK);
        check_equal(emitted.count, 5U);
        check_equal(emitted.chunks[4].snapshot_offset,
                      4U * TR_RAFT_WIRE_MAX_SNAPSHOT_CHUNK_BYTES);
        check(emitted.chunks[4].done);
        tr_raft_snapshot_coordinator_destroy(coordinator);
        free(snapshot);
    }
}
