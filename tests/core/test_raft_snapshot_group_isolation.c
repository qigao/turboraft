#include "raft_group_queue.h"

#include <turboraft/raft_snapshot_receiver.h>
#include <turboraft/raft_snapshot_sender.h>
#include <turboraft/raft_transport.h>

#include <salts_error.h>
#include <tinytest.h>

#include <stdbool.h>
#include <stdint.h>
#include <string.h>

enum {
    SNAPSHOT_CHUNK_BYTES = 64U,
    SNAPSHOT_BYTES = SNAPSHOT_CHUNK_BYTES * 3U + 17U
};

typedef struct snapshot_isolation_fixture {
    size_t source_reads;
    size_t source_releases;
    size_t sink_writes;
    bool sink_begun;
    bool sink_committed;
    bool sink_aborted;
    uint8_t sink_data[SNAPSHOT_BYTES];
} snapshot_isolation_fixture_t;

static const uint8_t snapshot_digest[TR_RAFT_WIRE_SNAPSHOT_DIGEST_SIZE] = {
    0xf9U, 0x34U, 0xaeU, 0xa4U, 0x92U, 0x62U, 0xb4U, 0xfdU,
    0x58U, 0x7eU, 0xb7U, 0x4eU, 0xbeU, 0x2cU, 0x69U, 0xb8U,
    0x57U, 0xacU, 0xa0U, 0x78U, 0x76U, 0xacU, 0xadU, 0xc2U,
    0x3fU, 0x89U, 0xd6U, 0xc0U, 0xbbU, 0xbcU, 0xcdU, 0xd5U
};

static int source_read(void *context,
                       uint64_t offset,
                       uint8_t *buffer,
                       size_t capacity,
                       size_t *out_size)
{
    snapshot_isolation_fixture_t *fixture =
        (snapshot_isolation_fixture_t *)context;
    size_t index;

    if (fixture == NULL || out_size == NULL ||
        offset > SNAPSHOT_BYTES ||
        capacity > SNAPSHOT_BYTES - (size_t)offset ||
        (capacity != 0U && buffer == NULL)) {
        return SALTS_EINVAL;
    }
    for (index = 0U; index < capacity; ++index) {
        buffer[index] = (uint8_t)((offset + index) & 0xffU);
    }
    ++fixture->source_reads;
    *out_size = capacity;
    return SALTS_OK;
}

static void source_release(void *context)
{
    snapshot_isolation_fixture_t *fixture =
        (snapshot_isolation_fixture_t *)context;

    if (fixture != NULL) {
        ++fixture->source_releases;
    }
}

static int sink_begin(void *context,
                      tr_raft_term_t leader_term,
                      tr_raft_index_t snapshot_index,
                      tr_raft_term_t snapshot_term,
                      const tr_raft_conf_t *configuration,
                      uint64_t snapshot_size)
{
    snapshot_isolation_fixture_t *fixture =
        (snapshot_isolation_fixture_t *)context;

    if (fixture == NULL || leader_term != 3U || snapshot_index != 10U ||
        snapshot_term != 2U || configuration == NULL ||
        configuration->member_count != 1U ||
        snapshot_size != SNAPSHOT_BYTES) {
        return SALTS_EPROTO;
    }
    fixture->sink_begun = true;
    return SALTS_OK;
}

static int sink_write(void *context,
                      uint64_t offset,
                      const uint8_t *data,
                      size_t size)
{
    snapshot_isolation_fixture_t *fixture =
        (snapshot_isolation_fixture_t *)context;

    if (fixture == NULL || offset > SNAPSHOT_BYTES ||
        size > SNAPSHOT_BYTES - (size_t)offset ||
        (size != 0U && data == NULL)) {
        return SALTS_EINVAL;
    }
    if (size != 0U) {
        memcpy(fixture->sink_data + (size_t)offset, data, size);
    }
    ++fixture->sink_writes;
    return SALTS_OK;
}

static int sink_commit(void *context)
{
    snapshot_isolation_fixture_t *fixture =
        (snapshot_isolation_fixture_t *)context;
    size_t index;

    if (fixture == NULL || !fixture->sink_begun) {
        return SALTS_EPROTO;
    }
    for (index = 0U; index < SNAPSHOT_BYTES; ++index) {
        if (fixture->sink_data[index] != (uint8_t)(index & 0xffU)) {
            return SALTS_EPROTO;
        }
    }
    fixture->sink_committed = true;
    return SALTS_OK;
}

static void sink_abort(void *context)
{
    snapshot_isolation_fixture_t *fixture =
        (snapshot_isolation_fixture_t *)context;

    if (fixture != NULL) {
        fixture->sink_aborted = true;
    }
}

static void release_queue_item(tr_raft_owned_transport_payload_t *owned)
{
    if (owned != NULL) {
        memset(owned, 0, sizeof(*owned));
    }
}

static tr_raft_owned_transport_payload_t heartbeat(
    tr_raft_group_id_t group_id,
    uint64_t term)
{
    tr_raft_owned_transport_payload_t owned;

    memset(&owned, 0, sizeof(owned));
    owned.payload.group_id = group_id;
    owned.payload.kind = TR_RAFT_WIRE_PAYLOAD_RAFT;
    owned.payload.data.raft.type = TR_RAFT_MSG_HEARTBEAT_REQUEST;
    owned.payload.data.raft.from = 1U;
    owned.payload.data.raft.to = 2U;
    owned.payload.data.raft.term = term;
    return owned;
}

static int enqueue_snapshot_chunk(
    tr_raft_group_queue_t *queue,
    tr_raft_snapshot_sender_t *sender)
{
    tr_raft_snapshot_chunk_t chunk;
    tr_raft_owned_transport_payload_t owned;
    int result;

    memset(&chunk, 0, sizeof(chunk));
    result = tr_raft_snapshot_sender_next_chunk(sender, &chunk);
    if (result != SALTS_OK) {
        return result;
    }
    memset(&owned, 0, sizeof(owned));
    owned.payload.group_id = 100U;
    owned.payload.kind = TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_CHUNK;
    owned.payload.data.snapshot_chunk = chunk;
    result = tr_raft_group_queue_enqueue(
        queue, &owned, chunk.data_length);
    if (result != SALTS_OK) {
        (void)tr_raft_snapshot_sender_cancel_chunk(
            sender, chunk.snapshot_offset);
    }
    return result;
}

spec("snapshot catch-up group isolation")
{
    it("lets sibling raft groups progress between snapshot chunks")
    {
        snapshot_isolation_fixture_t fixture;
        tr_raft_snapshot_sender_config_t sender_config;
        tr_raft_snapshot_receiver_config_t receiver_config;
        tr_raft_snapshot_source_t source;
        tr_raft_snapshot_sender_t *sender = NULL;
        tr_raft_snapshot_receiver_t *receiver = NULL;
        tr_raft_group_queue_config_t queue_config;
        tr_raft_group_queue_t queue;
        tr_raft_conf_t configuration;
        size_t snapshot_chunks = 0U;
        size_t sibling_progress = 0U;
        size_t sibling_since_snapshot = 0U;
        uint64_t sibling_term = 10U;
        bool complete = false;

        memset(&fixture, 0, sizeof(fixture));
        memset(&sender_config, 0, sizeof(sender_config));
        memset(&receiver_config, 0, sizeof(receiver_config));
        memset(&source, 0, sizeof(source));
        memset(&queue_config, 0, sizeof(queue_config));
        memset(&queue, 0, sizeof(queue));
        memset(&configuration, 0, sizeof(configuration));

        configuration.phase = TR_RAFT_CONF_FINAL;
        configuration.transition_id = 1U;
        configuration.member_count = 1U;
        configuration.members[0].node_id = 1U;
        configuration.members[0].roles =
            TR_RAFT_CONF_OLD_VOTER | TR_RAFT_CONF_NEW_VOTER;

        source.context = &fixture;
        source.size = SNAPSHOT_BYTES;
        memcpy(source.digest, snapshot_digest, sizeof(source.digest));
        source.read_at = source_read;
        source.release = source_release;

        sender_config.self_id = 1U;
        sender_config.peer_id = 2U;
        sender_config.max_snapshot_bytes = 1024U;
        sender_config.chunk_size = SNAPSHOT_CHUNK_BYTES;
        sender_config.max_inflight_chunks = 1U;
        check_equal(tr_raft_snapshot_sender_create(
                        &sender_config, &sender),
                    SALTS_OK);
        check_equal(tr_raft_snapshot_sender_begin_source(
                        sender, 3U, 10U, 2U, &configuration, &source),
                    SALTS_OK);

        receiver_config.self_id = 2U;
        receiver_config.max_snapshot_bytes = 1024U;
        receiver_config.stream.begin = sink_begin;
        receiver_config.stream.write = sink_write;
        receiver_config.stream.commit = sink_commit;
        receiver_config.stream.abort = sink_abort;
        receiver_config.stream.context = &fixture;
        check_equal(tr_raft_snapshot_receiver_create(
                        &receiver_config, &receiver),
                    SALTS_OK);

        queue_config.max_groups = 3U;
        queue_config.total_item_capacity = 6U;
        queue_config.total_data_bytes = 1024U;
        queue_config.per_group_item_capacity = 2U;
        queue_config.per_group_data_bytes = 512U;
        queue_config.release = release_queue_item;
        check_equal(tr_raft_group_queue_init(&queue, &queue_config),
                    SALTS_OK);

        check_equal(enqueue_snapshot_chunk(&queue, sender), SALTS_OK);

        while (!complete) {
            tr_raft_group_queue_token_t token;
            const tr_raft_owned_transport_payload_t *front = NULL;
            tr_raft_owned_transport_payload_t popped;
            size_t data_bytes = 0U;

            check_equal(tr_raft_group_queue_peek_next(
                            &queue, &token, &front),
                        SALTS_OK);
            check_not_null(front);
            if (front == NULL) {
                break;
            }

            if (front->payload.group_id == 100U) {
                tr_raft_snapshot_receive_result_t receive_result;
                tr_raft_snapshot_sender_status_t status;

                if (snapshot_chunks != 0U) {
                    check(sibling_since_snapshot >= 2U);
                }
                memset(&receive_result, 0, sizeof(receive_result));
                check_equal(tr_raft_snapshot_receiver_handle(
                                receiver,
                                &front->payload.data.snapshot_chunk,
                                &receive_result),
                            SALTS_OK);
                memset(&popped, 0, sizeof(popped));
                check_equal(tr_raft_group_queue_pop(
                                &queue, token, &popped, &data_bytes),
                            SALTS_OK);
                check_equal(data_bytes,
                            popped.payload.data.snapshot_chunk.data_length);
                check_equal(tr_raft_snapshot_sender_acknowledge(
                                sender, &receive_result.ack),
                            SALTS_OK);
                ++snapshot_chunks;
                sibling_since_snapshot = 0U;
                check_equal(tr_raft_snapshot_sender_get_status(
                                sender, &status),
                            SALTS_OK);
                complete = status.complete;
                if (!complete) {
                    tr_raft_owned_transport_payload_t g101 =
                        heartbeat(101U, sibling_term++);
                    tr_raft_owned_transport_payload_t g102 =
                        heartbeat(102U, sibling_term++);

                    check_equal(enqueue_snapshot_chunk(&queue, sender),
                                SALTS_OK);
                    check_equal(tr_raft_group_queue_enqueue(
                                    &queue, &g101, 0U),
                                SALTS_OK);
                    check_equal(tr_raft_group_queue_enqueue(
                                    &queue, &g102, 0U),
                                SALTS_OK);
                }
            } else {
                check(front->payload.group_id == 101U ||
                      front->payload.group_id == 102U);
                memset(&popped, 0, sizeof(popped));
                check_equal(tr_raft_group_queue_pop(
                                &queue, token, &popped, &data_bytes),
                            SALTS_OK);
                check_equal(data_bytes, 0U);
                ++sibling_progress;
                ++sibling_since_snapshot;
            }
        }

        check_equal(snapshot_chunks, 4U);
        check_equal(sibling_progress, 6U);
        check_equal(fixture.source_reads, 4U);
        check_equal(fixture.source_releases, 1U);
        check_equal(fixture.sink_writes, 4U);
        check(fixture.sink_committed);
        check_false(fixture.sink_aborted);
        check_equal(tr_raft_group_queue_size(&queue), 0U);

        tr_raft_group_queue_destroy(&queue);
        tr_raft_snapshot_receiver_destroy(receiver);
        tr_raft_snapshot_sender_destroy(sender);
    }
}
