#include <turboraft/raft_snapshot_sender.h>

#include <tinytest.h>
#include <salts_error.h>

#include <stdlib.h>
#include <string.h>

static const tr_raft_conf_t sender_configuration = {
    TR_RAFT_CONF_FINAL, 0U, 1U,
    {{1U, TR_RAFT_CONF_OLD_VOTER | TR_RAFT_CONF_NEW_VOTER}}
};

typedef struct generated_snapshot_source {
    uint64_t size;
    size_t read_calls;
    size_t max_requested;
    size_t release_calls;
} generated_snapshot_source_t;

static int generated_snapshot_read(
    void *context,
    uint64_t offset,
    uint8_t *buffer,
    size_t capacity,
    size_t *out_size)
{
    generated_snapshot_source_t *source =
        (generated_snapshot_source_t *)context;
    size_t index;

    if (source == NULL || out_size == NULL ||
        offset > source->size ||
        capacity > source->size - offset ||
        (capacity != 0U && buffer == NULL)) {
        return SALTS_EINVAL;
    }
    ++source->read_calls;
    if (capacity > source->max_requested) {
        source->max_requested = capacity;
    }
    for (index = 0U; index < capacity; ++index) {
        buffer[index] = (uint8_t)((offset + index) & 0xffU);
    }
    *out_size = capacity;
    return SALTS_OK;
}

static void generated_snapshot_release(void *context)
{
    generated_snapshot_source_t *source =
        (generated_snapshot_source_t *)context;

    if (source != NULL) {
        ++source->release_calls;
    }
}


spec("raft snapshot sender")
{
    it("advances only after an acknowledgement")
    {
        tr_raft_snapshot_sender_config_t config;
        tr_raft_snapshot_sender_t *sender = NULL;
        tr_raft_snapshot_chunk_t first;
        tr_raft_snapshot_chunk_t repeated;
        tr_raft_snapshot_chunk_t final;
        tr_raft_snapshot_ack_t ack;
        tr_raft_snapshot_sender_status_t status;
        uint8_t snapshot[600];
        size_t index;

        memset(&config, 0, sizeof(config));
        memset(&ack, 0, sizeof(ack));
        for (index = 0U; index < sizeof(snapshot); ++index) {
            snapshot[index] = (uint8_t)index;
        }
        config.self_id = 1U;
        config.peer_id = 2U;
        config.max_snapshot_bytes = 1024U;
        config.chunk_size = 512U;
        config.max_inflight_chunks = 1U;

        check_equal(tr_raft_snapshot_sender_create(&config, &sender), SALTS_OK);
        check_equal(tr_raft_snapshot_sender_begin(sender, 7U, 9U, 6U,
                                                   &sender_configuration,
                                                   snapshot, sizeof(snapshot)),
                     SALTS_OK);
        check_equal(tr_raft_snapshot_sender_next_chunk(sender, &first), SALTS_OK);
        check_equal(first.snapshot_offset, 0U);
        check_equal(first.data_length, 512U);
        check(first.has_configuration);
        check(!first.done);

        check_equal(tr_raft_snapshot_sender_next_chunk(sender, &repeated),
                     SALTS_OK);
        check_equal(&first, &repeated, sizeof(first));

        ack.from = 2U;
        ack.to = 1U;
        ack.term = 7U;
        ack.snapshot_index = 9U;
        ack.snapshot_size = sizeof(snapshot);
        ack.next_offset = 512U;
        ack.accepted = true;
        memcpy(ack.snapshot_digest, first.snapshot_digest,
               sizeof(ack.snapshot_digest));
        check_equal(tr_raft_snapshot_sender_acknowledge(sender, &ack), SALTS_OK);
        check_equal(tr_raft_snapshot_sender_acknowledge(sender, &ack), SALTS_OK);

        check_equal(tr_raft_snapshot_sender_next_chunk(sender, &final), SALTS_OK);
        check_equal(final.snapshot_offset, 512U);
        check_equal(final.data_length, 88U);
        check(!final.has_configuration);
        check(final.done);

        ack.accepted = false;
        ack.next_offset = 123U;
        check_equal(tr_raft_snapshot_sender_acknowledge(sender, &ack),
                     SALTS_EPROTO);
        check_equal(tr_raft_snapshot_sender_next_chunk(sender, &repeated),
                     SALTS_OK);
        check_equal(&final, &repeated, sizeof(final));

        ack.next_offset = 0U;
        check_equal(tr_raft_snapshot_sender_acknowledge(sender, &ack), SALTS_OK);
        check_equal(tr_raft_snapshot_sender_next_chunk(sender, &repeated),
                     SALTS_OK);
        check_equal(&first, &repeated, sizeof(first));
        ack.accepted = true;
        ack.next_offset = 512U;
        check_equal(tr_raft_snapshot_sender_acknowledge(sender, &ack), SALTS_OK);
        check_equal(tr_raft_snapshot_sender_next_chunk(sender, &final), SALTS_OK);

        ack.next_offset = sizeof(snapshot);
        check_equal(tr_raft_snapshot_sender_acknowledge(sender, &ack), SALTS_OK);
        check_equal(tr_raft_snapshot_sender_acknowledge(sender, &ack), SALTS_OK);
        check_equal(tr_raft_snapshot_sender_get_status(sender, &status), SALTS_OK);
        check(status.complete);
        check_equal(status.acknowledged_offset, sizeof(snapshot));

        ack.snapshot_digest[0] ^= 0xffU;
        check_equal(tr_raft_snapshot_sender_acknowledge(sender, &ack),
                     SALTS_EPROTO);

        tr_raft_snapshot_sender_destroy(sender);
    }

    it("completes an empty snapshot after its final acknowledgement")
    {
        tr_raft_snapshot_sender_config_t config;
        tr_raft_snapshot_sender_t *sender = NULL;
        tr_raft_snapshot_chunk_t chunk;
        tr_raft_snapshot_ack_t ack;
        tr_raft_snapshot_sender_status_t status;

        memset(&config, 0, sizeof(config));
        memset(&ack, 0, sizeof(ack));
        config.self_id = 1U;
        config.peer_id = 2U;
        config.max_snapshot_bytes = 1024U;
        config.chunk_size = 512U;
        config.max_inflight_chunks = 1U;
        check_equal(tr_raft_snapshot_sender_create(&config, &sender), SALTS_OK);
        check_equal(tr_raft_snapshot_sender_begin(
                         sender, 3U, 4U, 2U, &sender_configuration, NULL, 0U),
                     SALTS_OK);
        check_equal(tr_raft_snapshot_sender_next_chunk(sender, &chunk), SALTS_OK);
        check_equal(chunk.data_length, 0U);
        check(chunk.done);

        ack.from = 2U;
        ack.to = 1U;
        ack.term = 3U;
        ack.snapshot_index = 4U;
        ack.accepted = true;
        memcpy(ack.snapshot_digest, chunk.snapshot_digest,
               sizeof(ack.snapshot_digest));
        check_equal(tr_raft_snapshot_sender_acknowledge(sender, &ack), SALTS_OK);
        check_equal(tr_raft_snapshot_sender_get_status(sender, &status), SALTS_OK);
        check(status.complete);

        tr_raft_snapshot_sender_destroy(sender);
    }

    it("keeps four 64 KiB chunks in flight and applies cumulative ACKs")
    {
        enum { SNAPSHOT_BYTES = 5U * TR_RAFT_WIRE_MAX_SNAPSHOT_CHUNK_BYTES };
        tr_raft_snapshot_sender_config_t config;
        tr_raft_snapshot_sender_t *sender = NULL;
        tr_raft_snapshot_chunk_t chunks[5];
        tr_raft_snapshot_ack_t ack;
        tr_raft_snapshot_sender_status_t status;
        uint8_t *snapshot = (uint8_t *)malloc(SNAPSHOT_BYTES);
        size_t index;

        check(snapshot != NULL);
        if (snapshot == NULL) {
            return;
        }
        memset(snapshot, 0x5a, SNAPSHOT_BYTES);
        memset(&config, 0, sizeof(config));
        config.self_id = 1U;
        config.peer_id = 2U;
        config.max_snapshot_bytes = SNAPSHOT_BYTES;
        config.chunk_size = TR_RAFT_WIRE_MAX_SNAPSHOT_CHUNK_BYTES;
        config.max_inflight_chunks =
            TR_RAFT_SNAPSHOT_RECOMMENDED_INFLIGHT_CHUNKS;
        check_equal(tr_raft_snapshot_sender_create(&config, &sender), SALTS_OK);
        check_equal(tr_raft_snapshot_sender_begin(
                         sender, 7U, 9U, 6U, &sender_configuration,
                         snapshot, SNAPSHOT_BYTES), SALTS_OK);
        for (index = 0U; index < 4U; ++index) {
            check_equal(tr_raft_snapshot_sender_next_chunk(
                             sender, &chunks[index]), SALTS_OK);
            check_equal(chunks[index].snapshot_offset,
                          index * TR_RAFT_WIRE_MAX_SNAPSHOT_CHUNK_BYTES);
            check_equal(chunks[index].data_length,
                          TR_RAFT_WIRE_MAX_SNAPSHOT_CHUNK_BYTES);
        }
        check_equal(tr_raft_snapshot_sender_next_chunk(sender, &chunks[4]),
                     SALTS_EBUSY);

        memset(&ack, 0, sizeof(ack));
        ack.from = 2U;
        ack.to = 1U;
        ack.term = 7U;
        ack.snapshot_index = 9U;
        ack.snapshot_size = SNAPSHOT_BYTES;
        ack.next_offset = 2U * TR_RAFT_WIRE_MAX_SNAPSHOT_CHUNK_BYTES;
        ack.accepted = true;
        memcpy(ack.snapshot_digest, chunks[0].snapshot_digest,
               sizeof(ack.snapshot_digest));
        check_equal(tr_raft_snapshot_sender_acknowledge(sender, &ack), SALTS_OK);
        check_equal(tr_raft_snapshot_sender_next_chunk(sender, &chunks[4]), SALTS_OK);
        check(chunks[4].done);
        check_equal(tr_raft_snapshot_sender_get_status(sender, &status), SALTS_OK);
        check_equal(status.inflight_chunks, 3U);
        check_equal(status.next_offset, SNAPSHOT_BYTES);

        ack.next_offset = SNAPSHOT_BYTES;
        check_equal(tr_raft_snapshot_sender_acknowledge(sender, &ack), SALTS_OK);
        check_equal(tr_raft_snapshot_sender_get_status(sender, &status), SALTS_OK);
        check(status.complete);
        check_equal(status.inflight_chunks, 0U);
        tr_raft_snapshot_sender_destroy(sender);
        free(snapshot);
    }
    it("streams snapshot bytes from a bounded read-at source")
    {
        enum {
            SNAPSHOT_BYTES =
                3U * TR_RAFT_WIRE_MAX_SNAPSHOT_CHUNK_BYTES + 17U
        };
        tr_raft_snapshot_sender_config_t config;
        tr_raft_snapshot_sender_t *sender = NULL;
        tr_raft_snapshot_source_t source;
        generated_snapshot_source_t generated;
        tr_raft_snapshot_chunk_t chunk;
        tr_raft_snapshot_ack_t ack;
        uint64_t expected_offset = 0U;
        size_t chunk_count = 0U;

        memset(&config, 0, sizeof(config));
        memset(&source, 0, sizeof(source));
        memset(&generated, 0, sizeof(generated));
        memset(&ack, 0, sizeof(ack));

        generated.size = SNAPSHOT_BYTES;
        source.context = &generated;
        source.size = SNAPSHOT_BYTES;
        memset(source.digest, 0x5a, sizeof(source.digest));
        source.read_at = generated_snapshot_read;
        source.release = generated_snapshot_release;

        config.self_id = 1U;
        config.peer_id = 2U;
        config.max_snapshot_bytes = SNAPSHOT_BYTES;
        config.chunk_size = TR_RAFT_WIRE_MAX_SNAPSHOT_CHUNK_BYTES;
        config.max_inflight_chunks = 1U;

        check_equal(tr_raft_snapshot_sender_create(&config, &sender),
                    SALTS_OK);
        check_equal(tr_raft_snapshot_sender_begin_source(
                         sender, 7U, 9U, 6U,
                         &sender_configuration, &source),
                    SALTS_OK);

        ack.from = 2U;
        ack.to = 1U;
        ack.term = 7U;
        ack.snapshot_index = 9U;
        ack.snapshot_size = SNAPSHOT_BYTES;
        ack.accepted = true;
        memcpy(ack.snapshot_digest, source.digest,
               sizeof(ack.snapshot_digest));

        while (expected_offset < SNAPSHOT_BYTES) {
            size_t index;

            check_equal(tr_raft_snapshot_sender_next_chunk(
                             sender, &chunk),
                        SALTS_OK);
            check_equal(chunk.snapshot_offset, expected_offset);
            check(chunk.data_length <=
                  TR_RAFT_WIRE_MAX_SNAPSHOT_CHUNK_BYTES);
            for (index = 0U; index < chunk.data_length; ++index) {
                check_equal(chunk.data[index],
                            (uint8_t)((expected_offset + index) & 0xffU));
            }
            expected_offset += chunk.data_length;
            ++chunk_count;

            ack.next_offset = expected_offset;
            check_equal(tr_raft_snapshot_sender_acknowledge(
                             sender, &ack),
                        SALTS_OK);
        }

        check_equal(chunk_count, 4U);
        check_equal(generated.read_calls, 4U);
        check_equal(generated.max_requested,
                    TR_RAFT_WIRE_MAX_SNAPSHOT_CHUNK_BYTES);
        check_equal(generated.release_calls, 1U);

        tr_raft_snapshot_sender_destroy(sender);
        check_equal(generated.release_calls, 1U);
    }

}
