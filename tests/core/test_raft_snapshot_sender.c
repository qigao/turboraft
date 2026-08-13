#include <turboraft/raft_snapshot_sender.h>

#include <tinytest.h>
#include <turbo_error.h>

#include <stdlib.h>
#include <string.h>

static const tr_raft_conf_t sender_configuration = {
    TR_RAFT_CONF_FINAL, 0U, 1U,
    {{1U, TR_RAFT_CONF_OLD_VOTER | TR_RAFT_CONF_NEW_VOTER}}
};

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

        check_int_eq(tr_raft_snapshot_sender_create(&config, &sender), TURBO_OK);
        check_int_eq(tr_raft_snapshot_sender_begin(sender, 7U, 9U, 6U,
                                                   &sender_configuration,
                                                   snapshot, sizeof(snapshot)),
                     TURBO_OK);
        check_int_eq(tr_raft_snapshot_sender_next_chunk(sender, &first), TURBO_OK);
        check_long_eq(first.snapshot_offset, 0U);
        check_long_eq(first.data_length, 512U);
        check(first.has_configuration);
        check(!first.done);

        check_int_eq(tr_raft_snapshot_sender_next_chunk(sender, &repeated),
                     TURBO_OK);
        check_mem_eq(&first, &repeated, sizeof(first));

        ack.from = 2U;
        ack.to = 1U;
        ack.term = 7U;
        ack.snapshot_index = 9U;
        ack.snapshot_size = sizeof(snapshot);
        ack.next_offset = 512U;
        ack.accepted = true;
        memcpy(ack.snapshot_digest, first.snapshot_digest,
               sizeof(ack.snapshot_digest));
        check_int_eq(tr_raft_snapshot_sender_acknowledge(sender, &ack), TURBO_OK);
        check_int_eq(tr_raft_snapshot_sender_acknowledge(sender, &ack), TURBO_OK);

        check_int_eq(tr_raft_snapshot_sender_next_chunk(sender, &final), TURBO_OK);
        check_long_eq(final.snapshot_offset, 512U);
        check_long_eq(final.data_length, 88U);
        check(!final.has_configuration);
        check(final.done);

        ack.accepted = false;
        ack.next_offset = 123U;
        check_int_eq(tr_raft_snapshot_sender_acknowledge(sender, &ack),
                     TURBO_EPROTO);
        check_int_eq(tr_raft_snapshot_sender_next_chunk(sender, &repeated),
                     TURBO_OK);
        check_mem_eq(&final, &repeated, sizeof(final));

        ack.next_offset = 0U;
        check_int_eq(tr_raft_snapshot_sender_acknowledge(sender, &ack), TURBO_OK);
        check_int_eq(tr_raft_snapshot_sender_next_chunk(sender, &repeated),
                     TURBO_OK);
        check_mem_eq(&first, &repeated, sizeof(first));
        ack.accepted = true;
        ack.next_offset = 512U;
        check_int_eq(tr_raft_snapshot_sender_acknowledge(sender, &ack), TURBO_OK);
        check_int_eq(tr_raft_snapshot_sender_next_chunk(sender, &final), TURBO_OK);

        ack.next_offset = sizeof(snapshot);
        check_int_eq(tr_raft_snapshot_sender_acknowledge(sender, &ack), TURBO_OK);
        check_int_eq(tr_raft_snapshot_sender_acknowledge(sender, &ack), TURBO_OK);
        check_int_eq(tr_raft_snapshot_sender_get_status(sender, &status), TURBO_OK);
        check(status.complete);
        check_long_eq(status.acknowledged_offset, sizeof(snapshot));

        ack.snapshot_digest[0] ^= 0xffU;
        check_int_eq(tr_raft_snapshot_sender_acknowledge(sender, &ack),
                     TURBO_EPROTO);

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
        check_int_eq(tr_raft_snapshot_sender_create(&config, &sender), TURBO_OK);
        check_int_eq(tr_raft_snapshot_sender_begin(
                         sender, 3U, 4U, 2U, &sender_configuration, NULL, 0U),
                     TURBO_OK);
        check_int_eq(tr_raft_snapshot_sender_next_chunk(sender, &chunk), TURBO_OK);
        check_long_eq(chunk.data_length, 0U);
        check(chunk.done);

        ack.from = 2U;
        ack.to = 1U;
        ack.term = 3U;
        ack.snapshot_index = 4U;
        ack.accepted = true;
        memcpy(ack.snapshot_digest, chunk.snapshot_digest,
               sizeof(ack.snapshot_digest));
        check_int_eq(tr_raft_snapshot_sender_acknowledge(sender, &ack), TURBO_OK);
        check_int_eq(tr_raft_snapshot_sender_get_status(sender, &status), TURBO_OK);
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
        config.max_inflight_chunks = TR_RAFT_SNAPSHOT_DEFAULT_INFLIGHT_CHUNKS;
        check_int_eq(tr_raft_snapshot_sender_create(&config, &sender), TURBO_OK);
        check_int_eq(tr_raft_snapshot_sender_begin(
                         sender, 7U, 9U, 6U, &sender_configuration,
                         snapshot, SNAPSHOT_BYTES), TURBO_OK);
        for (index = 0U; index < 4U; ++index) {
            check_int_eq(tr_raft_snapshot_sender_next_chunk(
                             sender, &chunks[index]), TURBO_OK);
            check_long_eq(chunks[index].snapshot_offset,
                          index * TR_RAFT_WIRE_MAX_SNAPSHOT_CHUNK_BYTES);
            check_size_eq(chunks[index].data_length,
                          TR_RAFT_WIRE_MAX_SNAPSHOT_CHUNK_BYTES);
        }
        check_int_eq(tr_raft_snapshot_sender_next_chunk(sender, &chunks[4]),
                     TURBO_EBUSY);

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
        check_int_eq(tr_raft_snapshot_sender_acknowledge(sender, &ack), TURBO_OK);
        check_int_eq(tr_raft_snapshot_sender_next_chunk(sender, &chunks[4]), TURBO_OK);
        check(chunks[4].done);
        check_int_eq(tr_raft_snapshot_sender_get_status(sender, &status), TURBO_OK);
        check_size_eq(status.inflight_chunks, 3U);
        check_long_eq(status.next_offset, SNAPSHOT_BYTES);

        ack.next_offset = SNAPSHOT_BYTES;
        check_int_eq(tr_raft_snapshot_sender_acknowledge(sender, &ack), TURBO_OK);
        check_int_eq(tr_raft_snapshot_sender_get_status(sender, &status), TURBO_OK);
        check(status.complete);
        check_size_eq(status.inflight_chunks, 0U);
        tr_raft_snapshot_sender_destroy(sender);
        free(snapshot);
    }
}
