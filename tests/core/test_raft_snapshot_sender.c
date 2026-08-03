#include <turboraft/raft_snapshot_sender.h>

#include <tinytest.h>
#include <turbo_error.h>

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
}
