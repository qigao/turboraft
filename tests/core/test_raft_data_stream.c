#include <turboraft/raft_data_stream.h>

#include <tinytest.h>
#include <salts_error.h>

#include <string.h>

enum { TEST_STREAM_BYTES = 256U * 1024U };

typedef struct test_stream_sink {
    uint8_t data[TEST_STREAM_BYTES];
    size_t used;
    size_t writes;
    bool committed;
    bool aborted;
} test_stream_sink_t;

static int test_stream_begin(
    void *context, tr_raft_node_id_t leader_id, tr_raft_term_t term,
    uint64_t stream_id, uint64_t stream_size,
    const uint8_t digest[TR_RAFT_WIRE_DATA_DIGEST_SIZE])
{
    test_stream_sink_t *sink = (test_stream_sink_t *)context;
    (void)digest;
    if (leader_id != 1U || term != 3U || stream_id != 9U ||
        stream_size != TEST_STREAM_BYTES) return SALTS_EPROTO;
    sink->used = 0U;
    return SALTS_OK;
}

static int test_stream_write(
    void *context, uint64_t offset, const uint8_t *data, size_t size)
{
    test_stream_sink_t *sink = (test_stream_sink_t *)context;
    if (offset != sink->used || size > sizeof(sink->data) - sink->used)
        return SALTS_EPROTO;
    memcpy(sink->data + sink->used, data, size);
    sink->used += size;
    ++sink->writes;
    return SALTS_OK;
}

static int test_stream_commit(void *context)
{
    ((test_stream_sink_t *)context)->committed = true;
    return SALTS_OK;
}

static void test_stream_abort(void *context)
{
    ((test_stream_sink_t *)context)->aborted = true;
}

spec("raft data stream")
{
    it("encodes the committed stream descriptor inside one small Raft entry")
    {
        tr_raft_data_descriptor_t descriptor = {0};
        tr_raft_data_descriptor_t decoded;
        uint8_t encoded[TR_RAFT_DATA_DESCRIPTOR_ENCODED_SIZE];
        size_t encoded_size = 0U;

        descriptor.stream_id = 19U;
        descriptor.stream_size = TEST_STREAM_BYTES;
        memset(descriptor.stream_digest, 0x71,
               sizeof(descriptor.stream_digest));
        check_equal(tr_raft_data_descriptor_encode(
                         &descriptor, encoded, sizeof(encoded),
                         &encoded_size), SALTS_OK);
        check_equal(encoded_size, TR_RAFT_DATA_DESCRIPTOR_ENCODED_SIZE);
        check_less_equal(encoded_size, TR_RAFT_MAX_ENTRY_BYTES);
        check_equal(tr_raft_data_descriptor_decode(
                         encoded, encoded_size, &decoded), SALTS_OK);
        check_equal(decoded.stream_id, descriptor.stream_id);
        check_equal(decoded.stream_size, descriptor.stream_size);
        check_equal(decoded.stream_digest, descriptor.stream_digest,
                     sizeof(descriptor.stream_digest));
    }

    it("allows a descriptor proposal only after all replication targets are durable")
    {
        tr_raft_data_quorum_config_t config = {0};
        tr_raft_data_quorum_t *quorum = NULL;
        tr_raft_data_ack_t ack = {0};
        tr_raft_proposal_t proposal;
        uint8_t descriptor[TR_RAFT_DATA_DESCRIPTOR_ENCODED_SIZE];

        config.self_id = 1U;
        config.term = 7U;
        config.configuration.phase = TR_RAFT_CONF_FINAL;
        config.configuration.member_count = 3U;
        for (size_t index = 0U; index < 3U; ++index) {
            config.configuration.members[index].node_id = index + 1U;
            config.configuration.members[index].roles =
                TR_RAFT_CONF_OLD_VOTER | TR_RAFT_CONF_NEW_VOTER;
        }
        config.descriptor.stream_id = 22U;
        config.descriptor.stream_size = TEST_STREAM_BYTES;
        memset(config.descriptor.stream_digest, 0x4a,
               sizeof(config.descriptor.stream_digest));
        check_equal(tr_raft_data_quorum_create(&config, &quorum), SALTS_OK);
        check_equal(tr_raft_data_quorum_mark_local_durable(quorum), SALTS_OK);
        check_false(tr_raft_data_quorum_ready(quorum));
        check_equal(tr_raft_data_quorum_make_proposal(
                         quorum, 91U, descriptor, &proposal), SALTS_EBUSY);

        ack.from = 2U;
        ack.to = 1U;
        ack.term = config.term;
        ack.stream_id = config.descriptor.stream_id;
        ack.stream_size = config.descriptor.stream_size;
        ack.next_offset = ack.stream_size;
        ack.accepted = true;
        ack.durable = true;
        memcpy(ack.stream_digest, config.descriptor.stream_digest,
               sizeof(ack.stream_digest));
        check_equal(tr_raft_data_quorum_acknowledge(quorum, &ack), SALTS_OK);
        check_false(tr_raft_data_quorum_ready(quorum));
        ack.from = 3U;
        check_equal(tr_raft_data_quorum_acknowledge(quorum, &ack), SALTS_OK);
        check_true(tr_raft_data_quorum_ready(quorum));
        check_equal(tr_raft_data_quorum_make_proposal(
                         quorum, 91U, descriptor, &proposal), SALTS_OK);
        check_equal(proposal.command_id, 91U);
        check_equal(proposal.data_length,
                      TR_RAFT_DATA_DESCRIPTOR_ENCODED_SIZE);
        check_equal(proposal.data, descriptor);
        tr_raft_data_quorum_destroy(quorum);
    }

    it("checks joint majorities and stages every joint replication target")
    {
        tr_raft_data_quorum_config_t config = {0};
        tr_raft_data_quorum_t *quorum = NULL;
        tr_raft_data_ack_t ack = {0};
        const uint8_t roles[5] = {
            TR_RAFT_CONF_OLD_VOTER,
            TR_RAFT_CONF_OLD_VOTER,
            TR_RAFT_CONF_OLD_VOTER | TR_RAFT_CONF_NEW_VOTER,
            TR_RAFT_CONF_NEW_VOTER,
            TR_RAFT_CONF_NEW_VOTER};

        config.self_id = 3U;
        config.term = 8U;
        config.configuration.phase = TR_RAFT_CONF_JOINT;
        config.configuration.transition_id = 33U;
        config.configuration.member_count = 5U;
        for (size_t index = 0U; index < 5U; ++index) {
            config.configuration.members[index].node_id = index + 1U;
            config.configuration.members[index].roles = roles[index];
        }
        config.descriptor.stream_id = 24U;
        config.descriptor.stream_size = TEST_STREAM_BYTES;
        memset(config.descriptor.stream_digest, 0x5b,
               sizeof(config.descriptor.stream_digest));
        check_equal(tr_raft_data_quorum_create(&config, &quorum), SALTS_OK);
        check_equal(tr_raft_data_quorum_mark_local_durable(quorum), SALTS_OK);
        ack.to = 3U;
        ack.term = config.term;
        ack.stream_id = config.descriptor.stream_id;
        ack.stream_size = config.descriptor.stream_size;
        ack.next_offset = ack.stream_size;
        ack.accepted = true;
        ack.durable = true;
        memcpy(ack.stream_digest, config.descriptor.stream_digest,
               sizeof(ack.stream_digest));
        ack.from = 2U;
        check_equal(tr_raft_data_quorum_acknowledge(quorum, &ack), SALTS_OK);
        check_false(tr_raft_data_quorum_ready(quorum));
        ack.from = 4U;
        check_equal(tr_raft_data_quorum_acknowledge(quorum, &ack), SALTS_OK);
        check_false(tr_raft_data_quorum_ready(quorum));
        ack.from = 1U;
        check_equal(tr_raft_data_quorum_acknowledge(quorum, &ack), SALTS_OK);
        check_false(tr_raft_data_quorum_ready(quorum));
        ack.from = 5U;
        check_equal(tr_raft_data_quorum_acknowledge(quorum, &ack), SALTS_OK);
        check_true(tr_raft_data_quorum_ready(quorum));
        tr_raft_data_quorum_destroy(quorum);
    }

    it("moves 256 KiB through four 64 KiB chunks and one cumulative durable ack")
    {
        static uint8_t source[TEST_STREAM_BYTES];
        tr_raft_data_stream_sender_t *sender = NULL;
        tr_raft_data_stream_receiver_t *receiver = NULL;
        tr_raft_data_stream_sender_config_t sender_config = {0};
        tr_raft_data_stream_receiver_config_t receiver_config = {0};
        tr_raft_data_stream_sender_status_t status;
        tr_raft_data_stream_receive_result_t received;
        tr_raft_data_chunk_t chunks[4];
        test_stream_sink_t sink = {0};
        size_t index;

        for (index = 0U; index < sizeof(source); ++index)
            source[index] = (uint8_t)(index * 31U);
        sender_config.self_id = 1U;
        sender_config.peer_id = 2U;
        sender_config.max_stream_bytes = sizeof(source);
        sender_config.chunk_size = TR_RAFT_WIRE_MAX_DATA_CHUNK_BYTES;
        sender_config.max_inflight_chunks =
            TR_RAFT_DATA_STREAM_RECOMMENDED_INFLIGHT_CHUNKS;
        receiver_config.self_id = 2U;
        receiver_config.max_stream_bytes = sizeof(source);
        receiver_config.sink.begin = test_stream_begin;
        receiver_config.sink.write = test_stream_write;
        receiver_config.sink.commit = test_stream_commit;
        receiver_config.sink.abort = test_stream_abort;
        receiver_config.sink.context = &sink;

        check_equal(tr_raft_data_stream_sender_create(
                         &sender_config, &sender), SALTS_OK);
        check_equal(tr_raft_data_stream_receiver_create(
                         &receiver_config, &receiver), SALTS_OK);
        check_equal(tr_raft_data_stream_sender_begin(
                         sender, 3U, 9U, source, sizeof(source)), SALTS_OK);
        for (index = 0U; index < 4U; ++index) {
            check_equal(tr_raft_data_stream_sender_next(
                             sender, &chunks[index]), SALTS_OK);
            check_equal(chunks[index].data_length,
                          TR_RAFT_WIRE_MAX_DATA_CHUNK_BYTES);
        }
        check_equal(tr_raft_data_stream_sender_next(sender, &chunks[0]),
                     SALTS_EBUSY);
        for (index = 0U; index < 4U; ++index) {
            check_equal(tr_raft_data_stream_receiver_handle(
                             receiver, &chunks[index], &received), SALTS_OK);
        }
        check_true(received.committed);
        check_true(received.ack.durable);
        check_equal(received.ack.next_offset, sizeof(source));
        check_equal(tr_raft_data_stream_sender_acknowledge(
                         sender, &received.ack), SALTS_OK);
        check_equal(tr_raft_data_stream_sender_get_status(sender, &status),
                     SALTS_OK);
        check_true(status.complete);
        check_equal(sink.writes, 4U);
        check_equal(sink.used, sizeof(source));
        check_equal(sink.data, source, sizeof(source));
        check_true(sink.committed);
        check_false(sink.aborted);

        tr_raft_data_stream_receiver_destroy(receiver);
        tr_raft_data_stream_sender_destroy(sender);
    }

    it("streams 2 MiB through repeated 256 KiB windows")
    {
        enum { LARGE_STREAM_BYTES = 2U * 1024U * 1024U };
        static uint8_t source[LARGE_STREAM_BYTES];
        tr_raft_data_stream_sender_t *sender = NULL;
        tr_raft_data_stream_sender_config_t config = {0};
        tr_raft_data_stream_sender_status_t status;
        tr_raft_data_chunk_t chunks[4];
        tr_raft_data_ack_t ack;
        size_t window;
        size_t index;

        memset(source, 0x2f, sizeof(source));
        config.self_id = 1U;
        config.peer_id = 2U;
        config.max_stream_bytes = sizeof(source);
        config.chunk_size = TR_RAFT_WIRE_MAX_DATA_CHUNK_BYTES;
        config.max_inflight_chunks =
            TR_RAFT_DATA_STREAM_RECOMMENDED_INFLIGHT_CHUNKS;
        check_equal(tr_raft_data_stream_sender_create(&config, &sender),
                     SALTS_OK);
        check_equal(tr_raft_data_stream_sender_begin(
                         sender, 5U, 81U, source, sizeof(source)), SALTS_OK);
        for (window = 0U; window < 8U; ++window) {
            for (index = 0U; index < 4U; ++index) {
                check_equal(tr_raft_data_stream_sender_next(
                                 sender, &chunks[index]), SALTS_OK);
            }
            memset(&ack, 0, sizeof(ack));
            ack.from = 2U;
            ack.to = 1U;
            ack.term = 5U;
            ack.stream_id = 81U;
            ack.stream_size = sizeof(source);
            ack.next_offset =
                (window + 1U) * 4U * TR_RAFT_WIRE_MAX_DATA_CHUNK_BYTES;
            ack.accepted = true;
            ack.durable = ack.next_offset == sizeof(source);
            memcpy(ack.stream_digest, chunks[0].stream_digest,
                   sizeof(ack.stream_digest));
            check_equal(tr_raft_data_stream_sender_acknowledge(sender, &ack),
                         SALTS_OK);
        }
        check_equal(tr_raft_data_stream_sender_get_status(sender, &status),
                     SALTS_OK);
        check_true(status.complete);
        check_equal(status.acknowledged_offset, sizeof(source));
        tr_raft_data_stream_sender_destroy(sender);
    }
}
