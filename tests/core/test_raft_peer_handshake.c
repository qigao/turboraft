#include <turboraft/raft_peer_handshake.h>

#include <tinytest.h>
#include <turbo_error.h>

#include <string.h>

static tr_raft_handshake_config_t make_config(uint8_t seed,
                                               tr_raft_node_id_t node_id,
                                               uint64_t features,
                                               uint32_t snapshot_size)
{
    tr_raft_handshake_config_t config;
    size_t index;

    memset(&config, 0, sizeof(config));
    for (index = 0U; index < sizeof(config.cluster_id.bytes); ++index) {
        config.cluster_id.bytes[index] = (uint8_t) (10U + index);
        config.process_incarnation.bytes[index] = (uint8_t) (seed + index);
    }
    config.local_node_id = node_id;
    config.config_epoch = 7U;
    config.feature_bits = features;
    config.wire_major_min = TR_RAFT_HANDSHAKE_WIRE_MAJOR;
    config.wire_major_max = TR_RAFT_HANDSHAKE_WIRE_MAJOR;
    config.wire_minor_min = TR_RAFT_HANDSHAKE_WIRE_MINOR;
    config.wire_minor_max = TR_RAFT_HANDSHAKE_WIRE_MINOR;
    config.max_frame_size = TR_RAFT_HANDSHAKE_MIN_FRAME_SIZE;
    config.max_snapshot_chunk_size = snapshot_size;
    return config;
}

spec("raft peer handshake")
{
    it("round trips and completes mutual negotiation")
    {
        tr_raft_handshake_config_t first_config =
            make_config(20U, 1U, 3U, 1024U * 1024U);
        tr_raft_handshake_config_t second_config =
            make_config(40U, 2U, 1U, 512U * 1024U);
        tr_raft_handshake_message_t first_hello;
        tr_raft_handshake_message_t second_hello;
        tr_raft_handshake_message_t decoded_second_hello;
        tr_raft_handshake_message_t first_ack;
        tr_raft_handshake_message_t second_ack;
        tr_raft_handshake_result_t first_result;
        tr_raft_handshake_result_t second_result;
        uint8_t packet[TR_RAFT_HANDSHAKE_PACKET_SIZE];
        size_t packet_size = 0U;

        check_equal(tr_raft_handshake_make_hello(&first_config, &first_hello),
                     TURBO_OK);
        check_equal(tr_raft_handshake_make_hello(&second_config,
                                                   &second_hello),
                     TURBO_OK);
        check_equal(tr_raft_handshake_encode(&second_hello, packet,
                                               sizeof(packet), &packet_size),
                     TURBO_OK);
        check_equal(packet_size, TR_RAFT_HANDSHAKE_PACKET_SIZE);
        check_equal(tr_raft_handshake_decode(packet, packet_size,
                                               &decoded_second_hello),
                     TURBO_OK);
        check_equal(tr_raft_handshake_negotiate(
                         &first_config, 2U, &decoded_second_hello, &first_ack,
                         &first_result),
                     TURBO_OK);
        check_equal(tr_raft_handshake_negotiate(
                         &second_config, 1U, &first_hello, &second_ack,
                         &second_result),
                     TURBO_OK);
        check_equal(first_result.feature_bits, 1U);
        check_equal(first_result.max_snapshot_chunk_size, 512U * 1024U);
        check_equal(tr_raft_handshake_validate_ack(&first_result, &second_ack),
                     TURBO_OK);
        check_equal(tr_raft_handshake_validate_ack(&second_result, &first_ack),
                     TURBO_OK);
        check_equal(tr_raft_handshake_result_validate(
                         &first_result, &first_config.cluster_id, 1U, 2U),
                     TURBO_OK);
    }

    it("rejects a claimed node that differs from TLS identity")
    {
        tr_raft_handshake_config_t local =
            make_config(60U, 1U,
                        TR_RAFT_HANDSHAKE_FEATURE_SNAPSHOT_CONF_STATE,
                        1024U * 1024U);
        tr_raft_handshake_config_t remote =
            make_config(80U, 2U,
                        TR_RAFT_HANDSHAKE_FEATURE_SNAPSHOT_CONF_STATE,
                        1024U * 1024U);
        tr_raft_handshake_message_t remote_hello;
        tr_raft_handshake_message_t local_ack;
        tr_raft_handshake_result_t result;

        check_equal(tr_raft_handshake_make_hello(&remote, &remote_hello),
                     TURBO_OK);
        check_equal(tr_raft_handshake_negotiate(
                         &local, 3U, &remote_hello, &local_ack, &result),
                     TURBO_EPROTO);
    }

    it("rejects malformed packet bounds and reserved fields")
    {
        tr_raft_handshake_config_t config =
            make_config(100U, 1U,
                        TR_RAFT_HANDSHAKE_FEATURE_SNAPSHOT_CONF_STATE,
                        1024U * 1024U);
        tr_raft_handshake_message_t hello;
        tr_raft_handshake_message_t decoded;
        uint8_t packet[TR_RAFT_HANDSHAKE_PACKET_SIZE];
        size_t packet_size = 0U;

        check_equal(tr_raft_handshake_make_hello(&config, &hello), TURBO_OK);
        check_equal(tr_raft_handshake_encode(&hello, packet, sizeof(packet),
                                               &packet_size),
                     TURBO_OK);
        check_equal(tr_raft_handshake_decode(packet, packet_size - 1U,
                                               &decoded),
                     TURBO_EPROTO);
        packet[TR_RAFT_HANDSHAKE_PACKET_SIZE - 1U] = 1U;
        check_equal(tr_raft_handshake_decode(packet, packet_size, &decoded),
                     TURBO_EPROTO);
    }

    it("rejects a peer without snapshot ConfState support")
    {
        tr_raft_handshake_config_t legacy =
            make_config(110U, 2U, 0U, 1024U * 1024U);
        tr_raft_handshake_message_t hello;

        check_equal(tr_raft_handshake_make_hello(&legacy, &hello),
                     TURBO_EPROTO);
    }

    it("streams fragmented and coalesced handshake packets")
    {
        tr_raft_handshake_config_t first_config =
            make_config(120U, 1U, 1U, 1024U * 1024U);
        tr_raft_handshake_config_t second_config =
            make_config(140U, 2U, 1U, 1024U * 1024U);
        tr_raft_handshake_exchange_t *first = NULL;
        tr_raft_handshake_exchange_t *second = NULL;
        tr_raft_handshake_result_t result;
        tr_raft_handshake_exchange_state_t state;
        uint8_t first_hello[TR_RAFT_HANDSHAKE_PACKET_SIZE];
        uint8_t second_hello[TR_RAFT_HANDSHAKE_PACKET_SIZE];
        uint8_t first_ack[TR_RAFT_HANDSHAKE_PACKET_SIZE];
        uint8_t second_ack[TR_RAFT_HANDSHAKE_PACKET_SIZE];
        uint8_t combined[TR_RAFT_HANDSHAKE_PACKET_SIZE * 2U];
        size_t first_hello_size = 0U;
        size_t second_hello_size = 0U;
        size_t first_ack_size = 0U;
        size_t second_ack_size = 0U;
        size_t consumed = 0U;

        check_equal(tr_raft_handshake_exchange_create(&first_config, 2U,
                                                        &first),
                     TURBO_OK);
        check_equal(tr_raft_handshake_exchange_create(&second_config, 1U,
                                                        &second),
                     TURBO_OK);
        check_equal(tr_raft_handshake_exchange_start(
                         first, first_hello, sizeof(first_hello),
                         &first_hello_size),
                     TURBO_OK);
        check_equal(tr_raft_handshake_exchange_start(
                         second, second_hello, sizeof(second_hello),
                         &second_hello_size),
                     TURBO_OK);

        check_equal(tr_raft_handshake_exchange_feed(
                         first, second_hello, 3U, &consumed, first_ack,
                         sizeof(first_ack), &first_ack_size),
                     TURBO_OK);
        check_equal(consumed, 3U);
        check_equal(first_ack_size, 0U);
        check_equal(tr_raft_handshake_exchange_feed(
                         first, second_hello + 3U, second_hello_size - 3U,
                         &consumed, first_ack, sizeof(first_ack),
                         &first_ack_size),
                     TURBO_OK);
        check_equal(first_ack_size, TR_RAFT_HANDSHAKE_PACKET_SIZE);

        memcpy(combined, first_hello, first_hello_size);
        memcpy(combined + first_hello_size, first_ack, first_ack_size);
        check_equal(tr_raft_handshake_exchange_feed(
                         second, combined, first_hello_size + first_ack_size,
                         &consumed, second_ack, sizeof(second_ack),
                         &second_ack_size),
                     TURBO_OK);
        check_equal(consumed, first_hello_size + first_ack_size);
        check_equal(second_ack_size, TR_RAFT_HANDSHAKE_PACKET_SIZE);
        check_equal(tr_raft_handshake_exchange_get_state(second, &state),
                     TURBO_OK);
        check_equal(state, TR_RAFT_HANDSHAKE_EXCHANGE_COMPLETE);

        memcpy(combined, second_ack, second_ack_size);
        memset(combined + second_ack_size, 0xA5, 5U);
        check_equal(tr_raft_handshake_exchange_feed(
                         first, combined, second_ack_size + 5U, &consumed,
                         first_ack, sizeof(first_ack), &first_ack_size),
                     TURBO_OK);
        check_equal(consumed, second_ack_size);
        check_equal(tr_raft_handshake_exchange_get_result(first, &result),
                     TURBO_OK);
        check_equal(result.complete, 1);

        tr_raft_handshake_exchange_destroy(second);
        tr_raft_handshake_exchange_destroy(first);
    }
}
