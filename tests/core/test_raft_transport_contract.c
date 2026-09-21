#include <turboraft/raft_transport.h>

#include <salts_error.h>
#include <tinytest.h>

#include <string.h>

static int discard_payload(void *context,
                           const tr_raft_transport_payload_t *payload)
{
    (void)context;
    (void)payload;
    return SALTS_OK;
}

static tr_raft_handshake_result_t current_contract(void)
{
    tr_raft_handshake_result_t result;
    size_t index;

    memset(&result, 0, sizeof(result));
    result.complete = 1;
    for (index = 0U; index < sizeof(result.cluster_id.bytes); ++index) {
        result.cluster_id.bytes[index] = (uint8_t)(10U + index);
    }
    result.local_node_id = 1U;
    result.peer_node_id = 2U;
    result.peer_process_incarnation.bytes[0] = 1U;
    result.feature_bits = 0U;
    result.wire_major = TR_RAFT_HANDSHAKE_WIRE_MAJOR;
    result.wire_minor = TR_RAFT_HANDSHAKE_WIRE_MINOR;
    result.max_frame_size = TR_RAFT_WIRE_MAX_FRAME_SIZE;
    result.max_snapshot_chunk_size =
        TR_RAFT_WIRE_MAX_SNAPSHOT_CHUNK_BYTES;
    return result;
}

static int create_session(const tr_raft_handshake_result_t *handshake,
                          tr_raft_transport_session_t **out_session)
{
    tr_raft_transport_session_config_t config;
    tr_raft_handshake_result_t current;

    memset(&config, 0, sizeof(config));
    current = current_contract();
    config.cluster_id = current.cluster_id;
    config.local_node_id = current.local_node_id;
    config.peer_node_id = current.peer_node_id;
    config.first_outbound_message_id = 1U;
    config.handshake = handshake;
    config.on_payload = discard_payload;
    return tr_raft_transport_session_create(&config, out_session);
}

static tr_raft_message_t append_request(void)
{
    tr_raft_message_t message;

    memset(&message, 0, sizeof(message));
    message.type = TR_RAFT_MSG_APPEND_REQUEST;
    message.from = 1U;
    message.to = 2U;
    message.term = 4U;
    message.entry_count = 2U;
    message.entries[0].index = 1U;
    message.entries[0].term = 4U;
    message.entries[0].command_id = 1U;
    message.entries[1].index = 2U;
    message.entries[1].term = 4U;
    message.entries[1].command_id = 2U;
    return message;
}

spec("Raft transport contract")
{
    it("rejects missing and non-baseline contracts")
    {
        tr_raft_handshake_result_t invalid = current_contract();
        tr_raft_transport_session_t *session = NULL;

        check_equal(create_session(NULL, &session), SALTS_EINVAL);
        invalid.feature_bits = UINT64_C(1);
        check_equal(create_session(&invalid, &session),
                    SALTS_EPROTONOSUPPORT);
        check_null(session);
    }

    it("uses only the current mandatory group-aware wire")
    {
        tr_raft_handshake_result_t contract = current_contract();
        tr_raft_transport_session_t *session = NULL;
        tr_raft_message_t message = append_request();
        uint8_t packet[TR_RAFT_TRANSPORT_MAX_PACKET_SIZE];
        size_t packet_size = 0U;

        check_equal(create_session(&contract, &session), SALTS_OK);
        check_equal(tr_raft_transport_encode(session, 42U, &message, packet,
                                             sizeof(packet), &packet_size),
                    SALTS_OK);
        check(packet_size > TR_RAFT_TRANSPORT_LENGTH_PREFIX_SIZE);
        check_equal(packet[TR_RAFT_TRANSPORT_LENGTH_PREFIX_SIZE + 5U],
                    TR_RAFT_WIRE_VERSION);
        check_equal(packet[TR_RAFT_TRANSPORT_LENGTH_PREFIX_SIZE + 7U],
                    TR_RAFT_WIRE_HEADER_SIZE);
        check_equal(tr_raft_transport_session_destroy(session), SALTS_OK);
    }
}
