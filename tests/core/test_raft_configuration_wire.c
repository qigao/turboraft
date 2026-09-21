#include "raft_configuration.h"

#include <turboraft/raft_wire_codec.h>

#include <tinytest.h>
#include <salts_error.h>

#include <string.h>

static void configuration_wire_fixture(tr_raft_conf_t *configuration,
                                       tr_raft_message_t *message)
{
    memset(configuration, 0, sizeof(*configuration));
    configuration->phase = TR_RAFT_CONF_JOINT;
    configuration->transition_id = 1001U;
    configuration->member_count = 3U;
    configuration->members[0].node_id = 1U;
    configuration->members[0].roles =
        TR_RAFT_CONF_OLD_VOTER | TR_RAFT_CONF_NEW_VOTER;
    configuration->members[1].node_id = 2U;
    configuration->members[1].roles = TR_RAFT_CONF_OLD_VOTER;
    configuration->members[2].node_id = 3U;
    configuration->members[2].roles = TR_RAFT_CONF_NEW_VOTER;

    memset(message, 0, sizeof(*message));
    message->type = TR_RAFT_MSG_APPEND_REQUEST;
    message->from = 1U;
    message->to = 2U;
    message->term = 7U;
    message->previous_log_index = 8U;
    message->previous_log_term = 6U;
    message->entry_count = 1U;
    check_equal(tr_raft_conf_entry_encode(configuration, 9U, 7U,
                                           &message->entry),
                 SALTS_OK);
}

spec("raft configuration wire contract")
{
    it("round trips a configuration entry through the current wire contract")
    {
        tr_raft_wire_codec_t *codec = NULL;
        tr_raft_wire_metadata_t metadata;
        tr_raft_wire_metadata_t decoded_metadata;
        tr_raft_conf_t configuration;
        tr_raft_conf_t decoded_configuration;
        tr_raft_message_t message;
        tr_raft_message_t decoded;
        uint8_t frame[TR_RAFT_WIRE_MAX_FRAME_SIZE];
        size_t frame_length = 0U;
        uint16_t wire_version = 0U;

        memset(&metadata, 0, sizeof(metadata));
        metadata.group_id = 1U;
        metadata.message_id = 7001U;
        configuration_wire_fixture(&configuration, &message);
        check_equal(tr_raft_wire_codec_create(&codec), SALTS_OK);
        check_equal(tr_raft_wire_encode(codec, &metadata, &message, frame,
                                         sizeof(frame), &frame_length),
                     SALTS_OK);
        check_equal(tr_raft_wire_peek_version(frame, frame_length,
                                                &wire_version),
                     SALTS_OK);
        check_equal(wire_version, TR_RAFT_WIRE_VERSION);
        check_equal(tr_raft_wire_decode(codec, frame, frame_length,
                                         &decoded_metadata, &decoded),
                     SALTS_OK);
        check_equal(decoded_metadata.message_id, 7001U);
        check_equal(decoded.entry.command_id, 0U);
        check_equal(tr_raft_conf_entry_decode(&decoded.entry,
                                               &decoded_configuration),
                     SALTS_OK);
        check_equal(decoded_configuration.phase, TR_RAFT_CONF_JOINT);
        check_equal(decoded_configuration.transition_id, 1001U);
        tr_raft_wire_codec_destroy(codec);
    }


    it("rejects a malformed command zero entry at the wire boundary")
    {
        tr_raft_wire_codec_t *codec = NULL;
        tr_raft_wire_metadata_t metadata;
        tr_raft_conf_t configuration;
        tr_raft_message_t message;
        uint8_t frame[TR_RAFT_WIRE_MAX_FRAME_SIZE];
        size_t frame_length = 0U;

        memset(&metadata, 0, sizeof(metadata));
        configuration_wire_fixture(&configuration, &message);
        message.entry.data[0] = 'X';
        check_equal(tr_raft_wire_codec_create(&codec), SALTS_OK);
        check_equal(tr_raft_wire_encode(codec, &metadata, &message, frame,
                                         sizeof(frame), &frame_length),
                     SALTS_EINVAL);

        memset(&message.entry, 0, sizeof(message.entry));
        message.entry.index = 9U;
        message.entry.term = 7U;
        check_equal(tr_raft_wire_encode(codec, &metadata, &message, frame,
                                         sizeof(frame), &frame_length),
                     SALTS_EINVAL);
        tr_raft_wire_codec_destroy(codec);
    }
}
