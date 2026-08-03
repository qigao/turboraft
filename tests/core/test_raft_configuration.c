#include "raft_configuration.h"

#include <tinytest.h>
#include <turbo_error.h>

#include <string.h>

spec("raft configuration codec")
{
    it("round trips a canonical final configuration")
    {
        tr_raft_conf_t configuration;
        tr_raft_conf_t decoded;
        uint8_t encoded[TR_RAFT_CONF_MAX_ENCODED_SIZE];
        size_t encoded_length = 0U;

        memset(&configuration, 0, sizeof(configuration));
        configuration.phase = TR_RAFT_CONF_FINAL;
        configuration.transition_id = 41U;
        configuration.member_count = 3U;
        configuration.members[0].node_id = 1U;
        configuration.members[0].roles =
            TR_RAFT_CONF_OLD_VOTER | TR_RAFT_CONF_NEW_VOTER;
        configuration.members[1].node_id = 2U;
        configuration.members[1].roles =
            TR_RAFT_CONF_OLD_VOTER | TR_RAFT_CONF_NEW_VOTER;
        configuration.members[2].node_id = 4U;
        configuration.members[2].roles = TR_RAFT_CONF_LEARNER;

        check_int_eq(tr_raft_conf_encode(&configuration, encoded,
                                         sizeof(encoded), &encoded_length),
                     TURBO_OK);
        check_size_eq(encoded_length, 43U);
        check_int_eq(tr_raft_conf_decode(encoded, encoded_length, &decoded),
                     TURBO_OK);
        check_int_eq(decoded.phase, TR_RAFT_CONF_FINAL);
        check_long_eq(decoded.transition_id, 41U);
        check_size_eq(decoded.member_count, 3U);
        check_long_eq(decoded.members[2].node_id, 4U);
        check_int_eq(decoded.members[2].roles, TR_RAFT_CONF_LEARNER);
    }

    it("represents promotion and demotion during a joint transition")
    {
        tr_raft_conf_t configuration;
        uint8_t encoded[TR_RAFT_CONF_MAX_ENCODED_SIZE];
        size_t encoded_length = 0U;

        memset(&configuration, 0, sizeof(configuration));
        configuration.phase = TR_RAFT_CONF_JOINT;
        configuration.transition_id = 42U;
        configuration.member_count = 3U;
        configuration.members[0].node_id = 1U;
        configuration.members[0].roles =
            TR_RAFT_CONF_OLD_VOTER | TR_RAFT_CONF_NEW_VOTER;
        configuration.members[1].node_id = 2U;
        configuration.members[1].roles =
            TR_RAFT_CONF_OLD_VOTER | TR_RAFT_CONF_LEARNER;
        configuration.members[2].node_id = 3U;
        configuration.members[2].roles = TR_RAFT_CONF_NEW_VOTER;

        check_int_eq(tr_raft_conf_validate(&configuration), TURBO_OK);
        check_int_eq(tr_raft_conf_encode(&configuration, encoded,
                                         sizeof(encoded), &encoded_length),
                     TURBO_OK);
        check_size_eq(encoded_length, 43U);
    }

    it("rejects malformed and non-canonical bytes")
    {
        tr_raft_conf_t configuration;
        tr_raft_conf_t decoded;
        uint8_t encoded[TR_RAFT_CONF_MAX_ENCODED_SIZE];
        size_t encoded_length = 0U;

        memset(&configuration, 0, sizeof(configuration));
        configuration.phase = TR_RAFT_CONF_FINAL;
        configuration.transition_id = 43U;
        configuration.member_count = 1U;
        configuration.members[0].node_id = 1U;
        configuration.members[0].roles =
            TR_RAFT_CONF_OLD_VOTER | TR_RAFT_CONF_NEW_VOTER;
        check_int_eq(tr_raft_conf_encode(&configuration, encoded,
                                         sizeof(encoded), &encoded_length),
                     TURBO_OK);

        encoded[0] = 'X';
        check_int_eq(tr_raft_conf_decode(encoded, encoded_length, &decoded),
                     TURBO_EPROTO);
        encoded[0] = 'T';
        encoded[7] = 1U;
        check_int_eq(tr_raft_conf_decode(encoded, encoded_length, &decoded),
                     TURBO_EPROTO);
        encoded[7] = 0U;
        encoded[24] = 0x80U;
        check_int_eq(tr_raft_conf_decode(encoded, encoded_length, &decoded),
                     TURBO_EPROTO);
        check_int_eq(tr_raft_conf_decode(encoded, encoded_length - 1U,
                                         &decoded),
                     TURBO_EPROTO);
    }

    it("rejects invalid in-memory role and ordering combinations")
    {
        tr_raft_conf_t configuration;

        memset(&configuration, 0, sizeof(configuration));
        configuration.phase = TR_RAFT_CONF_FINAL;
        configuration.transition_id = 44U;
        configuration.member_count = 2U;
        configuration.members[0].node_id = 2U;
        configuration.members[0].roles = TR_RAFT_CONF_OLD_VOTER;
        configuration.members[1].node_id = 1U;
        configuration.members[1].roles = TR_RAFT_CONF_NEW_VOTER;
        check_int_eq(tr_raft_conf_validate(&configuration), TURBO_EINVAL);

        configuration.phase = TR_RAFT_CONF_JOINT;
        configuration.members[0].node_id = 1U;
        configuration.members[0].roles =
            TR_RAFT_CONF_NEW_VOTER | TR_RAFT_CONF_LEARNER;
        configuration.members[1].node_id = 2U;
        check_int_eq(tr_raft_conf_validate(&configuration), TURBO_EINVAL);
    }

    it("encodes the bounded 31 member maximum")
    {
        tr_raft_conf_t configuration;
        uint8_t encoded[TR_RAFT_CONF_MAX_ENCODED_SIZE];
        size_t encoded_length = 0U;
        size_t index;

        memset(&configuration, 0, sizeof(configuration));
        configuration.phase = TR_RAFT_CONF_FINAL;
        configuration.transition_id = 45U;
        configuration.member_count = TR_RAFT_MAX_MEMBERS;
        for (index = 0U; index < configuration.member_count; ++index) {
            configuration.members[index].node_id = index + 1U;
            configuration.members[index].roles =
                TR_RAFT_CONF_OLD_VOTER | TR_RAFT_CONF_NEW_VOTER;
        }

        check_int_eq(tr_raft_conf_encode(&configuration, encoded,
                                         sizeof(encoded) - 1U,
                                         &encoded_length),
                     TURBO_ENOSPC);
        check_size_eq(encoded_length, TR_RAFT_CONF_MAX_ENCODED_SIZE);
        check_int_eq(tr_raft_conf_encode(&configuration, encoded,
                                         sizeof(encoded), &encoded_length),
                     TURBO_OK);
        check_size_eq(encoded_length, 295U);
    }
}
