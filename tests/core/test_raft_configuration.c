#include "raft_configuration.h"

#include <tinytest.h>
#include <salts_error.h>

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

        check_equal(tr_raft_conf_encode(&configuration, encoded,
                                         sizeof(encoded), &encoded_length),
                     SALTS_OK);
        check_equal(encoded_length, 43U);
        check_equal(tr_raft_conf_decode(encoded, encoded_length, &decoded),
                     SALTS_OK);
        check_equal(decoded.phase, TR_RAFT_CONF_FINAL);
        check_equal(decoded.transition_id, 41U);
        check_equal(decoded.member_count, 3U);
        check_equal(decoded.members[2].node_id, 4U);
        check_equal(decoded.members[2].roles, TR_RAFT_CONF_LEARNER);
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

        check_equal(tr_raft_conf_validate(&configuration), SALTS_OK);
        check_equal(tr_raft_conf_encode(&configuration, encoded,
                                         sizeof(encoded), &encoded_length),
                     SALTS_OK);
        check_equal(encoded_length, 43U);
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
        check_equal(tr_raft_conf_encode(&configuration, encoded,
                                         sizeof(encoded), &encoded_length),
                     SALTS_OK);

        encoded[0] = 'X';
        check_equal(tr_raft_conf_decode(encoded, encoded_length, &decoded),
                     SALTS_EPROTO);
        encoded[0] = 'T';
        encoded[7] = 1U;
        check_equal(tr_raft_conf_decode(encoded, encoded_length, &decoded),
                     SALTS_EPROTO);
        encoded[7] = 0U;
        encoded[24] = 0x80U;
        check_equal(tr_raft_conf_decode(encoded, encoded_length, &decoded),
                     SALTS_EPROTO);
        check_equal(tr_raft_conf_decode(encoded, encoded_length - 1U,
                                         &decoded),
                     SALTS_EPROTO);
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
        check_equal(tr_raft_conf_validate(&configuration), SALTS_EINVAL);

        configuration.phase = TR_RAFT_CONF_JOINT;
        configuration.members[0].node_id = 1U;
        configuration.members[0].roles =
            TR_RAFT_CONF_NEW_VOTER | TR_RAFT_CONF_LEARNER;
        configuration.members[1].node_id = 2U;
        check_equal(tr_raft_conf_validate(&configuration), SALTS_EINVAL);
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

        check_equal(tr_raft_conf_encode(&configuration, encoded,
                                         sizeof(encoded) - 1U,
                                         &encoded_length),
                     SALTS_ENOSPC);
        check_equal(encoded_length, TR_RAFT_CONF_MAX_ENCODED_SIZE);
        check_equal(tr_raft_conf_encode(&configuration, encoded,
                                         sizeof(encoded), &encoded_length),
                     SALTS_OK);
        check_equal(encoded_length, 295U);
    }
}
