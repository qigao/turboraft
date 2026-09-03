#include "raft_configuration.h"

#include <salts_error.h>

#include <stdbool.h>
#include <string.h>

static const uint8_t tr_raft_conf_magic[4] = {'T', 'R', 'C', 'F'};

static void tr_raft_conf_put_u64(uint8_t *output, uint64_t value)
{
    size_t index;

    for (index = 0U; index < 8U; ++index) {
        output[7U - index] = (uint8_t) (value & UINT64_C(0xff));
        value >>= 8U;
    }
}

static uint64_t tr_raft_conf_get_u64(const uint8_t *input)
{
    uint64_t value = 0U;
    size_t index;

    for (index = 0U; index < 8U; ++index) {
        value = (value << 8U) | input[index];
    }
    return value;
}

static bool tr_raft_conf_roles_valid(tr_raft_conf_phase_t phase,
                                     uint8_t roles)
{
    const uint8_t voter_roles = TR_RAFT_CONF_OLD_VOTER |
                                TR_RAFT_CONF_NEW_VOTER;
    const uint8_t all_roles = voter_roles | TR_RAFT_CONF_LEARNER;

    if (roles == 0U || (roles & (uint8_t) ~all_roles) != 0U ||
        (roles & TR_RAFT_CONF_NEW_VOTER) != 0U &&
            (roles & TR_RAFT_CONF_LEARNER) != 0U) {
        return false;
    }
    if (phase == TR_RAFT_CONF_FINAL) {
        return roles == voter_roles || roles == TR_RAFT_CONF_LEARNER;
    }
    return phase == TR_RAFT_CONF_JOINT;
}

int tr_raft_conf_validate(const tr_raft_conf_t *configuration)
{
    size_t old_voter_count = 0U;
    size_t new_voter_count = 0U;
    size_t index;

    if (configuration == NULL ||
        (configuration->phase == TR_RAFT_CONF_JOINT &&
         configuration->transition_id == 0U) ||
        configuration->member_count == 0U ||
        configuration->member_count > TR_RAFT_MAX_MEMBERS ||
        (configuration->phase != TR_RAFT_CONF_JOINT &&
         configuration->phase != TR_RAFT_CONF_FINAL)) {
        return SALTS_EINVAL;
    }
    for (index = 0U; index < configuration->member_count; ++index) {
        const tr_raft_conf_member_t *member = &configuration->members[index];

        if (member->node_id == 0U ||
            (index != 0U &&
             configuration->members[index - 1U].node_id >= member->node_id) ||
            !tr_raft_conf_roles_valid(configuration->phase, member->roles)) {
            return SALTS_EINVAL;
        }
        old_voter_count +=
            (member->roles & TR_RAFT_CONF_OLD_VOTER) != 0U;
        new_voter_count +=
            (member->roles & TR_RAFT_CONF_NEW_VOTER) != 0U;
    }
    return old_voter_count != 0U && new_voter_count != 0U
               ? SALTS_OK
               : SALTS_EINVAL;
}

int tr_raft_conf_encode(const tr_raft_conf_t *configuration,
                        uint8_t *output,
                        size_t output_capacity,
                        size_t *output_length)
{
    size_t required;
    size_t index;

    if (output_length != NULL) {
        *output_length = 0U;
    }
    if (configuration == NULL || output == NULL || output_length == NULL) {
        return SALTS_EINVAL;
    }
    if (tr_raft_conf_validate(configuration) != SALTS_OK) {
        return SALTS_EINVAL;
    }
    required = TR_RAFT_CONF_HEADER_SIZE +
               TR_RAFT_CONF_MEMBER_SIZE * configuration->member_count;
    *output_length = required;
    if (output_capacity < required) {
        return SALTS_ENOSPC;
    }

    memset(output, 0, required);
    memcpy(output, tr_raft_conf_magic, sizeof(tr_raft_conf_magic));
    output[4] = TR_RAFT_CONF_CODEC_VERSION;
    output[5] = (uint8_t) configuration->phase;
    output[6] = (uint8_t) configuration->member_count;
    tr_raft_conf_put_u64(output + 8U, configuration->transition_id);
    for (index = 0U; index < configuration->member_count; ++index) {
        size_t offset = TR_RAFT_CONF_HEADER_SIZE +
                        TR_RAFT_CONF_MEMBER_SIZE * index;

        tr_raft_conf_put_u64(output + offset,
                             configuration->members[index].node_id);
        output[offset + 8U] = configuration->members[index].roles;
    }
    return SALTS_OK;
}

int tr_raft_conf_decode(const uint8_t *input,
                        size_t input_length,
                        tr_raft_conf_t *configuration)
{
    size_t expected_length;
    size_t member_count;
    size_t index;

    if (input == NULL || configuration == NULL) {
        return SALTS_EINVAL;
    }
    memset(configuration, 0, sizeof(*configuration));
    if (input_length < TR_RAFT_CONF_HEADER_SIZE ||
        memcmp(input, tr_raft_conf_magic, sizeof(tr_raft_conf_magic)) != 0 ||
        input[4] != TR_RAFT_CONF_CODEC_VERSION || input[7] != 0U) {
        return SALTS_EPROTO;
    }
    member_count = input[6];
    if (member_count == 0U || member_count > TR_RAFT_MAX_MEMBERS) {
        return SALTS_EPROTO;
    }
    expected_length = TR_RAFT_CONF_HEADER_SIZE +
                      TR_RAFT_CONF_MEMBER_SIZE * member_count;
    if (input_length != expected_length) {
        return SALTS_EPROTO;
    }

    configuration->phase = (tr_raft_conf_phase_t) input[5];
    configuration->transition_id = tr_raft_conf_get_u64(input + 8U);
    configuration->member_count = member_count;
    for (index = 0U; index < member_count; ++index) {
        size_t offset = TR_RAFT_CONF_HEADER_SIZE +
                        TR_RAFT_CONF_MEMBER_SIZE * index;

        configuration->members[index].node_id =
            tr_raft_conf_get_u64(input + offset);
        configuration->members[index].roles = input[offset + 8U];
    }
    if (tr_raft_conf_validate(configuration) != SALTS_OK) {
        memset(configuration, 0, sizeof(*configuration));
        return SALTS_EPROTO;
    }
    return SALTS_OK;
}

bool tr_raft_conf_entry_is_configuration(const tr_raft_entry_t *entry)
{
    return entry != NULL && entry->command_id == 0U;
}

int tr_raft_conf_entry_encode(const tr_raft_conf_t *configuration,
                              tr_raft_index_t index,
                              tr_raft_term_t term,
                              tr_raft_entry_t *entry)
{
    size_t encoded_length = 0U;
    int result;

    if (configuration == NULL || index == 0U || term == 0U || entry == NULL) {
        return SALTS_EINVAL;
    }
    memset(entry, 0, sizeof(*entry));
    result = tr_raft_conf_encode(configuration, entry->data,
                                 sizeof(entry->data), &encoded_length);
    if (result != SALTS_OK) {
        memset(entry, 0, sizeof(*entry));
        return result;
    }
    entry->index = index;
    entry->term = term;
    entry->data_length = encoded_length;
    return SALTS_OK;
}

int tr_raft_conf_entry_decode(const tr_raft_entry_t *entry,
                              tr_raft_conf_t *configuration)
{
    if (entry == NULL || configuration == NULL) {
        return SALTS_EINVAL;
    }
    if (!tr_raft_conf_entry_is_configuration(entry)) {
        return SALTS_ENOENT;
    }
    if (entry->index == 0U || entry->term == 0U ||
        entry->data_length > sizeof(entry->data)) {
        return SALTS_EPROTO;
    }
    return tr_raft_conf_decode(entry->data, entry->data_length,
                               configuration);
}
