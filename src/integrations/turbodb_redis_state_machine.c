#include <turboraft/turbodb_redis_state_machine.h>

#include <redis/redis_lua_apply_batch.h>
#include <salts_error.h>

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define TR_TURBODB_REDIS_COMMAND_ID_TEXT_BYTES 21u

struct tr_turbodb_redis_state_machine {
    tr_turbodb_redis_state_machine_config_t config;
    size_t metadata_key_length;
    size_t journal_key_length;
    size_t identity_key_length;
    size_t outbox_key_length;
};

static int tr_turbodb_redis_apply(void *context,
                                  const tr_raft_entry_t *entries,
                                  size_t entry_count)
{
    tr_turbodb_redis_state_machine_t *state_machine =
        (tr_turbodb_redis_state_machine_t *)context;
    redis_lua_apply_batch_record *records = NULL;
    redis_lua_apply_batch_request request =
        REDIS_LUA_APPLY_BATCH_REQUEST_INIT;
    redis_lua_apply_batch operation = {0};
    redis_lua_apply_batch_step step;
    char *command_ids = NULL;
    size_t index;
    int result;

    if (state_machine == NULL || entries == NULL || entry_count == 0U ||
        entry_count > state_machine->config.max_batch_entries)
        return SALTS_EINVAL;
    records = (redis_lua_apply_batch_record *)calloc(entry_count,
                                                      sizeof(*records));
    command_ids = (char *)calloc(entry_count,
                                 TR_TURBODB_REDIS_COMMAND_ID_TEXT_BYTES);
    if (records == NULL || command_ids == NULL) {
        free(command_ids);
        free(records);
        return SALTS_ENOMEM;
    }
    for (index = 0U; index < entry_count; ++index) {
        int written;
        char *command_id = command_ids +
            index * TR_TURBODB_REDIS_COMMAND_ID_TEXT_BYTES;

        if (entries[index].command_id == 0U ||
            entries[index].data_length > TR_RAFT_MAX_ENTRY_BYTES) {
            free(command_ids);
            free(records);
            return SALTS_EINVAL;
        }
        written = snprintf(command_id, TR_TURBODB_REDIS_COMMAND_ID_TEXT_BYTES,
                           "%" PRIu64, entries[index].command_id);
        if (written < 0 ||
            (size_t)written >= TR_TURBODB_REDIS_COMMAND_ID_TEXT_BYTES) {
            free(command_ids);
            free(records);
            return SALTS_ERANGE;
        }
        records[index].index = entries[index].index;
        records[index].term = entries[index].term;
        records[index].command_id = command_id;
        records[index].command_id_length = (size_t)written;
        records[index].payload = (const char *)entries[index].data;
        records[index].payload_length = entries[index].data_length;
    }
    request.metadata_key = state_machine->config.metadata_key;
    request.metadata_key_length = state_machine->metadata_key_length;
    request.journal_key = state_machine->config.journal_key;
    request.journal_key_length = state_machine->journal_key_length;
    request.identity_key = state_machine->config.identity_key;
    request.identity_key_length = state_machine->identity_key_length;
    request.outbox_key = state_machine->config.outbox_key;
    request.outbox_key_length = state_machine->outbox_key_length;
    request.records = records;
    request.record_count = entry_count;
    result = redis_lua_apply_batch_open(state_machine->config.connection,
                                        &request, &operation);
    free(command_ids);
    free(records);
    if (result != SALTS_OK) return result;
    for (index = 0U; index < state_machine->config.max_wait_steps; ++index) {
        step = redis_lua_apply_batch_next(&operation);
        if (step.kind == REDIS_LUA_APPLY_BATCH_WAIT) {
            result = redis_io_runtime_wait_idle(
                state_machine->config.io_runtime,
                state_machine->config.wait_timeout_ns);
            if (result != SALTS_OK) break;
            continue;
        }
        result = step.kind == REDIS_LUA_APPLY_BATCH_DONE &&
                         (step.receipt.kind == REDIS_LUA_APPLY_APPLIED ||
                          step.receipt.kind == REDIS_LUA_APPLY_REPLAYED)
                     ? SALTS_OK
                     : step.receipt.status != SALTS_OK
                         ? step.receipt.status
                         : SALTS_EPROTO;
        break;
    }
    if (index == state_machine->config.max_wait_steps) result = SALTS_ETIMEDOUT;
    {
        int destroy_result = redis_lua_apply_batch_destroy(&operation);
        if (result == SALTS_OK) result = destroy_result;
    }
    return result;
}

int tr_turbodb_redis_state_machine_open(
    const tr_turbodb_redis_state_machine_config_t *config,
    tr_turbodb_redis_state_machine_t **out_state_machine)
{
    tr_turbodb_redis_state_machine_t *state_machine;
    if (config == NULL || out_state_machine == NULL || *out_state_machine != NULL ||
        config->connection == NULL || config->io_runtime == NULL ||
        config->wait_timeout_ns == 0U || config->max_wait_steps == 0U ||
        config->max_batch_entries == 0U ||
        config->max_batch_entries > REDIS_LUA_APPLY_BATCH_MAX_RECORDS ||
        config->metadata_key == NULL || config->journal_key == NULL ||
        config->identity_key == NULL || config->outbox_key == NULL)
        return SALTS_EINVAL;
    state_machine = (tr_turbodb_redis_state_machine_t *)calloc(
        1U, sizeof(*state_machine));
    if (state_machine == NULL) return SALTS_ENOMEM;
    state_machine->config = *config;
    state_machine->metadata_key_length = strlen(config->metadata_key);
    state_machine->journal_key_length = strlen(config->journal_key);
    state_machine->identity_key_length = strlen(config->identity_key);
    state_machine->outbox_key_length = strlen(config->outbox_key);
    if (state_machine->metadata_key_length == 0U ||
        state_machine->journal_key_length == 0U ||
        state_machine->identity_key_length == 0U ||
        state_machine->outbox_key_length == 0U) {
        free(state_machine);
        return SALTS_EINVAL;
    }
    *out_state_machine = state_machine;
    return SALTS_OK;
}

int tr_turbodb_redis_state_machine_close(
    tr_turbodb_redis_state_machine_t *state_machine)
{
    if (state_machine == NULL) return SALTS_EINVAL;
    free(state_machine);
    return SALTS_OK;
}

int tr_turbodb_redis_state_machine_bind(
    tr_turbodb_redis_state_machine_t *state_machine,
    tr_raft_state_machine_t *out_state_machine)
{
    if (state_machine == NULL || out_state_machine == NULL ||
        out_state_machine->apply_batch != NULL || out_state_machine->context != NULL)
        return SALTS_EINVAL;
    out_state_machine->context = state_machine;
    out_state_machine->apply_batch = tr_turbodb_redis_apply;
    return SALTS_OK;
}
