#include "raft_log.h"
#include "../turboraft_stl_status.h"

#include <salts_error.h>

#include <limits.h>
#include <string.h>

static bool tr_raft_log_base_valid(tr_raft_log_index_t index,
                                   tr_raft_log_term_t term)
{
    return (index == 0U && term == 0U) || (index != 0U && term != 0U);
}

static bool tr_raft_log_entry_equal(const tr_raft_log_entry_t *left,
                                    const tr_raft_log_entry_t *right)
{
    return left->index == right->index && left->term == right->term &&
           left->command_id == right->command_id &&
           left->data_length == right->data_length &&
           memcmp(left->data, right->data, left->data_length) == 0;
}

static tr_raft_log_index_t tr_raft_log_reject_hint(
    const tr_raft_log_t *log,
    tr_raft_log_index_t previous_index)
{
    tr_raft_log_index_t last_index = tr_raft_log_last_index(log);
    const tr_raft_log_entry_t *conflict;
    tr_raft_log_term_t conflict_term;

    if (previous_index > last_index) {
        return last_index + 1U;
    }
    if (previous_index <= log->base_index) {
        return log->base_index + 1U;
    }

    conflict = tr_raft_log_get(log, previous_index);
    conflict_term = conflict->term;
    while (previous_index > log->base_index + 1U) {
        const tr_raft_log_entry_t *prior =
            tr_raft_log_get(log, previous_index - 1U);

        if (prior->term != conflict_term) {
            break;
        }
        --previous_index;
    }
    return previous_index;
}

int tr_raft_log_init(tr_raft_log_t *log,
                     size_t max_entries,
                     tr_raft_log_index_t base_index,
                     tr_raft_log_term_t base_term)
{
    int result;

    if (log == NULL || max_entries == 0U ||
        !tr_raft_log_base_valid(base_index, base_term) ||
        max_entries > UINT64_MAX - base_index) {
        return SALTS_EINVAL;
    }

    memset(log, 0, sizeof(*log));
    result = tr_raft_stl_status_to_error(vec_init_bytes(
        &log->entries, sizeof(tr_raft_log_entry_t),
        _Alignof(tr_raft_log_entry_t), max_entries));
    if (result != SALTS_OK) {
        return result;
    }
    result = tr_raft_stl_status_to_error(vec_reserve(&log->entries, max_entries));
    if (result != SALTS_OK) {
        vec_destroy(&log->entries);
        memset(log, 0, sizeof(*log));
        return result;
    }
    log->max_entries = max_entries;
    log->base_index = base_index;
    log->base_term = base_term;
    return SALTS_OK;
}

void tr_raft_log_destroy(tr_raft_log_t *log)
{
    if (log == NULL) {
        return;
    }
    vec_destroy(&log->entries);
    memset(log, 0, sizeof(*log));
}

size_t tr_raft_log_count(const tr_raft_log_t *log)
{
    return log == NULL ? 0U : vec_size(&log->entries);
}

tr_raft_log_index_t tr_raft_log_last_index(const tr_raft_log_t *log)
{
    return log == NULL ? 0U : log->base_index + tr_raft_log_count(log);
}

tr_raft_log_term_t tr_raft_log_last_term(const tr_raft_log_t *log)
{
    const tr_raft_log_entry_t *entry;

    if (log == NULL || tr_raft_log_count(log) == 0U) {
        return log == NULL ? 0U : log->base_term;
    }
    entry = (const tr_raft_log_entry_t *)vec_at_const(
        &log->entries, tr_raft_log_count(log) - 1U);
    return entry->term;
}

const tr_raft_log_entry_t *tr_raft_log_get(const tr_raft_log_t *log,
                                           tr_raft_log_index_t index)
{
    tr_raft_log_index_t offset;

    if (log == NULL || index <= log->base_index ||
        index > tr_raft_log_last_index(log)) {
        return NULL;
    }
    offset = index - log->base_index - 1U;
    return (const tr_raft_log_entry_t *)vec_at_const(
        &log->entries, (size_t)offset);
}

bool tr_raft_log_matches(const tr_raft_log_t *log,
                         tr_raft_log_index_t index,
                         tr_raft_log_term_t term)
{
    const tr_raft_log_entry_t *entry;

    if (log == NULL) {
        return false;
    }
    if (index == log->base_index) {
        return term == log->base_term;
    }
    entry = tr_raft_log_get(log, index);
    return entry != NULL && entry->term == term;
}

int tr_raft_log_compact(tr_raft_log_t *log,
                        tr_raft_log_index_t index,
                        tr_raft_log_term_t term)
{
    size_t removed_count;
    size_t retained_count;
    tr_raft_log_entry_t *entries;
    int result;

    if (log == NULL || index <= log->base_index || term == 0U ||
        index > tr_raft_log_last_index(log)) {
        return SALTS_EINVAL;
    }
    if (!tr_raft_log_matches(log, index, term)) {
        return SALTS_EPROTO;
    }

    removed_count = (size_t) (index - log->base_index);
    retained_count = tr_raft_log_count(log) - removed_count;
    entries = (tr_raft_log_entry_t *)vec_at(&log->entries, 0U);
    if (retained_count != 0U) {
        memmove(entries, entries + removed_count,
                retained_count * sizeof(*entries));
    }
    result = tr_raft_stl_status_to_error(vec_resize(&log->entries, retained_count));
    if (result != SALTS_OK) {
        return result;
    }
    log->base_index = index;
    log->base_term = term;
    return SALTS_OK;
}

int tr_raft_log_append_local(tr_raft_log_t *log,
                             tr_raft_log_term_t term,
                             uint64_t command_id,
                             const void *data,
                             size_t data_length,
                             const tr_raft_log_entry_t **out_entry)
{
    tr_raft_log_entry_t entry;
    int result;

    if (log == NULL || term == 0U || command_id == 0U ||
        (data_length != 0U && data == NULL)) {
        return SALTS_EINVAL;
    }
    if (data_length > TR_RAFT_LOG_MAX_ENTRY_BYTES ||
        tr_raft_log_count(log) >= log->max_entries) {
        return SALTS_ENOSPC;
    }

    memset(&entry, 0, sizeof(entry));
    entry.index = tr_raft_log_last_index(log) + 1U;
    entry.term = term;
    entry.command_id = command_id;
    entry.data_length = data_length;
    if (data_length != 0U) {
        memcpy(entry.data, data, data_length);
    }

    result = tr_raft_stl_status_to_error(vec_push(&log->entries, &entry));
    if (result != SALTS_OK) {
        return result;
    }
    if (out_entry != NULL) {
        *out_entry = tr_raft_log_get(log, entry.index);
    }
    return SALTS_OK;
}

int tr_raft_log_append_configuration(
    tr_raft_log_t *log,
    tr_raft_log_term_t term,
    const tr_raft_conf_t *configuration,
    const tr_raft_log_entry_t **out_entry)
{
    tr_raft_log_entry_t entry;
    int result;

    if (log == NULL || term == 0U || configuration == NULL) {
        return SALTS_EINVAL;
    }
    if (tr_raft_log_count(log) >= log->max_entries) {
        return SALTS_ENOSPC;
    }
    result = tr_raft_conf_entry_encode(
        configuration, tr_raft_log_last_index(log) + 1U, term, &entry);
    if (result != SALTS_OK) {
        return result;
    }
    result = tr_raft_stl_status_to_error(vec_push(&log->entries, &entry));
    if (result != SALTS_OK) {
        return result;
    }
    if (out_entry != NULL) {
        *out_entry = tr_raft_log_get(log, entry.index);
    }
    return SALTS_OK;
}

int tr_raft_log_reconcile(tr_raft_log_t *log,
                          tr_raft_log_index_t previous_index,
                          tr_raft_log_term_t previous_term,
                          const tr_raft_log_entry_t *incoming,
                          size_t incoming_count,
                          tr_raft_log_index_t protected_index,
                          tr_raft_log_reconcile_result_t *result)
{
    size_t first_change = incoming_count;
    size_t retained_count;
    size_t final_count;
    size_t index;
    bool truncates_existing = false;
    int resize_result;

    if (log == NULL || result == NULL ||
        (incoming_count != 0U && incoming == NULL) ||
        incoming_count > UINT64_MAX - previous_index) {
        return SALTS_EINVAL;
    }
    memset(result, 0, sizeof(*result));

    if (!tr_raft_log_matches(log, previous_index, previous_term)) {
        result->reject_hint = tr_raft_log_reject_hint(log, previous_index);
        return SALTS_OK;
    }
    result->matched = true;

    for (index = 0U; index < incoming_count; ++index) {
        tr_raft_log_index_t expected_index = previous_index + index + 1U;
        const tr_raft_log_entry_t *local;
        tr_raft_conf_t configuration;

        if (incoming[index].index != expected_index ||
            incoming[index].term == 0U ||
            incoming[index].data_length > TR_RAFT_LOG_MAX_ENTRY_BYTES) {
            return SALTS_EINVAL;
        }
        if (incoming[index].command_id == 0U &&
            tr_raft_conf_entry_decode(&incoming[index], &configuration) !=
                SALTS_OK) {
            return SALTS_EPROTO;
        }
        local = tr_raft_log_get(log, expected_index);
        if (local == NULL) {
            first_change = index;
            break;
        }
        if (local->term != incoming[index].term) {
            first_change = index;
            truncates_existing = true;
            break;
        }
        if (!tr_raft_log_entry_equal(local, &incoming[index])) {
            return SALTS_EPROTO;
        }
    }

    if (first_change == incoming_count) {
        return SALTS_OK;
    }

    retained_count = (size_t) (previous_index - log->base_index) + first_change;
    if (truncates_existing && incoming[first_change].index <= protected_index) {
        return SALTS_EPROTO;
    }
    if (retained_count > log->max_entries ||
        incoming_count - first_change > log->max_entries - retained_count) {
        return SALTS_ENOSPC;
    }
    final_count = retained_count + incoming_count - first_change;

    resize_result = tr_raft_stl_status_to_error(
        vec_resize(&log->entries, final_count));
    if (resize_result != SALTS_OK) {
        return resize_result;
    }
    for (index = first_change; index < incoming_count; ++index) {
        tr_raft_log_entry_t *destination = (tr_raft_log_entry_t *)vec_at(
            &log->entries,
            (size_t) (incoming[index].index - log->base_index - 1U));

        *destination = incoming[index];
    }

    result->changed = true;
    result->truncate_from = truncates_existing
                                ? incoming[first_change].index
                                : 0U;
    result->append_from = incoming[first_change].index;
    result->append_count = incoming_count - first_change;
    return SALTS_OK;
}
