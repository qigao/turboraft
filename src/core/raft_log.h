#ifndef TURBORAFT_RAFT_LOG_H
#define TURBORAFT_RAFT_LOG_H

#include <turboraft/raft_core.h>

#include "raft_configuration.h"

#include <turbostl/vec.h>

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#define TR_RAFT_LOG_MAX_ENTRY_BYTES TR_RAFT_MAX_ENTRY_BYTES

typedef tr_raft_index_t tr_raft_log_index_t;
typedef tr_raft_term_t tr_raft_log_term_t;
typedef tr_raft_entry_t tr_raft_log_entry_t;

typedef struct tr_raft_log {
    vec_t entries;
    size_t max_entries;
    tr_raft_log_index_t base_index;
    tr_raft_log_term_t base_term;
} tr_raft_log_t;

typedef struct tr_raft_log_reconcile_result {
    bool matched;
    bool changed;
    tr_raft_log_index_t reject_hint;
    tr_raft_log_index_t truncate_from;
    tr_raft_log_index_t append_from;
    size_t append_count;
} tr_raft_log_reconcile_result_t;

int tr_raft_log_init(tr_raft_log_t *log,
                     size_t max_entries,
                     tr_raft_log_index_t base_index,
                     tr_raft_log_term_t base_term);

void tr_raft_log_destroy(tr_raft_log_t *log);

size_t tr_raft_log_count(const tr_raft_log_t *log);

tr_raft_log_index_t tr_raft_log_last_index(const tr_raft_log_t *log);

tr_raft_log_term_t tr_raft_log_last_term(const tr_raft_log_t *log);

const tr_raft_log_entry_t *tr_raft_log_get(const tr_raft_log_t *log,
                                           tr_raft_log_index_t index);

bool tr_raft_log_matches(const tr_raft_log_t *log,
                         tr_raft_log_index_t index,
                         tr_raft_log_term_t term);

int tr_raft_log_compact(tr_raft_log_t *log,
                        tr_raft_log_index_t index,
                        tr_raft_log_term_t term);

int tr_raft_log_append_local(tr_raft_log_t *log,
                             tr_raft_log_term_t term,
                             uint64_t command_id,
                             const void *data,
                             size_t data_length,
                             const tr_raft_log_entry_t **out_entry);

int tr_raft_log_append_configuration(
    tr_raft_log_t *log,
    tr_raft_log_term_t term,
    const tr_raft_conf_t *configuration,
    const tr_raft_log_entry_t **out_entry);

int tr_raft_log_reconcile(tr_raft_log_t *log,
                          tr_raft_log_index_t previous_index,
                          tr_raft_log_term_t previous_term,
                          const tr_raft_log_entry_t *incoming,
                          size_t incoming_count,
                          tr_raft_log_index_t protected_index,
                          tr_raft_log_reconcile_result_t *result);

#endif
