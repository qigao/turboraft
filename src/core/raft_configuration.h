#ifndef TURBORAFT_RAFT_CONFIGURATION_H
#define TURBORAFT_RAFT_CONFIGURATION_H

#include <turboraft/raft_core.h>

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

bool tr_raft_conf_entry_is_configuration(const tr_raft_entry_t *entry);

int tr_raft_conf_entry_encode(const tr_raft_conf_t *configuration,
                              tr_raft_index_t index,
                              tr_raft_term_t term,
                              tr_raft_entry_t *entry);

int tr_raft_conf_entry_decode(const tr_raft_entry_t *entry,
                              tr_raft_conf_t *configuration);

#endif
