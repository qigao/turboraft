#include <turboraft/raft_runtime.h>

#include <salts_error.h>

#include <stddef.h>

_Static_assert(offsetof(tr_raft_state_machine_t, context) == 0U,
               "legacy state-machine context ABI changed");
_Static_assert(offsetof(tr_raft_state_machine_t, apply_batch) == sizeof(void *),
               "legacy state-machine callback ABI changed");
_Static_assert(sizeof(tr_raft_state_machine_t) == 2U * sizeof(void *),
               "legacy state-machine ABI size changed");

static int legacy_apply_batch(void *context,
                              const tr_raft_entry_t *entries,
                              size_t entry_count)
{
    (void) context;
    if (entry_count != 0U && entries == NULL) {
        return SALTS_EINVAL;
    }
    return SALTS_OK;
}

int main(void)
{
    tr_raft_state_machine_t state_machine = {0};
    int (*ack_ready)(tr_raft_core_t *, size_t) = tr_raft_core_ack_ready;
    int (*ack_applied)(tr_raft_core_t *, tr_raft_index_t) =
        tr_raft_core_ack_applied;
    int (*legacy_advance)(tr_raft_core_t *) = tr_raft_core_advance;

    state_machine.apply_batch = legacy_apply_batch;
    return state_machine.apply_batch == NULL ||
                   ack_ready == NULL ||
                   ack_applied == NULL ||
                   legacy_advance == NULL
               ? 1
               : 0;
}
