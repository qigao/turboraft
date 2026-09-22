#include <turboraft/raft_apply_runtime.h>
#include <turboraft/raft_cflow_state_machine.h>

#include <stddef.h>

_Static_assert(TR_RAFT_ENTRY_STATE_MACHINE_ABI_V1 == 1U,
               "unexpected async state-machine ABI");
_Static_assert(TR_RAFT_APPLY_RUNTIME_CONFIG_ABI_V1 == 1U,
               "unexpected ApplyRuntime ABI");
_Static_assert(TR_RAFT_CFLOW_STATE_MACHINE_CONFIG_ABI_V1 == 1U,
               "unexpected CFlow adapter ABI");

int main(void)
{
    int (*apply_create)(const tr_raft_apply_runtime_config_v1_t *,
                        tr_raft_apply_runtime_t **) =
        tr_raft_apply_runtime_create;
    int (*cflow_create)(const tr_raft_cflow_state_machine_config_v1_t *,
                        tr_raft_cflow_state_machine_t **) =
        tr_raft_cflow_state_machine_create;
    int (*cflow_destroy)(tr_raft_cflow_state_machine_t *) =
        tr_raft_cflow_state_machine_destroy;

    return apply_create == NULL ||
                   cflow_create == NULL ||
                   cflow_destroy == NULL
               ? 1
               : 0;
}
