#include <turboraft/raft_control_plane.h>
#include <turboraft/raft_apply_runtime.h>
#include <turboraft/raft_core.h>
#include <turboraft/raft_cflow_state_machine.h>
#include <turboraft/raft_transport.h>
#include <turboraft/raft_service.h>
#include <turboraft/raft_snapshot_receiver.h>
#include <turboraft/raft_snapshot_sender.h>
#include <turboraft/raft_snapshot_manager.h>
#include <turboraft/raft_wal_storage.h>
#include <turboraft/raft_flowmq_peer_service.h>

#include <salts_error.h>

int main(void)
{
    if (TR_RAFT_MAX_MEMBERS == 0U) {
        return 1;
    }
    return tr_raft_apply_runtime_reconcile(
               NULL, NULL, TR_RAFT_APPLY_OUTCOME_INVALID, NULL) ==
                   SALTS_EINVAL
               ? 0
               : 1;
}
