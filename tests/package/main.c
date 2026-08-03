#include <turboraft/raft_control_plane.h>
#include <turboraft/raft_core.h>
#include <turboraft/raft_coronet_peer_service.h>
#include <turboraft/raft_service.h>
#include <turboraft/raft_service_owner.h>
#include <turboraft/raft_snapshot_receiver.h>
#include <turboraft/raft_snapshot_sender.h>
#include <turboraft/raft_snapshot_manager.h>
#include <turboraft/raft_sqlite_storage.h>

int main(void)
{
    return TR_RAFT_MAX_MEMBERS == 0U ? 1 : 0;
}
