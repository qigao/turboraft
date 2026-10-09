#include <turboraft/raft_cnet_peer.h>
#include <turboraft/raft_cnet_channel.h>

#include <type_traits>

static_assert(std::is_same<
    decltype(&tr_raft_cnet_group_transport_bind),
    int (*)(tr_raft_cnet_group_binding_t *, tr_raft_transport_t *)>::value,
    "CNet group-to-Raft adapter must keep one exact native binding ABI");

static_assert(std::is_same<
    decltype(&tr_raft_cnet_channel_send),
    int (*)(tr_raft_cnet_channel_t *,
            const tr_raft_transport_payload_t *)>::value,
    "Authenticated CNet Raft channel must retain exact native send signature");
static_assert(std::is_same<
    decltype(&tr_raft_cnet_channel_attach),
    int (*)(tr_raft_cnet_channel_t *, cnet_connection)>::value,
    "Authenticated CNet Raft channel has one exact final-owner connection");

int main()
{
    tr_raft_transport_t transport = {};
    tr_raft_cnet_group_binding_t group = {};
    tr_raft_cnet_channel_status_t status = {};
    if (tr_raft_cnet_group_transport_bind(&group, &transport) != SALTS_EINVAL)
        return 1;
    if (tr_raft_cnet_channel_get_status(nullptr, &status) != SALTS_EINVAL)
        return 2;
    if (tr_raft_cnet_channel_stop(nullptr) != SALTS_EINVAL)
        return 3;
    return tr_raft_cnet_channel_destroy(nullptr) == SALTS_OK ? 0 : 4;
}
