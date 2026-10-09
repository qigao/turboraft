#include <turboraft/raft_cnet_peer.h>
#include <turboraft/raft_cnet_channel.h>
#include <turboraft/raft_cnet_managed_peer.h>
#include <turboraft/raft_cnet_peer_directory.h>

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

static_assert(std::is_same<
    decltype(&tr_raft_cnet_channel_group_transport_bind),
    int (*)(tr_raft_cnet_channel_group_binding_t *,
            tr_raft_transport_t *)>::value,
    "Raft Service -> verified CNet Channel uses one typed borrowed transport");

static_assert(std::is_same<
    decltype(&tr_raft_cnet_managed_peer_advance),
    int (*)(tr_raft_cnet_managed_peer_t *, uint64_t, uint64_t *)>::value,
    "Managed Raft CNet dial must preserve owner-local explicit progress ABI");
static_assert(std::is_same<
    decltype(&tr_raft_cnet_managed_peer_send),
    int (*)(tr_raft_cnet_managed_peer_t *,
            const tr_raft_transport_payload_t *)>::value,
    "Raft managed peer has a typed, capacity-bounded transport SPI");

static_assert(std::is_same<
    decltype(&tr_raft_cnet_managed_group_transport_bind),
    int (*)(tr_raft_cnet_managed_group_binding_t *,
            tr_raft_transport_t *)>::value,
    "Stable owner-bound Raft transport must survive managed dial generations");

static_assert(std::is_same<
    decltype(&tr_raft_cnet_peer_directory_transport_bind),
    int (*)(tr_raft_cnet_directory_group_binding_t *,
            tr_raft_transport_t *)>::value,
    "Node/Group directory must preserve the canonical Raft Transport signature");
static_assert(std::is_same<
    decltype(&tr_raft_cnet_peer_directory_send),
    int (*)(tr_raft_cnet_peer_directory_t *,
            const tr_raft_transport_payload_t *)>::value,
    "Peer directory keeps an exact typed C11/C++17 payload admission boundary");

int main()
{
    tr_raft_transport_t transport = {};
    tr_raft_cnet_group_binding_t group = {};
    tr_raft_cnet_channel_status_t status = {};
    if (tr_raft_cnet_group_transport_bind(&group, &transport) != SALTS_EINVAL)
        return 1;
    tr_raft_cnet_channel_group_binding_t unbound = {};
    if (tr_raft_cnet_channel_group_transport_bind(&unbound, &transport) !=
        SALTS_EINVAL)
        return 5;
    if (tr_raft_cnet_channel_get_status(nullptr, &status) != SALTS_EINVAL)
        return 2;
    if (tr_raft_cnet_channel_stop(nullptr) != SALTS_EINVAL)
        return 3;
    return tr_raft_cnet_channel_destroy(nullptr) == SALTS_OK ? 0 : 4;
}
