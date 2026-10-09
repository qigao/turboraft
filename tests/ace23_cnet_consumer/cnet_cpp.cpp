#include <turboraft/raft_cnet_peer.h>

#include <type_traits>

static_assert(std::is_same<
    decltype(&tr_raft_cnet_group_transport_bind),
    int (*)(tr_raft_cnet_group_binding_t *, tr_raft_transport_t *)>::value,
    "CNet group-to-Raft adapter must keep one exact native binding ABI");

int main()
{
    tr_raft_transport_t transport = {};
    tr_raft_cnet_group_binding_t group = {};
    return tr_raft_cnet_group_transport_bind(&group, &transport) == SALTS_EINVAL
               ? 0 : 1;
}
