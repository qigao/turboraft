#include <turboraft/raft_flowmq_peer_service.h>
#include <turboraft/raft_flowmq_owner.h>
#include <flowmq.h>

#include <cstdint>
#include <type_traits>

static_assert(TR_RAFT_FLOWMQ_MAX_CERTIFICATES_PER_PEER == 4U,
              "The exact transport identity bound changed");
static_assert(std::is_standard_layout<tr_raft_flowmq_peer_config_t>::value,
              "Installed FlowMQ peer config lost C++17 ABI layout");

int main()
{
    auto *const create = &tr_raft_flowmq_peer_service_create;
    auto *const step = &tr_raft_flowmq_peer_service_step;
    auto *const owner_poll = &tr_raft_flowmq_owner_poll;
    return create == nullptr || step == nullptr || owner_poll == nullptr;
}
