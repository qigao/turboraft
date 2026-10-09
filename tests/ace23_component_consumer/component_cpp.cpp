#include <turboraft/raft_component.h>

#include <type_traits>

static_assert(std::is_same<
    decltype(tr_raft_storage_source_vtable::snapshot),
    int (*)(void *, tr_raft_storage_t *)>::value,
    "Storage strategy must preserve the exact native CMeta vtable ABI");
static_assert(std::is_same<
    decltype(tr_raft_transport_source_vtable::snapshot),
    int (*)(void *, tr_raft_transport_t *)>::value,
    "Transport strategy must preserve the exact native CMeta vtable ABI");
static_assert(std::is_same<
    decltype(tr_raft_state_machine_source_vtable::snapshot),
    int (*)(void *, tr_raft_state_machine_t *)>::value,
    "Apply strategy must preserve the exact native CMeta vtable ABI");

int main()
{
    const tr_raft_storage_source unbound =
        tr_raft_storage_source_bind(nullptr, nullptr);
    return tr_raft_storage_source_valid(&unbound) ? 1 : 0;
}
