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
    const cmeta_component_desc *descriptor =
        tr_raft_component_descriptor(TR_RAFT_COMPONENT_STORAGE);
    if (descriptor == nullptr || !cmeta_component_desc_valid(descriptor))
        return 2;
    if (tr_raft_component_descriptor(static_cast<tr_raft_component_kind_t>(0)) != nullptr)
        return 3;
    return tr_raft_storage_source_valid(&unbound) ? 1 : 0;
}
