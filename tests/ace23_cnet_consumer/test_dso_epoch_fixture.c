#include "test_dso_epoch_fixture.h"

#include <salts/plugin.h>
#include <salts/component_plugin_abi.h>
#include <cmeta/component.h>
#include <cmeta/data.h>

#include <threads.h>

/* This entire image is *actually unloaded and reloaded* by Salts::Plugin.
 * Its query-time serial intentionally resets after every successful dlclose.
 * The host-owned ComponentPlugin generation does NOT live in this image. */
static unsigned tr_local_query_serial;
static tr_ace23_dso_callback_state tr_callback_state;

static cmeta_plugin_status CMETA_PLUGIN_CALL tr_plugin_start(void *self)
{
    tr_ace23_dso_callback_state *state = self;
    atomic_store_explicit(&state->stop_requested, false, memory_order_release);
    atomic_store_explicit(&state->callback_entered, 0U, memory_order_release);
    atomic_store_explicit(&state->callback_completed, 0U, memory_order_release);
    return CMETA_PLUGIN_OK;
}

static cmeta_plugin_status CMETA_PLUGIN_CALL tr_plugin_request_stop(void *self)
{
    tr_ace23_dso_callback_state *state = self;
    atomic_store_explicit(&state->stop_requested, true, memory_order_release);
    return CMETA_PLUGIN_OK;
}

static bool CMETA_PLUGIN_CALL tr_plugin_is_quiescent(const void *self)
{
    tr_ace23_dso_callback_state *state = (tr_ace23_dso_callback_state *)(void *)self;

    /* Direct DSO callback invoked under the caller's real Plugin lease.
     * The host request_stop races this callback without racing dlclose:
     * unload must return BUSY until this callback returns and lease drops. */
    atomic_fetch_add_explicit(
        &state->callback_entered, 1U, memory_order_release);
    while (!atomic_load_explicit(
               &state->stop_requested, memory_order_acquire))
        thrd_yield();
    atomic_fetch_add_explicit(
        &state->callback_completed, 1U, memory_order_release);
    return true;
}

static void CMETA_PLUGIN_CALL tr_plugin_destroy(void *self)
{
    (void)self;
}

/* A genuine ComponentPlugin provider export from this very DSO. Its
 * Component instance borrows module-owned storage; the ComponentPlugin
 * generation's module lease, not a caller's ad-hoc Plugin lease, must keep
 * this code and storage alive through Component stop and callback drain. */
cmeta_component_empty(TurboRaftDsoEpochProvider);

static int tr_component_value = 23;

static cmeta_status SALTS_COMPONENT_CALL tr_component_create(
    void *context,
    const cmeta_data_desc *config_data,
    const void *config_value,
    const salts_component_dependency *dependencies,
    size_t dependency_count,
    cmeta_object_ref *out)
{
    (void)dependencies;
    if (context != &tr_component_value || config_data != NULL ||
        config_value != NULL || dependency_count != 0U || out == NULL)
        return CMETA_INVALID_ARGUMENT;
    return cmeta_object_borrow(
        out, context, &cmeta_data_int, NULL);
}

static const salts_component_provider_binding tr_component_binding = {
    .struct_size = sizeof(salts_component_provider_binding),
    .abi_version = SALTS_COMPONENT_PROVIDER_BINDING_ABI_VERSION,
    .component = cmeta_component_meta(TurboRaftDsoEpochProvider),
    .provider_context = &tr_component_value,
    .create = tr_component_create
};

static const salts_component_provider_binding *tr_provider_binding(
    void *context)
{
    return (const salts_component_provider_binding *)context;
}

CMETA_IMPLEMENTS(
    salts_component_provider, tr_dso_provider_impl, 0u,
    .get_binding = tr_provider_binding);

static salts_component_provider tr_provider = {
    (void *)&tr_component_binding,
    &tr_dso_provider_impl_vtable
};

static const cmeta_plugin_export tr_provider_exports[] = {{
    .struct_size = CMETA_PLUGIN_EXPORT_SIZE,
    .kind = CMETA_PLUGIN_EXPORT_INTERFACE,
    .contract_version = SALTS_COMPONENT_PROVIDER_CONTRACT_VERSION,
    .export_id = "turboraft.ace23.dso-component",
    .contract_id = SALTS_COMPONENT_PROVIDER_CONTRACT_ID,
    .value = { .interface = {
        &salts_component_provider_interface_meta, &tr_provider
    } }
}};

static cmeta_plugin_manifest tr_manifest = {
    .struct_size = CMETA_PLUGIN_MANIFEST_SIZE,
    .abi_version = CMETA_PLUGIN_ABI_VERSION,
    .plugin_id = "turboraft.ace23.host-epoch-fixture",
    .version = {1u, 0u, 0u},
    .exports = tr_provider_exports,
    .export_count = sizeof(tr_provider_exports) / sizeof(tr_provider_exports[0]),
    .self = &tr_callback_state,
    .start = tr_plugin_start,
    .request_stop = tr_plugin_request_stop,
    .is_quiescent = tr_plugin_is_quiescent,
    .destroy = tr_plugin_destroy
};

CMETA_PLUGIN_QUERY_EXPORT
const cmeta_plugin_manifest *CMETA_PLUGIN_CALL
cmeta_plugin_query(uint32_t host_abi)
{
    if (host_abi != CMETA_PLUGIN_ABI_VERSION) return NULL;
    /* Test-visible proof that this image's BSS was reset by the actual
     * dynamic loader, not a simulated N->N+1 counter increment. */
    tr_manifest.version.patch = ++tr_local_query_serial;
    return &tr_manifest;
}
