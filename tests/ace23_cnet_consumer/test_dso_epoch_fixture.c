#include "test_dso_epoch_fixture.h"

#include <salts/plugin.h>
#include <salts/component_plugin_abi.h>
#include <cmeta/component.h>
#include <cmeta/data.h>

#include <threads.h>

/* This source is built into two *different* installed consumer DSOs.
 * The variant changes the native image's Component data and Plugin ID;
 * each image owns separate callback atomics and query-local BSS. */
#ifndef TURBORAFT_ACE23_DSO_VARIANT
#define TURBORAFT_ACE23_DSO_VARIANT 1
#endif
#if TURBORAFT_ACE23_DSO_VARIANT == 1
#define TR_DSO_PLUGIN_ID "turboraft.ace23.host-epoch-fixture.a"
#elif TURBORAFT_ACE23_DSO_VARIANT == 2
#define TR_DSO_PLUGIN_ID "turboraft.ace23.host-epoch-fixture.b"
#else
#error "Only two explicit ABI5 provider images A and B are admitted"
#endif

/* This entire image is *actually unloaded and reloaded* by Salts::Plugin.
 * Its query-time serial intentionally resets after every successful dlclose.
 * The host-owned ComponentPlugin generation does NOT live in this image. */
static unsigned tr_local_query_serial;
static tr_ace23_dso_callback_state tr_callback_state;

static cmeta_plugin_status CMETA_PLUGIN_CALL tr_plugin_start(void *self)
{
    tr_ace23_dso_callback_state *state = self;
    atomic_store_explicit(&state->stop_requested, false, memory_order_release);
    atomic_store_explicit(&state->release_callback, false, memory_order_release);
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
               &state->stop_requested, memory_order_acquire) &&
           !atomic_load_explicit(
               &state->release_callback, memory_order_acquire))
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
cmeta_component(TurboRaftDsoEpochProvider,
    cmeta_provides(tr_ace23_dso_callback));

static int tr_component_value = 23 + TURBORAFT_ACE23_DSO_VARIANT;

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

static int tr_component_invoke(void *self)
{
    if (self != &tr_component_value) return 0;
    return tr_plugin_is_quiescent(&tr_callback_state) ? 1 : 0;
}

CMETA_IMPLEMENTS(
    tr_ace23_dso_callback, tr_component_callback_impl, 0u,
    .invoke = tr_component_invoke);

static cmeta_status tr_component_project(
    void *context, const cmeta_object_ref *object,
    const cmeta_interface_desc *expected,
    cmeta_interface_projection *out)
{
    (void)context;
    if (object == NULL || out == NULL) return CMETA_INVALID_ARGUMENT;
    if (!cmeta_interface_desc_equal(
            expected, tr_ace23_dso_callback_interface()))
        return CMETA_TRAIT_MISSING;
    out->size = sizeof(*out);
    out->self = object->object;
    out->interface = tr_ace23_dso_callback_interface();
    out->dispatch = &tr_component_callback_impl_vtable;
    return CMETA_OK;
}

static const cmeta_object_interface_provider tr_component_interfaces = {
    sizeof(cmeta_object_interface_provider),
    NULL,
    tr_component_project
};

static const salts_component_provider_binding tr_component_binding = {
    .struct_size = sizeof(salts_component_provider_binding),
    .abi_version = SALTS_COMPONENT_PROVIDER_BINDING_ABI_VERSION,
    .component = cmeta_component_meta(TurboRaftDsoEpochProvider),
    .provider_context = &tr_component_value,
    .interfaces = &tr_component_interfaces,
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
    .plugin_id = TR_DSO_PLUGIN_ID,
    .version = {1u, (uint32_t)TURBORAFT_ACE23_DSO_VARIANT, 0u},
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
