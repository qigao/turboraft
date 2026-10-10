#include "test_dso_epoch_fixture.h"

#include <salts/plugin.h>

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

static cmeta_plugin_manifest tr_manifest = {
    .struct_size = CMETA_PLUGIN_MANIFEST_SIZE,
    .abi_version = CMETA_PLUGIN_ABI_VERSION,
    .plugin_id = "turboraft.ace23.host-epoch-fixture",
    .version = {1u, 0u, 0u},
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
