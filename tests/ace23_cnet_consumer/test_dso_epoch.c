#include "test_dso_epoch_fixture.h"

#include <salts/plugin.h>
#include <salts/component_plugin.h>
#include <salts/clock.h>
#include <salts/thread.h>
#include <turboraft/raft_transport.h>
#include <tinytest.h>

#include <stdint.h>
#include <string.h>

#ifndef TURBORAFT_ACE23_EPOCH_DSO_PATH
#error "DSO fixture must be selected by the installed CMake consumer"
#endif

enum { CALLBACK_DEADLINE_MS = 5000U, DSO_CALLBACK_THREADS = 4U };

typedef struct dso_callback_thread {
    const cmeta_plugin_manifest *manifest; /* borrowed under actual lease */
    bool result; /* only read after joining thread */
} dso_callback_thread;

static void run_dso_callback(void *context)
{
    dso_callback_thread *thread = context;
    thread->result = thread->manifest->is_quiescent(
        thread->manifest->self);
}

static int await_callback_entry(
    const tr_ace23_dso_callback_state *state, unsigned expected)
{
    const uint64_t deadline = cmeta_monotonic_ms() + CALLBACK_DEADLINE_MS;
    while (atomic_load_explicit(
               &state->callback_entered, memory_order_acquire) < expected) {
        if (cmeta_monotonic_ms() >= deadline)
            return -1;
        cmeta_thread_yield();
    }
    return 0;
}

static salts_component_plugin_status publish_host_generation(
    salts_component_plugin_runtime *runtime,
    salts_component_plugin_generation *generation, uint64_t requested)
{
    const salts_component_plugin_generation_storage empty_storage = {0};
    salts_component_plugin_generation *previous = NULL;
    salts_component_plugin_status result;

    result = salts_component_plugin_generation_build(
        generation, requested, NULL, &empty_storage,
        NULL, 0U, NULL, 0U, NULL, 0U);
    if (result != SALTS_COMPONENT_PLUGIN_OK) return result;
    return salts_component_plugin_runtime_publish(
        runtime, generation, &previous);
}

spec("ACE 2.3 real Plugin DSO callback quiescence and stable host epoch")
{
    it("actually unloads and reloads a Plugin DSO without recycling the host generation")
    {
        const cmeta_plugin_registry_config registry_config = {.capacity = 1U};
        cmeta_plugin_registry registry = {0};
        salts_component_plugin_runtime runtime = SALTS_COMPONENT_PLUGIN_RUNTIME_INIT;
        salts_component_plugin_generation published[2] = {
            SALTS_COMPONENT_PLUGIN_GENERATION_INIT,
            SALTS_COMPONENT_PLUGIN_GENERATION_INIT
        };
        tr_raft_transport_reply_origin_t before = {0}, after = {0};
        uint32_t prior_plugin_generation = 0U;
        unsigned round;

        check_equal(cmeta_plugin_registry_init(&registry, &registry_config),
                    CMETA_PLUGIN_OK);
        check_equal(salts_component_plugin_runtime_init(&runtime),
                    SALTS_COMPONENT_PLUGIN_OK);

        for (round = 0U; round < 2U; ++round) {
            const uint64_t host_generation = UINT64_C(90010001) + round;
            salts_component_plugin_scope scope = SALTS_COMPONENT_PLUGIN_SCOPE_INIT;
            salts_component_plugin_generation *retired = NULL;
            cmeta_plugin_ref plugin = {0};
            cmeta_plugin_lease lease = {0};
            const cmeta_plugin_manifest *manifest = NULL;
            const tr_ace23_dso_callback_state *module = NULL;
            dso_callback_thread worker_state[DSO_CALLBACK_THREADS] = {{0}};
            cmeta_thread_t workers[DSO_CALLBACK_THREADS] = {{0}};
            size_t worker_index;
            bool quiet = false;
            tr_raft_transport_reply_origin_t reply_origin = {0};

            if (round != 0U) {
                /* The host's ComponentPlugin runtime cannot republish an
                 * already issued generation even though the DSO is gone. */
                salts_component_plugin_generation duplicate =
                    SALTS_COMPONENT_PLUGIN_GENERATION_INIT;
                const salts_component_plugin_generation_storage empty = {0};
                check_equal(salts_component_plugin_generation_build(
                    &duplicate, host_generation - 1U, NULL,
                    &empty, NULL, 0U, NULL, 0U, NULL, 0U),
                    SALTS_COMPONENT_PLUGIN_OK);
                check_equal(salts_component_plugin_runtime_publish(
                    &runtime, &duplicate, &retired),
                    SALTS_COMPONENT_PLUGIN_INVALID_STATE);
                check_null(retired);
                check_equal(salts_component_plugin_generation_discard(
                    &duplicate), SALTS_COMPONENT_PLUGIN_OK);
            }

            check_equal(publish_host_generation(
                &runtime, &published[round], host_generation),
                SALTS_COMPONENT_PLUGIN_OK);
            check_equal(salts_component_plugin_scope_acquire(
                &runtime, &scope), SALTS_COMPONENT_PLUGIN_OK);
            check_equal(salts_component_plugin_scope_generation_id(&scope),
                        host_generation);

            /* Real POSIX dlopen happens inside the *existing* Plugin
             * loader. No second dlopen authority or native handle exists. */
            check_equal(cmeta_plugin_registry_load(
                &registry, TURBORAFT_ACE23_EPOCH_DSO_PATH, &plugin),
                CMETA_PLUGIN_OK);
            if (round != 0U)
                check_true(plugin.generation != prior_plugin_generation);
            prior_plugin_generation = plugin.generation;
            check_equal(cmeta_plugin_registry_start(
                &registry, plugin), CMETA_PLUGIN_OK);
            check_equal(cmeta_plugin_registry_acquire(
                &registry, plugin, &lease, &manifest), CMETA_PLUGIN_OK);
            check_not_null(manifest);
            check_not_null(manifest->is_quiescent);
            check_not_null(manifest->self);

            /* The loader-owned DSO static serial is 1 after *each real
             * load*, proving that it resets across dlclose/dlopen. */
            check_equal(manifest->version.patch, (uint32_t)1U);
            module = manifest->self;
            /* Four independently scheduled OS threads enter plugin-owned
             * callback code simultaneously under the ONE shared host
             * lease. A proper stop/unload barrier must join all four. */
            for (worker_index = 0U; worker_index < DSO_CALLBACK_THREADS;
                 ++worker_index) {
                worker_state[worker_index].manifest = manifest;
                check_equal(cmeta_thread_create(
                    &workers[worker_index], run_dso_callback,
                    &worker_state[worker_index]), SALTS_OK);
            }
            check_equal(await_callback_entry(module, DSO_CALLBACK_THREADS), 0);
            check_false(atomic_load_explicit(
                &module->stop_requested, memory_order_acquire));
            check_equal(atomic_load_explicit(
                &module->callback_completed, memory_order_acquire), 0U);

            /* Plugin lease pins executable DSO code while this callback
             * blocks, even when stop is requested concurrently. */
            check_equal(cmeta_plugin_registry_unload(
                &registry, plugin), CMETA_PLUGIN_BUSY);
            check_equal(cmeta_plugin_registry_request_stop(
                &registry, plugin), CMETA_PLUGIN_OK);
            check_equal(cmeta_plugin_registry_unload(
                &registry, plugin), CMETA_PLUGIN_BUSY);
            check_equal(cmeta_plugin_registry_poll_quiescent(
                &registry, plugin, &quiet), CMETA_PLUGIN_OK);
            check_false(quiet);
            for (worker_index = 0U; worker_index < DSO_CALLBACK_THREADS;
                 ++worker_index) {
                check_equal(cmeta_thread_join(
                    &workers[worker_index]), SALTS_OK);
                cmeta_thread_destroy(&workers[worker_index]);
                check_true(worker_state[worker_index].result);
            }
            check_equal(atomic_load_explicit(
                &module->callback_completed, memory_order_acquire),
                (unsigned)DSO_CALLBACK_THREADS);

            /* Stable host Scope closes before provider teardown. The
             * still-live scope prevents Component generation drain. */
            check_equal(salts_component_plugin_runtime_close(
                &runtime, &retired), SALTS_COMPONENT_PLUGIN_OK);
            check_true(retired == &published[round]);
            check_equal(salts_component_plugin_generation_drain(
                &runtime, retired), SALTS_COMPONENT_PLUGIN_BUSY);

            reply_origin.host_module_generation =
                salts_component_plugin_scope_generation_id(&scope);
            reply_origin.channel_instance = manifest->version.patch;
            reply_origin.authenticated_peer_node_id = 2U;
            reply_origin.group_id = 43U;
            reply_origin.connection_token = UINT64_C(4294967297);
            if (round == 0U) before = reply_origin;
            else after = reply_origin;

            check_equal(salts_component_plugin_scope_release(&scope),
                        SALTS_COMPONENT_PLUGIN_OK);
            check_equal(salts_component_plugin_generation_drain(
                &runtime, retired), SALTS_COMPONENT_PLUGIN_OK);
            check_equal(cmeta_plugin_registry_release(
                &registry, &lease), CMETA_PLUGIN_OK);
            check_equal(cmeta_plugin_registry_poll_quiescent(
                &registry, plugin, &quiet), CMETA_PLUGIN_OK);
            check_true(quiet);
            check_equal(cmeta_plugin_registry_unload(
                &registry, plugin), CMETA_PLUGIN_OK);
            /* Do not dereference 'module' or 'manifest' after unload. */
        }

        /* Both dynamic images restarted their local serial and could reuse
         * the CNet slot/generation; the stable published host epoch cannot
         * repeat. This models the value carried by a delayed Group result.
         * Real Channel refusal on mismatched epoch is separately covered
         * by managed_peer's authenticated TLS regression test. */
        check_equal(before.channel_instance, after.channel_instance);
        check_equal(before.connection_token, after.connection_token);
        check_equal(before.authenticated_peer_node_id,
                    after.authenticated_peer_node_id);
        check_equal(before.group_id, after.group_id);
        check_true(before.host_module_generation !=
                   after.host_module_generation);
        check_equal(salts_component_plugin_runtime_destroy(
            &runtime), SALTS_COMPONENT_PLUGIN_OK);
        check_equal(cmeta_plugin_registry_destroy(
            &registry), CMETA_PLUGIN_OK);
    }
}
