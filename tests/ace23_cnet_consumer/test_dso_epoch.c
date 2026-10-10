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
#ifndef TURBORAFT_ACE23_EPOCH_DSO_B_PATH
#error "A second independent DSO image path is required"
#endif

enum { CALLBACK_DEADLINE_MS = 5000U, DSO_CALLBACK_THREADS = 4U };

typedef struct dso_callback_thread {
    tr_ace23_dso_callback callback; /* borrowed under the published Scope */
    bool result; /* only read after joining thread */
} dso_callback_thread;

static void run_dso_callback(void *context)
{
    dso_callback_thread *thread = context;
    thread->result = tr_ace23_dso_callback_invoke(&thread->callback) == 1;
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
    salts_component_plugin_generation *generation,
    uint64_t requested,
    cmeta_plugin_registry *registry,
    cmeta_plugin_ref plugin,
    const salts_component_plugin_generation_storage *storage)
{
    const salts_component_plugin_source source = {
        .plugin = plugin,
        .export_id = "turboraft.ace23.dso-component"
    };
    salts_component_plugin_generation *previous = NULL;
    salts_component_plugin_status result;

    result = salts_component_plugin_generation_build(
        generation, requested, registry, storage,
        NULL, 0U, &source, 1U, NULL, 0U);
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
            cmeta_plugin_lease temporary = {0};
            const cmeta_plugin_manifest *manifest = NULL;
            const tr_ace23_dso_callback_state *module = NULL;
            salts_component_service service = {0};
            tr_ace23_dso_callback callback =
                tr_ace23_dso_callback_bind(NULL, NULL);
            cmeta_plugin_lifecycle_info info = {0};
            salts_component_deployment deployments[1] = {{0}};
            salts_component_instance instances[1] = {{0}};
            salts_component_dependency dependencies[1] = {{0}};
            size_t activation_order[1] = {0};
            salts_component_plugin_module modules[1] = {{0}};
            const salts_component_plugin_generation_storage storage = {
                .deployments = deployments, .deployment_capacity = 1U,
                .instances = instances, .instance_capacity = 1U,
                .dependencies = dependencies, .dependency_capacity = 1U,
                .activation_order = activation_order, .activation_capacity = 1U,
                .modules = modules, .module_capacity = 1U
            };
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

            /* Real POSIX dlopen happens inside Salts::Plugin, the single
             * load/unload authority. A new ComponentProvider is resolved
             * from the reloaded DSO on every generation N -> N+1. */
            check_equal(cmeta_plugin_registry_load(
                &registry, TURBORAFT_ACE23_EPOCH_DSO_PATH, &plugin),
                CMETA_PLUGIN_OK);
            if (round != 0U)
                check_true(plugin.generation != prior_plugin_generation);
            prior_plugin_generation = plugin.generation;
            check_equal(cmeta_plugin_registry_start(
                &registry, plugin), CMETA_PLUGIN_OK);
            check_equal(publish_host_generation(
                &runtime, &published[round], host_generation,
                &registry, plugin, &storage), SALTS_COMPONENT_PLUGIN_OK);
            check_equal(published[round].module_count, (size_t)1U);
            check_equal(published[round].deployment_count, (size_t)1U);
            check_equal(salts_component_plugin_scope_acquire(
                &runtime, &scope), SALTS_COMPONENT_PLUGIN_OK);
            check_equal(salts_component_plugin_scope_generation_id(&scope),
                        host_generation);
            check_equal(salts_component_plugin_scope_find_service(
                &scope, tr_ace23_dso_callback_interface(),
                &service), SALTS_COMPONENT_PLUGIN_OK);
            check_equal(tr_ace23_dso_callback_borrow_from_object(
                service.object, service.interfaces, &callback), CMETA_OK);
            check_true(tr_ace23_dso_callback_valid(&callback));

            /* The sole long-lived module lease belongs to ComponentPlugin
             * generation.module[0]. Any temporary Plugin query borrow is
             * released BEFORE DSO callbacks run. */
            check_equal(cmeta_plugin_registry_acquire(
                &registry, plugin, &temporary, &manifest), CMETA_PLUGIN_OK);
            check_not_null(manifest);
            check_not_null(manifest->self);
            check_equal(manifest->version.patch, (uint32_t)1U);
            module = manifest->self;
            check_equal(cmeta_plugin_registry_release(
                &registry, &temporary), CMETA_PLUGIN_OK);
            check_equal(cmeta_plugin_registry_get_lifecycle(
                &registry, plugin, &info), CMETA_PLUGIN_OK);
            check_equal(info.active_leases, (size_t)1U);
            check_true(cmeta_plugin_lease_valid(modules[0].lease));
            /* Four independently scheduled OS threads call the Component
             * Interface vtable into this Plugin DSO. All four borrows are
             * pinned by ONE ComponentPlugin generation-owned module lease.
             * The vtable values are borrowed, not independent scopes. */
            for (worker_index = 0U; worker_index < DSO_CALLBACK_THREADS;
                 ++worker_index) {
                worker_state[worker_index].callback = callback;
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
            check_equal(cmeta_plugin_registry_get_lifecycle(
                &registry, plugin, &info), CMETA_PLUGIN_OK);
            check_equal(info.active_leases, (size_t)0U);
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
    it("keeps provider-backed N and N+1 scopes independently pinned during publication")
    {
        const cmeta_plugin_registry_config registry_config = {.capacity = 1U};
        cmeta_plugin_registry registry = {0};
        cmeta_plugin_ref plugin = {0};
        salts_component_plugin_runtime host = SALTS_COMPONENT_PLUGIN_RUNTIME_INIT;
        salts_component_plugin_generation generations[2] = {
            SALTS_COMPONENT_PLUGIN_GENERATION_INIT,
            SALTS_COMPONENT_PLUGIN_GENERATION_INIT
        };
        salts_component_plugin_scope scopes[2] = {
            SALTS_COMPONENT_PLUGIN_SCOPE_INIT,
            SALTS_COMPONENT_PLUGIN_SCOPE_INIT
        };
        salts_component_deployment deployments[2][1] = {{{0}}};
        salts_component_instance instances[2][1] = {{{0}}};
        salts_component_dependency dependencies[2][1] = {{{0}}};
        size_t activation_order[2][1] = {{0}};
        salts_component_plugin_module modules[2][1] = {{{0}}};
        salts_component_plugin_generation_storage stores[2] = {{0}};
        salts_component_plugin_generation *previous = NULL;
        salts_component_service services[2] = {{0}};
        tr_ace23_dso_callback callbacks[2] = {
            tr_ace23_dso_callback_bind(NULL, NULL),
            tr_ace23_dso_callback_bind(NULL, NULL)
        };
        cmeta_plugin_lifecycle_info info = {0};
        bool quiescent = false;
        unsigned g;

        check_equal(cmeta_plugin_registry_init(
            &registry, &registry_config), CMETA_PLUGIN_OK);
        check_equal(cmeta_plugin_registry_load(
            &registry, TURBORAFT_ACE23_EPOCH_DSO_PATH, &plugin),
            CMETA_PLUGIN_OK);
        check_equal(cmeta_plugin_registry_start(
            &registry, plugin), CMETA_PLUGIN_OK);
        check_equal(salts_component_plugin_runtime_init(
            &host), SALTS_COMPONENT_PLUGIN_OK);

        for (g = 0U; g < 2U; ++g) {
            stores[g] = (salts_component_plugin_generation_storage){
                .deployments = deployments[g], .deployment_capacity = 1U,
                .instances = instances[g], .instance_capacity = 1U,
                .dependencies = dependencies[g], .dependency_capacity = 1U,
                .activation_order = activation_order[g],
                .activation_capacity = 1U,
                .modules = modules[g], .module_capacity = 1U
            };
            if (g == 0U) {
                check_equal(publish_host_generation(
                    &host, &generations[g], UINT64_C(90011001) + g,
                    &registry, plugin, &stores[g]),
                    SALTS_COMPONENT_PLUGIN_OK);
            } else {
                const salts_component_plugin_source source = {
                    .plugin = plugin,
                    .export_id = "turboraft.ace23.dso-component"
                };
                check_equal(salts_component_plugin_generation_build(
                    &generations[g], UINT64_C(90011001) + g, &registry,
                    &stores[g], NULL, 0U, &source, 1U, NULL, 0U),
                    SALTS_COMPONENT_PLUGIN_OK);
                check_equal(salts_component_plugin_runtime_publish(
                    &host, &generations[g], &previous),
                    SALTS_COMPONENT_PLUGIN_OK);
                check_true(previous == &generations[0]);
                check_equal(generations[0].state,
                            SALTS_COMPONENT_PLUGIN_GENERATION_DRAINING);
            }
            check_equal(salts_component_plugin_scope_acquire(
                &host, &scopes[g]), SALTS_COMPONENT_PLUGIN_OK);
            check_equal(salts_component_plugin_scope_generation_id(
                &scopes[g]), UINT64_C(90011001) + g);
            check_equal(salts_component_plugin_scope_find_service(
                &scopes[g], tr_ace23_dso_callback_interface(),
                &services[g]), SALTS_COMPONENT_PLUGIN_OK);
            check_equal(tr_ace23_dso_callback_borrow_from_object(
                services[g].object, services[g].interfaces, &callbacks[g]),
                CMETA_OK);
            check_true(tr_ace23_dso_callback_valid(&callbacks[g]));
            check_true(cmeta_plugin_lease_valid(modules[g][0].lease));
        }

        /* The two published generation leases are owned independently,
         * even when both candidates happen to use one identical DSO
         * provider. A future integration can supply distinct DSOs without
         * introducing an alternate module lifetime authority. */
        check_equal(cmeta_plugin_registry_get_lifecycle(
            &registry, plugin, &info), CMETA_PLUGIN_OK);
        check_equal(info.active_leases, (size_t)2U);
        check_equal(salts_component_plugin_generation_drain(
            &host, &generations[0]), SALTS_COMPONENT_PLUGIN_BUSY);
        check_equal(salts_component_plugin_runtime_close(
            &host, &previous), SALTS_COMPONENT_PLUGIN_OK);
        check_true(previous == &generations[1]);
        check_equal(salts_component_plugin_generation_drain(
            &host, &generations[1]), SALTS_COMPONENT_PLUGIN_BUSY);

        check_equal(cmeta_plugin_registry_request_stop(
            &registry, plugin), CMETA_PLUGIN_OK);
        check_equal(cmeta_plugin_registry_unload(
            &registry, plugin), CMETA_PLUGIN_BUSY);

        check_equal(salts_component_plugin_scope_release(
            &scopes[0]), SALTS_COMPONENT_PLUGIN_OK);
        check_equal(salts_component_plugin_generation_drain(
            &host, &generations[0]), SALTS_COMPONENT_PLUGIN_OK);
        check_equal(cmeta_plugin_registry_get_lifecycle(
            &registry, plugin, &info), CMETA_PLUGIN_OK);
        check_equal(info.active_leases, (size_t)1U);
        check_equal(cmeta_plugin_registry_unload(
            &registry, plugin), CMETA_PLUGIN_BUSY);
        check_equal(salts_component_plugin_scope_release(
            &scopes[1]), SALTS_COMPONENT_PLUGIN_OK);
        check_equal(salts_component_plugin_generation_drain(
            &host, &generations[1]), SALTS_COMPONENT_PLUGIN_OK);
        check_equal(cmeta_plugin_registry_get_lifecycle(
            &registry, plugin, &info), CMETA_PLUGIN_OK);
        check_equal(info.active_leases, (size_t)0U);
        check_equal(cmeta_plugin_registry_poll_quiescent(
            &registry, plugin, &quiescent), CMETA_PLUGIN_OK);
        check_true(quiescent);
        check_equal(cmeta_plugin_registry_unload(
            &registry, plugin), CMETA_PLUGIN_OK);
        check_equal(salts_component_plugin_runtime_destroy(
            &host), SALTS_COMPONENT_PLUGIN_OK);
        check_equal(cmeta_plugin_registry_destroy(
            &registry), CMETA_PLUGIN_OK);
    }

    it("publishes distinct provider DSOs A and B while old callbacks and leases remain live")
    {
        const cmeta_plugin_registry_config cfg = {.capacity = 2U};
        cmeta_plugin_registry registry = {0};
        cmeta_plugin_ref modules_ref[2] = {{0}};
        salts_component_plugin_runtime host = SALTS_COMPONENT_PLUGIN_RUNTIME_INIT;
        salts_component_plugin_generation generations[2] = {
            SALTS_COMPONENT_PLUGIN_GENERATION_INIT,
            SALTS_COMPONENT_PLUGIN_GENERATION_INIT
        };
        salts_component_plugin_scope scopes[2] = {
            SALTS_COMPONENT_PLUGIN_SCOPE_INIT,
            SALTS_COMPONENT_PLUGIN_SCOPE_INIT
        };
        salts_component_deployment deployments[2][1] = {{{0}}};
        salts_component_instance instances[2][1] = {{{0}}};
        salts_component_dependency dependencies[2][1] = {{{0}}};
        size_t activation_order[2][1] = {{0}};
        salts_component_plugin_module owned_modules[2][1] = {{{0}}};
        salts_component_plugin_generation_storage storage[2] = {{0}};
        salts_component_service service[2] = {{0}};
        tr_ace23_dso_callback callback[2] = {
            tr_ace23_dso_callback_bind(NULL, NULL),
            tr_ace23_dso_callback_bind(NULL, NULL)
        };
        dso_callback_thread work[2] = {{0}};
        cmeta_thread_t threads[2] = {{0}};
        const tr_ace23_dso_callback_state *observed[2] = {NULL, NULL};
        cmeta_plugin_lifecycle_info lifecycle = {0};
        salts_component_plugin_generation *previous = NULL;
        bool quiescent = false;
        unsigned i;

        check_equal(cmeta_plugin_registry_init(&registry, &cfg),
                    CMETA_PLUGIN_OK);
        check_equal(salts_component_plugin_runtime_init(&host),
                    SALTS_COMPONENT_PLUGIN_OK);

        for (i = 0U; i < 2U; ++i) {
            cmeta_plugin_lease query = {0};
            const cmeta_plugin_manifest *manifest = NULL;
            const salts_component_plugin_source source = {
                .export_id = "turboraft.ace23.dso-component"
            };
            salts_component_plugin_source dynamic_source = source;
            const char *path = i == 0U
                ? TURBORAFT_ACE23_EPOCH_DSO_PATH
                : TURBORAFT_ACE23_EPOCH_DSO_B_PATH;

            storage[i] = (salts_component_plugin_generation_storage){
                .deployments = deployments[i], .deployment_capacity = 1U,
                .instances = instances[i], .instance_capacity = 1U,
                .dependencies = dependencies[i], .dependency_capacity = 1U,
                .activation_order = activation_order[i], .activation_capacity = 1U,
                .modules = owned_modules[i], .module_capacity = 1U
            };
            check_equal(cmeta_plugin_registry_load(
                &registry, path, &modules_ref[i]), CMETA_PLUGIN_OK);
            check_equal(cmeta_plugin_registry_start(
                &registry, modules_ref[i]), CMETA_PLUGIN_OK);
            dynamic_source.plugin = modules_ref[i];
            check_equal(salts_component_plugin_generation_build(
                &generations[i], UINT64_C(90012001) + (uint64_t)i,
                &registry, &storage[i], NULL, 0U, &dynamic_source, 1U,
                NULL, 0U), SALTS_COMPONENT_PLUGIN_OK);
            check_equal(salts_component_plugin_runtime_publish(
                &host, &generations[i], &previous), SALTS_COMPONENT_PLUGIN_OK);
            if (i == 0U) check_null(previous);
            else {
                check_true(previous == &generations[0]);
                check_equal(generations[0].state,
                            SALTS_COMPONENT_PLUGIN_GENERATION_DRAINING);
            }
            check_equal(salts_component_plugin_scope_acquire(
                &host, &scopes[i]), SALTS_COMPONENT_PLUGIN_OK);
            check_equal(salts_component_plugin_scope_generation_id(
                &scopes[i]), UINT64_C(90012001) + (uint64_t)i);
            check_equal(salts_component_plugin_scope_find_service(
                &scopes[i], tr_ace23_dso_callback_interface(),
                &service[i]), SALTS_COMPONENT_PLUGIN_OK);
            check_equal(tr_ace23_dso_callback_borrow_from_object(
                service[i].object, service[i].interfaces, &callback[i]), CMETA_OK);
            check_true(tr_ace23_dso_callback_valid(&callback[i]));

            /* Observe version and independently allocated module atomics
             * using a temporary Plugin lease, released before callbacks.
             * Both live Plugin leases now belong to ComponentPlugin. */
            check_equal(cmeta_plugin_registry_acquire(
                &registry, modules_ref[i], &query, &manifest), CMETA_PLUGIN_OK);
            check_not_null(manifest);
            check_equal(manifest->version.minor, i + 1U);
            check_equal(manifest->version.patch, 1U);
            observed[i] = (const tr_ace23_dso_callback_state *)manifest->self;
            check_not_null(observed[i]);
            check_equal(cmeta_plugin_registry_release(
                &registry, &query), CMETA_PLUGIN_OK);
            check_equal(cmeta_plugin_registry_get_lifecycle(
                &registry, modules_ref[i], &lifecycle), CMETA_PLUGIN_OK);
            check_equal(lifecycle.active_leases, (size_t)1U);
            check_true(cmeta_plugin_lease_valid(owned_modules[i][0].lease));

            /* Old N callback deliberately stays inside provider A while
             * N+1/B publishes. New B can also execute concurrently. */
            work[i].callback = callback[i];
            check_equal(cmeta_thread_create(
                &threads[i], run_dso_callback, &work[i]), SALTS_OK);
            check_equal(await_callback_entry(observed[i], 1U), 0);
            check_equal(atomic_load_explicit(
                &observed[i]->callback_completed, memory_order_acquire), 0U);
        }

        check_true(modules_ref[0].slot != modules_ref[1].slot);
        check_true(observed[0] != observed[1]);
        check_equal(salts_component_plugin_generation_drain(
            &host, &generations[0]), SALTS_COMPONENT_PLUGIN_BUSY);
        check_equal(cmeta_plugin_registry_unload(
            &registry, modules_ref[0]), CMETA_PLUGIN_BUSY);
        check_equal(cmeta_plugin_registry_unload(
            &registry, modules_ref[1]), CMETA_PLUGIN_BUSY);

        /* Shutdown first revokes new admissions. Both published scopes
         * retain independent execution and Plugin unload authority. */
        check_equal(salts_component_plugin_runtime_close(
            &host, &previous), SALTS_COMPONENT_PLUGIN_OK);
        check_true(previous == &generations[1]);
        check_equal(salts_component_plugin_generation_drain(
            &host, &generations[1]), SALTS_COMPONENT_PLUGIN_BUSY);
        check_equal(cmeta_plugin_registry_request_stop(
            &registry, modules_ref[0]), CMETA_PLUGIN_OK);
        check_equal(cmeta_thread_join(&threads[0]), SALTS_OK);
        cmeta_thread_destroy(&threads[0]);
        check_true(work[0].result);
        check_equal(atomic_load_explicit(
            &observed[0]->callback_completed, memory_order_acquire), 1U);
        check_equal(salts_component_plugin_scope_release(
            &scopes[0]), SALTS_COMPONENT_PLUGIN_OK);
        check_equal(salts_component_plugin_generation_drain(
            &host, &generations[0]), SALTS_COMPONENT_PLUGIN_OK);
        check_equal(cmeta_plugin_registry_get_lifecycle(
            &registry, modules_ref[0], &lifecycle), CMETA_PLUGIN_OK);
        check_equal(lifecycle.active_leases, (size_t)0U);
        check_equal(cmeta_plugin_registry_poll_quiescent(
            &registry, modules_ref[0], &quiescent), CMETA_PLUGIN_OK);
        check_true(quiescent);
        check_equal(cmeta_plugin_registry_unload(
            &registry, modules_ref[0]), CMETA_PLUGIN_OK);

        /* The B callback, Component scope, and lease are still live AFTER
         * A was actually dlclosed. No stale A dispatch may affect B. */
        check_equal(atomic_load_explicit(
            &observed[1]->callback_completed, memory_order_acquire), 0U);
        check_equal(salts_component_plugin_scope_generation_id(
            &scopes[1]), UINT64_C(90012002));
        check_true(tr_ace23_dso_callback_valid(&callback[1]));
        check_equal(cmeta_plugin_registry_get_lifecycle(
            &registry, modules_ref[1], &lifecycle), CMETA_PLUGIN_OK);
        check_equal(lifecycle.active_leases, (size_t)1U);
        check_equal(cmeta_plugin_registry_unload(
            &registry, modules_ref[1]), CMETA_PLUGIN_BUSY);

        check_equal(cmeta_plugin_registry_request_stop(
            &registry, modules_ref[1]), CMETA_PLUGIN_OK);
        check_equal(cmeta_thread_join(&threads[1]), SALTS_OK);
        cmeta_thread_destroy(&threads[1]);
        check_true(work[1].result);
        check_equal(atomic_load_explicit(
            &observed[1]->callback_completed, memory_order_acquire), 1U);
        check_equal(salts_component_plugin_scope_release(
            &scopes[1]), SALTS_COMPONENT_PLUGIN_OK);
        check_equal(salts_component_plugin_generation_drain(
            &host, &generations[1]), SALTS_COMPONENT_PLUGIN_OK);
        check_equal(cmeta_plugin_registry_get_lifecycle(
            &registry, modules_ref[1], &lifecycle), CMETA_PLUGIN_OK);
        check_equal(lifecycle.active_leases, (size_t)0U);
        check_equal(cmeta_plugin_registry_poll_quiescent(
            &registry, modules_ref[1], &quiescent), CMETA_PLUGIN_OK);
        check_true(quiescent);
        check_equal(cmeta_plugin_registry_unload(
            &registry, modules_ref[1]), CMETA_PLUGIN_OK);
        check_equal(salts_component_plugin_runtime_destroy(
            &host), SALTS_COMPONENT_PLUGIN_OK);
        check_equal(cmeta_plugin_registry_destroy(
            &registry), CMETA_PLUGIN_OK);
    }

}
