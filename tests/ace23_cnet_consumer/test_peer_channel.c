#include <turboraft/raft_cnet_channel.h>

#include "test_dso_epoch_fixture.h"
#include <salts/plugin.h>
#include <salts/component_plugin.h>
#include <salts/clock.h>
#include <salts/thread.h>

#include <cmeta_error.h>
#include <tinytest.h>

#include <stdio.h>
#include <string.h>

#ifndef TURBORAFT_ACE23_FIXTURE_DIR
#error "Set the directory containing the checked-in loopback-only TLS fixtures"
#endif
#ifndef TURBORAFT_ACE23_EPOCH_DSO_PATH
#error "Set the exact built Salts Plugin DSO fixture path"
#endif

#define CERT_NODE1 "eb571a92b33237897216c79501066b3e77391047eaeb6be084526c4769657549"
#define CERT_NODE2 "44e8fe3ce37ede1c2a1b5d36efa345cb662887d4250d17a000ebea2e834aed95"

enum peer_mode {
    PEER_VALID = 0,
    PEER_STREAMS,
    PEER_FOREIGN_CLUSTER,
    PEER_FORGED_NODE,
    PEER_UNAUTHORIZED_CERT,
    PEER_ABORT_SG,
    PEER_DSO_CALLBACK,
    PEER_DSO_AB_PUBLICATION
};

typedef struct peer_dso_harness {
    cmeta_plugin_registry registry;
    cmeta_plugin_ref plugin;
    /* The actual Plugin module lease is owned by generation.modules[0].
     * No separately held test-lease grants DSO callback authority. */
    const cmeta_plugin_manifest *manifest; /* borrowed under generation lease */
    tr_ace23_dso_callback callback; /* scope-bound typed Component Interface */
    salts_component_deployment deployments[1];
    salts_component_instance instances[1];
    salts_component_dependency dependencies[1];
    size_t activation_order[1];
    salts_component_plugin_module modules[1];
    const tr_ace23_dso_callback_state *module; /* same borrowed lifetime */
    tr_raft_cnet_channel_t *verified_channel; /* same CNet progress Owner */
    tr_raft_transport_reply_origin_t captured_origin; /* value-only */
    salts_component_plugin_runtime host;
    salts_component_plugin_generation generation;
    salts_component_plugin_scope scope;
    cmeta_thread_t controller;
    cmeta_plugin_status unload_while_callback;
    cmeta_plugin_status stop_result;
    cmeta_plugin_status unload_after_stop;
    salts_component_plugin_status close_while_callback;
    salts_component_plugin_status drain_while_callback;
    salts_component_plugin_generation *retired_by_controller;
    int controller_closed_publication;
    int controller_result;
    int registry_open, loaded, started;
    int host_open, published, scope_live, controller_live;
    unsigned network_callbacks;

    /* A and B are separate native Plugin images. B is published while the
     * certified CNet Owner's on_payload is still executing DSO A. */
    int ab_publication;
    cmeta_plugin_ref plugin_b;
    salts_component_plugin_generation generation_b;
    salts_component_plugin_scope scope_b;
    salts_component_deployment deployments_b[1];
    salts_component_instance instances_b[1];
    salts_component_dependency dependencies_b[1];
    size_t activation_order_b[1];
    salts_component_plugin_module modules_b[1];
    tr_ace23_dso_callback callback_b;
    tr_ace23_dso_callback_state *module_b; /* writable only while B Scope is held */
    tr_raft_cnet_channel_t *verified_channel_b;
    tr_raft_transport_reply_origin_t captured_origin_b;
    int loaded_b, started_b, published_b, scope_b_live;
    unsigned network_callbacks_b;
    cmeta_plugin_status unload_b_before_drain;
    salts_component_plugin_status drain_a_while_b_published;
    salts_component_plugin_status drain_b_while_callback;
    atomic_bool b_ready;
} peer_dso_harness;

static int peer_dso_publish_b(peer_dso_harness *dso)
{
    const salts_component_plugin_generation_storage storage = {
        .deployments = dso->deployments_b, .deployment_capacity = 1U,
        .instances = dso->instances_b, .instance_capacity = 1U,
        .dependencies = dso->dependencies_b, .dependency_capacity = 1U,
        .activation_order = dso->activation_order_b, .activation_capacity = 1U,
        .modules = dso->modules_b, .module_capacity = 1U
    };
    const salts_component_plugin_source source = {
        .plugin = {0},
        .export_id = "turboraft.ace23.dso-component"
    };
    salts_component_plugin_source selected = source;
    salts_component_plugin_generation *previous = NULL;
    salts_component_service service = {0};
    cmeta_plugin_lease query = {0};
    const cmeta_plugin_manifest *manifest = NULL;
    cmeta_plugin_lifecycle_info info = {0};

    if (cmeta_plugin_registry_load(
            &dso->registry, TURBORAFT_ACE23_EPOCH_DSO_B_PATH,
            &dso->plugin_b) != CMETA_PLUGIN_OK)
        return SALTS_EPROTO;
    dso->loaded_b = 1;
    if (cmeta_plugin_registry_start(
            &dso->registry, dso->plugin_b) != CMETA_PLUGIN_OK)
        return SALTS_EPROTO;
    dso->started_b = 1;
    selected.plugin = dso->plugin_b;
    if (salts_component_plugin_generation_build(
            &dso->generation_b, UINT64_C(90010002), &dso->registry,
            &storage, NULL, 0U, &selected, 1U, NULL, 0U) !=
        SALTS_COMPONENT_PLUGIN_OK)
        return SALTS_EPROTO;
    if (salts_component_plugin_runtime_publish(
            &dso->host, &dso->generation_b, &previous) !=
            SALTS_COMPONENT_PLUGIN_OK ||
        previous != &dso->generation ||
        dso->generation.state != SALTS_COMPONENT_PLUGIN_GENERATION_DRAINING)
        return SALTS_EPROTO;
    dso->published_b = 1;
    if (salts_component_plugin_scope_acquire(
            &dso->host, &dso->scope_b) != SALTS_COMPONENT_PLUGIN_OK)
        return SALTS_EPROTO;
    dso->scope_b_live = 1;
    if (salts_component_plugin_scope_generation_id(&dso->scope_b) !=
            UINT64_C(90010002) ||
        salts_component_plugin_scope_find_service(
            &dso->scope_b, tr_ace23_dso_callback_interface(), &service) !=
            SALTS_COMPONENT_PLUGIN_OK)
        return SALTS_EPROTO;
    if (tr_ace23_dso_callback_borrow_from_object(
            service.object, service.interfaces, &dso->callback_b) != CMETA_OK ||
        !tr_ace23_dso_callback_valid(&dso->callback_b))
        return SALTS_EPROTO;
    if (cmeta_plugin_registry_acquire(
            &dso->registry, dso->plugin_b, &query, &manifest) != CMETA_PLUGIN_OK)
        return SALTS_EPROTO;
    dso->module_b = (tr_ace23_dso_callback_state *)manifest->self;
    if (cmeta_plugin_registry_release(
            &dso->registry, &query) != CMETA_PLUGIN_OK)
        return SALTS_EPROTO;
    if (dso->module_b == NULL ||
        cmeta_plugin_registry_get_lifecycle(
            &dso->registry, dso->plugin_b, &info) != CMETA_PLUGIN_OK ||
        info.active_leases != 1U ||
        !cmeta_plugin_lease_valid(dso->modules_b[0].lease))
        return SALTS_EPROTO;
    /* CNet progress Owner now owns admission to the fresh TLS B Channel.
     * No duplicate worker/direct invocation of the Plugin DSO exists. */
    atomic_store_explicit(&dso->b_ready, true, memory_order_release);
    return SALTS_OK;
}

static void peer_dso_controller(void *context)
{
    peer_dso_harness *dso = (peer_dso_harness *)context;
    const uint64_t deadline = cmeta_monotonic_ms() + UINT64_C(5000);

    while (atomic_load_explicit(
               &dso->module->callback_entered, memory_order_acquire) == 0U) {
        if (cmeta_monotonic_ms() >= deadline) {
            dso->controller_result = SALTS_ETIMEDOUT;
            (void)cmeta_plugin_registry_request_stop(&dso->registry, dso->plugin);
            return;
        }
        cmeta_thread_yield();
    }
    /* Verified CNet on_payload is currently in provider A. Publish a
     * physically distinct Plugin image B from the stable host while A's
     * callback and original scope remain live. The Controller must never
     * touch a CNet Channel/Owner from this foreign thread. */
    if (dso->ab_publication) {
        const int published = peer_dso_publish_b(dso);
        if (published != SALTS_OK) {
            dso->controller_result = published;
            (void)cmeta_plugin_registry_request_stop(
                &dso->registry, dso->plugin);
            if (dso->started_b) (void)cmeta_plugin_registry_request_stop(
                &dso->registry, dso->plugin_b);
            return;
        }
        dso->drain_a_while_b_published =
            salts_component_plugin_generation_drain(
                &dso->host, &dso->generation);
        dso->unload_b_before_drain =
            cmeta_plugin_registry_unload(&dso->registry, dso->plugin_b);
    }
    /* The old Channel A is borrowed by its CNet callback. B's separately
     * published ComponentPlugin Scope pins B and admits a new CNet Channel
     * while the old A generation becomes DRAINING. */
    dso->unload_while_callback =
        cmeta_plugin_registry_unload(&dso->registry, dso->plugin);

    if (dso->ab_publication) {
        const uint64_t deadline_b =
            cmeta_monotonic_ms() + UINT64_C(5000);
        /* Only stop A, not B. A's on_payload can complete so the same
         * caller-driven CNet Owner can establish a second verified TLS
         * connection and invoke B's typed Component callback. */
        dso->stop_result =
            cmeta_plugin_registry_request_stop(&dso->registry, dso->plugin);
        dso->unload_after_stop =
            cmeta_plugin_registry_unload(&dso->registry, dso->plugin);
        while (atomic_load_explicit(
                   &dso->module_b->callback_entered,
                   memory_order_acquire) == 0U) {
            if (cmeta_monotonic_ms() >= deadline_b) {
                dso->controller_result = SALTS_ETIMEDOUT;
                break;
            }
            cmeta_thread_yield();
        }
        /* Revocation of the new publication occurs WHILE B's real TLS
         * on_payload holds its original ComponentPlugin Scope. */
        dso->close_while_callback =
            salts_component_plugin_runtime_close(
                &dso->host, &dso->retired_by_controller);
        if (dso->close_while_callback == SALTS_COMPONENT_PLUGIN_OK &&
            dso->retired_by_controller == &dso->generation_b) {
            dso->controller_closed_publication = 1;
            dso->drain_while_callback =
                salts_component_plugin_generation_drain(
                    &dso->host, &dso->generation_b);
        } else
            dso->drain_while_callback = SALTS_COMPONENT_PLUGIN_INVALID_STATE;
        /* Keep B in STARTED after the first real TLS on_payload. The
         * host releases only this callback, not B's module generation:
         * the same B TLS connection must accept a second message AFTER
         * A is actually dlclosed. */
        if (cmeta_plugin_registry_unload(
                &dso->registry, dso->plugin_b) != CMETA_PLUGIN_BUSY)
            dso->controller_result = SALTS_EPROTO;
        atomic_store_explicit(
            &dso->module_b->release_callback, true, memory_order_release);
    } else {
        dso->close_while_callback =
            salts_component_plugin_runtime_close(
                &dso->host, &dso->retired_by_controller);
        if (dso->close_while_callback == SALTS_COMPONENT_PLUGIN_OK &&
            dso->retired_by_controller == &dso->generation) {
            dso->controller_closed_publication = 1;
            dso->drain_while_callback =
                salts_component_plugin_generation_drain(
                    &dso->host, &dso->generation);
        } else
            dso->drain_while_callback = SALTS_COMPONENT_PLUGIN_INVALID_STATE;
        dso->stop_result =
            cmeta_plugin_registry_request_stop(&dso->registry, dso->plugin);
        dso->unload_after_stop =
            cmeta_plugin_registry_unload(&dso->registry, dso->plugin);
    }
    if (dso->controller_result == SALTS_OK || !dso->ab_publication)
        dso->controller_result = SALTS_OK;
}

static int peer_dso_setup(peer_dso_harness *dso)
{
    const cmeta_plugin_registry_config registry_config = {
        .capacity = dso->ab_publication ? 2U : 1U
    };
    const salts_component_plugin_generation_storage storage = {
        .deployments = dso->deployments,
        .deployment_capacity = 1U,
        .instances = dso->instances,
        .instance_capacity = 1U,
        .dependencies = dso->dependencies,
        .dependency_capacity = 1U,
        .activation_order = dso->activation_order,
        .activation_capacity = 1U,
        .modules = dso->modules,
        .module_capacity = 1U
    };
    const salts_component_plugin_source source = {
        .export_id = "turboraft.ace23.dso-component"
    };
    salts_component_plugin_source bound_source = source;
    salts_component_plugin_generation *previous = NULL;
    cmeta_plugin_lease temporary = {0};
    salts_component_service service = {0};
    cmeta_plugin_lifecycle_info info = {0};
    const cmeta_plugin_manifest *manifest = NULL;

    if (cmeta_plugin_registry_init(&dso->registry, &registry_config) !=
        CMETA_PLUGIN_OK) return SALTS_EPROTO;
    dso->registry_open = 1;
    if (cmeta_plugin_registry_load(
            &dso->registry, TURBORAFT_ACE23_EPOCH_DSO_PATH,
            &dso->plugin) != CMETA_PLUGIN_OK)
        return SALTS_EPROTO;
    dso->loaded = 1;
    if (cmeta_plugin_registry_start(
            &dso->registry, dso->plugin) != CMETA_PLUGIN_OK)
        return SALTS_EPROTO;
    dso->started = 1;

    if (salts_component_plugin_runtime_init(&dso->host) !=
        SALTS_COMPONENT_PLUGIN_OK) return SALTS_EPROTO;
    dso->host_open = 1;
    bound_source.plugin = dso->plugin;
    /* ComponentPlugin itself resolves the DSO's ABI5 provider export,
     * acquires the ONLY long-lived module lease, creates the Component,
     * and publishes its exact host epoch. This is not an empty generation. */
    if (salts_component_plugin_generation_build(
            &dso->generation, UINT64_C(90010001), &dso->registry,
            &storage, NULL, 0U, &bound_source, 1U, NULL, 0U) !=
        SALTS_COMPONENT_PLUGIN_OK) return SALTS_EPROTO;
    if (dso->generation.module_count != 1U ||
        dso->generation.deployment_count != 1U)
        return SALTS_EPROTO;
    if (salts_component_plugin_runtime_publish(
            &dso->host, &dso->generation, &previous) !=
            SALTS_COMPONENT_PLUGIN_OK || previous != NULL)
        return SALTS_EPROTO;
    dso->published = 1;
    if (salts_component_plugin_scope_acquire(
            &dso->host, &dso->scope) != SALTS_COMPONENT_PLUGIN_OK)
        return SALTS_EPROTO;
    dso->scope_live = 1;

    /* The CNet Owner binds its callback through the actually published
     * Component. The borrowed vtable and ObjectRef remain valid only
     * while the address-stable original Scope pins generation and its
     * Plugin module lease. No manifest callback is used for dispatch. */
    if (salts_component_plugin_scope_find_service(
            &dso->scope, tr_ace23_dso_callback_interface(),
            &service) != SALTS_COMPONENT_PLUGIN_OK)
        return SALTS_EPROTO;
    dso->callback = tr_ace23_dso_callback_bind(NULL, NULL);
    if (tr_ace23_dso_callback_borrow_from_object(
            service.object, service.interfaces,
            &dso->callback) != CMETA_OK ||
        !tr_ace23_dso_callback_valid(&dso->callback))
        return SALTS_EPROTO;

    /* A transient, explicit Plugin borrow obtains test callback-state.
     * Release it immediately. All later CNet callbacks are protected
     * exclusively by the published generation's own module lease. */
    if (cmeta_plugin_registry_acquire(
            &dso->registry, dso->plugin, &temporary,
            &manifest) != CMETA_PLUGIN_OK)
        return SALTS_EPROTO;
    dso->manifest = manifest;
    dso->module = (const tr_ace23_dso_callback_state *)manifest->self;
    if (cmeta_plugin_registry_release(
            &dso->registry, &temporary) != CMETA_PLUGIN_OK)
        return SALTS_EPROTO;
    if (dso->manifest->is_quiescent == NULL || dso->module == NULL ||
        cmeta_plugin_registry_get_lifecycle(
            &dso->registry, dso->plugin, &info) != CMETA_PLUGIN_OK ||
        info.active_leases != 1U ||
        !cmeta_plugin_lease_valid(dso->modules[0].lease))
        return SALTS_EPROTO;
    return SALTS_OK;
}

/* No native DSO may be unloaded while an old Channel callback can
 * borrow it. B is still published/leased when A is actually dlclosed.
 * This is the host's explicit post-CNet-terminal settlement barrier. */
static int peer_dso_finish_ab(peer_dso_harness *dso)
{
    cmeta_plugin_lifecycle_info info = {0};
    bool quiet = false;
    int result = SALTS_OK;

#define AB_REQUIRE(expr, wanted) do { \
    if ((expr) != (wanted)) result = SALTS_EPROTO; \
} while (0)
    if (!dso->controller_closed_publication ||
        dso->retired_by_controller != &dso->generation_b ||
        !dso->scope_live || !dso->scope_b_live ||
        !dso->loaded || !dso->loaded_b ||
        dso->network_callbacks_b != 1U ||
        dso->captured_origin_b.host_module_generation != UINT64_C(90010002) ||
        dso->captured_origin_b.channel_instance == 0U ||
        atomic_load_explicit(
            &dso->module_b->callback_completed, memory_order_acquire) != 1U)
        return SALTS_EBUSY; /* fail closed; never force a live DSO unload */

    AB_REQUIRE(salts_component_plugin_generation_drain(
        &dso->host, &dso->generation),
        SALTS_COMPONENT_PLUGIN_BUSY);
    AB_REQUIRE(salts_component_plugin_scope_release(
        &dso->scope), SALTS_COMPONENT_PLUGIN_OK);
    dso->scope_live = 0;
    AB_REQUIRE(salts_component_plugin_generation_drain(
        &dso->host, &dso->generation), SALTS_COMPONENT_PLUGIN_OK);
    if (result != SALTS_OK) return result;
    AB_REQUIRE(cmeta_plugin_registry_get_lifecycle(
        &dso->registry, dso->plugin, &info), CMETA_PLUGIN_OK);
    if (info.active_leases != 0U) return SALTS_EBUSY;
    AB_REQUIRE(cmeta_plugin_registry_poll_quiescent(
        &dso->registry, dso->plugin, &quiet), CMETA_PLUGIN_OK);
    if (!quiet || result != SALTS_OK) return SALTS_EBUSY;
    AB_REQUIRE(cmeta_plugin_registry_unload(
        &dso->registry, dso->plugin), CMETA_PLUGIN_OK);
    if (result != SALTS_OK) return result;
    dso->loaded = 0;

    /* The B Component interface, Scope and lease must be usable after
     * provider image A has really been unmapped. */
    if (!tr_ace23_dso_callback_valid(&dso->callback_b) ||
        salts_component_plugin_scope_generation_id(&dso->scope_b) !=
            UINT64_C(90010002))
        return SALTS_EPROTO;
    AB_REQUIRE(cmeta_plugin_registry_get_lifecycle(
        &dso->registry, dso->plugin_b, &info), CMETA_PLUGIN_OK);
    if (info.active_leases != 1U) return SALTS_EPROTO;
    AB_REQUIRE(cmeta_plugin_registry_unload(
        &dso->registry, dso->plugin_b), CMETA_PLUGIN_BUSY);
    AB_REQUIRE(salts_component_plugin_generation_drain(
        &dso->host, &dso->generation_b), SALTS_COMPONENT_PLUGIN_BUSY);
    AB_REQUIRE(salts_component_plugin_scope_release(
        &dso->scope_b), SALTS_COMPONENT_PLUGIN_OK);
    dso->scope_b_live = 0;
    AB_REQUIRE(salts_component_plugin_generation_drain(
        &dso->host, &dso->generation_b), SALTS_COMPONENT_PLUGIN_OK);
    if (result != SALTS_OK) return result;
    AB_REQUIRE(cmeta_plugin_registry_get_lifecycle(
        &dso->registry, dso->plugin_b, &info), CMETA_PLUGIN_OK);
    if (info.active_leases != 0U) return SALTS_EPROTO;
    quiet = false;
    AB_REQUIRE(cmeta_plugin_registry_poll_quiescent(
        &dso->registry, dso->plugin_b, &quiet), CMETA_PLUGIN_OK);
    if (!quiet || result != SALTS_OK) return SALTS_EBUSY;
    AB_REQUIRE(cmeta_plugin_registry_unload(
        &dso->registry, dso->plugin_b), CMETA_PLUGIN_OK);
    if (result != SALTS_OK) return result;
    dso->loaded_b = 0;
    dso->published_b = 0;
    dso->published = 0;
    AB_REQUIRE(salts_component_plugin_runtime_destroy(
        &dso->host), SALTS_COMPONENT_PLUGIN_OK);
    AB_REQUIRE(cmeta_plugin_registry_destroy(
        &dso->registry), CMETA_PLUGIN_OK);
#undef AB_REQUIRE
    return result;
}

/* Called ONLY after BOTH CNet clients have drained terminal callbacks
 * and both Channels were destroyed. Plugin is the sole dlclose authority;
 * this test does not copy a Scope or create an alternative DSO lease. */
static int peer_dso_finish(peer_dso_harness *dso)
{
    salts_component_plugin_generation *retired = NULL;
    bool quiet = false;
    int result = SALTS_OK;

#define DSO_CLEAN(expr, wanted) do { \
    if ((expr) != (wanted)) result = SALTS_EPROTO; \
} while (0)
    if (dso->published) {
        if (dso->controller_closed_publication) {
            retired = dso->retired_by_controller;
        } else {
            DSO_CLEAN(salts_component_plugin_runtime_close(
                &dso->host, &retired), SALTS_COMPONENT_PLUGIN_OK);
        }
        if (retired != &dso->generation) result = SALTS_EPROTO;
        if (dso->scope_live) {
            DSO_CLEAN(salts_component_plugin_generation_drain(
                &dso->host, &dso->generation), SALTS_COMPONENT_PLUGIN_BUSY);
            DSO_CLEAN(salts_component_plugin_scope_release(
                &dso->scope), SALTS_COMPONENT_PLUGIN_OK);
            dso->scope_live = 0;
        }
        DSO_CLEAN(salts_component_plugin_generation_drain(
            &dso->host, &dso->generation), SALTS_COMPONENT_PLUGIN_OK);
        dso->published = 0;
    } else if (dso->generation.state == SALTS_COMPONENT_PLUGIN_GENERATION_BUILT) {
        DSO_CLEAN(salts_component_plugin_generation_discard(
            &dso->generation), SALTS_COMPONENT_PLUGIN_OK);
    }
    if (dso->host_open) {
        DSO_CLEAN(salts_component_plugin_runtime_destroy(
            &dso->host), SALTS_COMPONENT_PLUGIN_OK);
        dso->host_open = 0;
    }
    if (dso->loaded) {
        if (dso->started) {
            cmeta_plugin_lifecycle_info info = {0};
            if (cmeta_plugin_registry_get_lifecycle(
                    &dso->registry, dso->plugin, &info) != CMETA_PLUGIN_OK)
                result = SALTS_EPROTO;
            else if (info.state == CMETA_PLUGIN_LIFECYCLE_STARTED) {
                /* Even a failed handshake needs an orderly Plugin stop.
                 * Never poll for quiescence while still STARTED. */
                if (cmeta_plugin_registry_request_stop(
                        &dso->registry, dso->plugin) != CMETA_PLUGIN_OK)
                    result = SALTS_EPROTO;
            }
        }
         if (dso->started) {
            DSO_CLEAN(cmeta_plugin_registry_poll_quiescent(
                &dso->registry, dso->plugin, &quiet), CMETA_PLUGIN_OK);
            if (!quiet) result = SALTS_EPROTO;
        }
        if (quiet || !dso->started) {
            DSO_CLEAN(cmeta_plugin_registry_unload(
                &dso->registry, dso->plugin), CMETA_PLUGIN_OK);
            dso->loaded = 0;
        }
    }
    if (dso->registry_open && !dso->loaded) {
        DSO_CLEAN(cmeta_plugin_registry_destroy(
            &dso->registry), CMETA_PLUGIN_OK);
        dso->registry_open = 0;
    }
#undef DSO_CLEAN
    return result;
}

typedef struct peer_sink {
    tr_raft_node_id_t expected_from;
    tr_raft_node_id_t expected_to;
    size_t count;
    size_t raft_count;
    size_t data_count;
    size_t snapshot_count;
    int violation;
    int provider_b; /* this certified Channel belongs to provider B */
    peer_dso_harness *dso; /* CNet Owner borrows DSO under ComponentPlugin lease */
} peer_sink_t;

typedef struct peer_fixture {
    cnet_client client;
    cnet_client server;
    cnet_listener listener;
    cnet_tls_server tls_server;
    tr_raft_cnet_channel_t *client_channel;
    tr_raft_cnet_channel_t *server_channel;
    tr_raft_cnet_channel_t *client_channel_b;
    tr_raft_cnet_channel_t *server_channel_b;
    cnet_connection outbound;
    cnet_connection inbound;
    cnet_connection outbound_b;
    cnet_connection inbound_b;
    peer_sink_t client_sink;
    peer_sink_t server_sink;
    peer_sink_t client_sink_b;
    peer_sink_t server_sink_b;
    tr_raft_cnet_peer_identity_t client_peer;
    tr_raft_cnet_peer_identity_t server_peer;
    tr_raft_cnet_identity_policy_t client_policy;
    tr_raft_cnet_identity_policy_t server_policy;
    const char *client_fingerprints[1];
    const char *server_fingerprints[1];
    int client_open;
    int server_open;
    int listener_open;
    int tls_open;
} peer_fixture_t;

static tr_raft_handshake_config_t peer_handshake(
    tr_raft_node_id_t local_id, int foreign_cluster)
{
    tr_raft_handshake_config_t config = {0};
    size_t i;
    for (i = 0U; i < sizeof(config.cluster_id.bytes); ++i)
        config.cluster_id.bytes[i] = (uint8_t)(i + 17U);
    if (foreign_cluster) config.cluster_id.bytes[0] ^= 0x55U;
    config.local_node_id = local_id;
    config.process_incarnation.bytes[0] = (uint8_t)local_id;
    config.config_epoch = 1U;
    config.wire_major_min = TR_RAFT_HANDSHAKE_WIRE_MAJOR;
    config.wire_major_max = TR_RAFT_HANDSHAKE_WIRE_MAJOR;
    config.wire_minor_min = TR_RAFT_HANDSHAKE_WIRE_MINOR;
    config.wire_minor_max = TR_RAFT_HANDSHAKE_WIRE_MINOR;
    config.max_frame_size = TR_RAFT_WIRE_MAX_FRAME_SIZE;
    config.max_snapshot_chunk_size = TR_RAFT_WIRE_MAX_SNAPSHOT_CHUNK_BYTES;
    return config;
}

static cnet_client_config peer_client_config(void)
{
    const cnet_client_config config = {
#if defined(_WIN32)
        .backend = NATIVE_IO_BACKEND_IOCP,
#elif defined(__linux__)
        .backend = NATIVE_IO_BACKEND_EPOLL,
#else
        .backend = NATIVE_IO_BACKEND_KQUEUE,
#endif
        .connection_capacity = 2U,
        .command_capacity = 16U,
        .request_capacity = 8U,
        .completion_batch_capacity = 8U,
        .event_capacity = 16U,
        .max_send_bytes = 1024U,
        .receive_buffer_bytes = 1024U,
        .connect_timeout_ms = 2000U,
        .read_timeout_ms = 2000U,
        .write_timeout_ms = 2000U,
        .tls_io_buffer_bytes = CNET_TLS_MIN_IO_BUFFER_BYTES,
        .tls_handshake_timeout_ms = 2000U
    };
    return config;
}

static int peer_record(void *context,
                       const tr_raft_transport_payload_t *payload)
{
    peer_sink_t *sink = (peer_sink_t *)context;
    if (payload == NULL) goto invalid;

    if (payload->kind == TR_RAFT_WIRE_PAYLOAD_RAFT &&
        payload->group_id == 42U &&
        payload->data.raft.from == sink->expected_from &&
        payload->data.raft.to == sink->expected_to &&
        (payload->data.raft.type == TR_RAFT_MSG_HEARTBEAT_REQUEST ||
         payload->data.raft.type == TR_RAFT_MSG_HEARTBEAT_RESPONSE)) {
        ++sink->raft_count;
    } else if (payload->kind == TR_RAFT_WIRE_PAYLOAD_DATA_CHUNK &&
               payload->group_id == 43U &&
               payload->data.data_chunk.from == sink->expected_from &&
               payload->data.data_chunk.to == sink->expected_to &&
               payload->data.data_chunk.stream_id == 9U &&
               payload->data.data_chunk.stream_size == 3U &&
               payload->data.data_chunk.data_length == 3U &&
               payload->data.data_chunk.data != NULL &&
               memcmp(payload->data.data_chunk.data, "abc", 3U) == 0 &&
               payload->data.data_chunk.done) {
        ++sink->data_count;
    } else if (payload->kind == TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_CHUNK &&
               payload->group_id == 43U &&
               payload->data.snapshot_chunk.from == sink->expected_from &&
               payload->data.snapshot_chunk.to == sink->expected_to &&
               payload->data.snapshot_chunk.snapshot_index == 19U &&
               payload->data.snapshot_chunk.snapshot_size == 3U &&
               payload->data.snapshot_chunk.data_length == 3U &&
               payload->data.snapshot_chunk.data != NULL &&
               memcmp(payload->data.snapshot_chunk.data, "xyz", 3U) == 0 &&
               payload->data.snapshot_chunk.done &&
               payload->data.snapshot_chunk.has_configuration) {
        ++sink->snapshot_count;
    } else {
        goto invalid;
    }

    ++sink->count;
    if (sink->dso != NULL) {
        /* CNet TLS and reciprocal HELLO must be ACTIVE on this Owner;
         * the admission ticket takes its epoch from the stable host scope,
         * never from a DSO-local static, pointer or recycled socket slot. */
        tr_raft_transport_reply_origin_t origin = {0};
        tr_raft_cnet_channel_t *channel = sink->provider_b
            ? sink->dso->verified_channel_b : sink->dso->verified_channel;
        salts_component_plugin_scope *scope = sink->provider_b
            ? &sink->dso->scope_b : &sink->dso->scope;
        tr_ace23_dso_callback *callback = sink->provider_b
            ? &sink->dso->callback_b : &sink->dso->callback;
        if (channel == NULL ||
            tr_raft_cnet_channel_capture_reply_origin(
                channel, payload->group_id, &origin) != SALTS_OK ||
            origin.host_module_generation !=
                salts_component_plugin_scope_generation_id(scope) ||
            origin.authenticated_peer_node_id != sink->expected_from ||
            origin.group_id != payload->group_id ||
            origin.channel_instance == 0U ||
            origin.connection_token == 0U)
            goto invalid;
        if (sink->provider_b)
            sink->dso->captured_origin_b = origin;
        else
            sink->dso->captured_origin = origin;

        /* The caller-driven CNet Owner dispatches to the exact published
         * provider generation from this Channel, never the old A vtable. */
        if (tr_ace23_dso_callback_invoke(callback) != 1)
            goto invalid;
        if (sink->provider_b) ++sink->dso->network_callbacks_b;
        else ++sink->dso->network_callbacks;
    }
    return SALTS_OK;
invalid:
    sink->violation = 1;
    return SALTS_EPROTO;
}

/* The Raft-specific binding must not replace the caller's snapshot SPI. */
static int peer_snapshot_stub(void *context,
                              const tr_raft_snapshot_request_t *request)
{
    (void)context;
    (void)request;
    return SALTS_OK;
}

static int peer_fixture_path(char *output, size_t capacity,
                             const char *file)
{
    int n = snprintf(output, capacity, "%s/%s",
                     TURBORAFT_ACE23_FIXTURE_DIR, file);
    return n > 0 && (size_t)n < capacity ? SALTS_OK : SALTS_ERANGE;
}

static int peer_case_run(int mode)
{
    peer_fixture_t f = {0};
    peer_dso_harness dso = {0};
    int safe_to_unload = 1;
    cnet_client_config client_config = peer_client_config();
    cnet_listener_config listen = {
        .backend = client_config.backend,
        .host = "127.0.0.1",
        .port = 0U,
        .backlog = 2U
    };
    cnet_tls_server_config server_tls = {0};
    cnet_tls_client_config client_tls = {0};
    tr_raft_cnet_channel_config_t client_channel = {0};
    tr_raft_cnet_channel_config_t server_channel = {0};
    tr_raft_cnet_channel_config_t client_channel_b = {0};
    tr_raft_cnet_channel_config_t server_channel_b = {0};
    tr_raft_cnet_channel_status_t cs = {0};
    tr_raft_cnet_channel_status_t ss = {0};
    tr_raft_cnet_channel_status_t cs_b = {0};
    tr_raft_cnet_channel_status_t ss_b = {0};
    cnet_connect_options options = {0};
    tr_raft_cnet_channel_group_binding_t raft_group = {0};
    tr_raft_transport_t raft_transport = {0};
    char ca_file[512], client_cert[512], client_key[512];
    char server_cert[512], server_key[512], uri[128];
    uint16_t port = 0U;
    size_t events = 0U;
    unsigned iteration;
    int accepted = 0;
    int sent = 0;
    int connected_b = 0;
    int accepted_b = 0;
    int sent_b = 0;
    int result = SALTS_OK;
    const char *error_stage = "none";

#define PEER_TRY(expr) do { \
    result = (expr); \
    if (result != SALTS_OK) { error_stage = #expr; goto cleanup; } \
} while (0)

    if (mode == PEER_ABORT_SG) {
        /* Only this terminal-race fixture enlarges CNet's explicit maximum
         * logical write. All ordinary small-message tests keep their limit. */
        client_config.max_send_bytes = TR_RAFT_TRANSPORT_MAX_PACKET_SIZE;
    }
    if (mode == PEER_DSO_CALLBACK || mode == PEER_DSO_AB_PUBLICATION) {
        dso.ab_publication = mode == PEER_DSO_AB_PUBLICATION;
        PEER_TRY(peer_dso_setup(&dso));
        f.server_sink.dso = &dso;
    }
    f.client_sink.expected_from = 2U;
    f.client_sink.expected_to = mode == PEER_FORGED_NODE ? 3U : 1U;
    f.server_sink.expected_from = 1U;
    f.server_sink.expected_to = 2U;
    f.client_sink_b.expected_from = 2U;
    f.client_sink_b.expected_to = 1U;
    f.server_sink_b.expected_from = 1U;
    f.server_sink_b.expected_to = 2U;
    f.server_sink_b.provider_b = 1;
    if (mode == PEER_DSO_AB_PUBLICATION)
        f.server_sink_b.dso = &dso;

    f.client_fingerprints[0] = CERT_NODE2;
    f.server_fingerprints[0] = mode == PEER_UNAUTHORIZED_CERT
        ? CERT_NODE2 : CERT_NODE1;
    f.client_peer = (tr_raft_cnet_peer_identity_t){
        2U, f.client_fingerprints, 1U
    };
    f.server_peer = (tr_raft_cnet_peer_identity_t){
        1U, f.server_fingerprints, 1U
    };
    f.client_policy = (tr_raft_cnet_identity_policy_t){
        mode == PEER_FORGED_NODE ? 3U : 1U, &f.client_peer, 1U
    };
    f.server_policy = (tr_raft_cnet_identity_policy_t){
        2U, &f.server_peer, 1U
    };

    PEER_TRY(peer_fixture_path(ca_file, sizeof(ca_file), "ca.pem"));
    PEER_TRY(peer_fixture_path(client_cert, sizeof(client_cert), "node1-cert.pem"));
    PEER_TRY(peer_fixture_path(client_key, sizeof(client_key), "node1-key.pem"));
    PEER_TRY(peer_fixture_path(server_cert, sizeof(server_cert), "node2-cert.pem"));
    PEER_TRY(peer_fixture_path(server_key, sizeof(server_key), "node2-key.pem"));

    server_tls.size = sizeof(server_tls);
    server_tls.cert_file = server_cert;
    server_tls.key_file = server_key;
    server_tls.ca_file = ca_file;
    server_tls.client_auth = CNET_TLS_CLIENT_AUTH_REQUIRED;
    PEER_TRY(cnet_tls_server_init(&f.tls_server, &server_tls));
    f.tls_open = 1;
    PEER_TRY(cnet_client_init(&f.server, &client_config));
    f.server_open = 1;
    PEER_TRY(cnet_client_init(&f.client, &client_config));
    f.client_open = 1;
    PEER_TRY(cnet_listener_init(&f.listener, &listen));
    f.listener_open = 1;
    PEER_TRY(cnet_listener_port(&f.listener, &port));
    if (snprintf(uri, sizeof(uri), "tls://127.0.0.1:%u",
                 (unsigned)port) <= 0) {
        result = SALTS_EINVAL;
        goto cleanup;
    }

    client_channel.client = &f.client;
    client_channel.identity = &f.client_policy;
    client_channel.handshake = peer_handshake(
        mode == PEER_FORGED_NODE ? 3U : 1U,
        mode == PEER_FOREIGN_CLUSTER);
    client_channel.first_outbound_message_id = 1U;
    client_channel.host_module_generation = UINT64_C(90010001);
    client_channel.on_payload = peer_record;
    client_channel.payload_context = &f.client_sink;
    server_channel.client = &f.server;
    server_channel.identity = &f.server_policy;
    server_channel.handshake = peer_handshake(2U, 0);
    server_channel.first_outbound_message_id = 1U;
    server_channel.host_module_generation =
        (mode == PEER_DSO_CALLBACK || mode == PEER_DSO_AB_PUBLICATION)
        ? salts_component_plugin_scope_generation_id(&dso.scope)
        : UINT64_C(90010001);
    server_channel.on_payload = peer_record;
    server_channel.payload_context = &f.server_sink;
    /* A DSO-local static serial is insufficient: the loader/host must
     * supply a nonzero epoch before ANY Channel can be published. */
    {
        tr_raft_cnet_channel_config_t invalid = client_channel;
        tr_raft_cnet_channel_t *unused = (tr_raft_cnet_channel_t *)(uintptr_t)1U;
        invalid.host_module_generation = 0U;
        if (tr_raft_cnet_channel_create(&invalid, &unused) != SALTS_EINVAL ||
            unused != NULL) {
            result = SALTS_EPROTO;
            error_stage = "missing host DSO epoch accepted";
            goto cleanup;
        }
    }
    PEER_TRY(tr_raft_cnet_channel_create(
        &client_channel, &f.client_channel));
    PEER_TRY(tr_raft_cnet_channel_create(
        &server_channel, &f.server_channel));
    if (mode == PEER_DSO_CALLBACK || mode == PEER_DSO_AB_PUBLICATION)
        dso.verified_channel = f.server_channel;

    /* Compose the existing Service/Runtime Transport SPI without creating a
     * second queue or moving the network connection across owners. */
    raft_group.channel = f.client_channel;
    raft_group.group_id = 42U;
    raft_transport.snapshot_context = &f;
    raft_transport.enqueue_snapshot = peer_snapshot_stub;
    PEER_TRY(tr_raft_cnet_channel_group_transport_bind(
        &raft_group, &raft_transport));
    if (raft_transport.context != &raft_group ||
        raft_transport.snapshot_context != &f ||
        raft_transport.enqueue_snapshot != peer_snapshot_stub ||
        tr_raft_cnet_channel_group_transport_bind(
            &raft_group, &raft_transport) != SALTS_EALREADY) {
        error_stage = "single-bind owned transport adapter";
        result = SALTS_EPROTO;
        goto cleanup;
    }

    /* A Raft payload cannot be emitted before reciprocal HELLO/ACK. */
    {
        tr_raft_transport_payload_t premature = {0};
        premature.group_id = 42U;
        premature.kind = TR_RAFT_WIRE_PAYLOAD_RAFT;
        if (tr_raft_cnet_channel_send(f.client_channel, &premature) !=
                SALTS_EBUSY ||
            raft_transport.enqueue(raft_transport.context,
                                   &premature.data.raft) != SALTS_ENOSPC) {
            error_stage = "admission before TLS";
            result = SALTS_EPROTO;
            goto cleanup;
        }
    }

    client_tls.size = sizeof(client_tls);
    client_tls.ca_file = ca_file;
    client_tls.cert_file = client_cert;
    client_tls.key_file = client_key;
    client_tls.server_name = "node-2.mesh";
    options.uri = uri;
    options.tls = &client_tls;
    options.observer = tr_raft_cnet_channel_observer(f.client_channel);
    PEER_TRY(cnet_connect(&f.client, &options, &f.outbound));
    PEER_TRY(tr_raft_cnet_channel_attach(f.client_channel, f.outbound));

    for (iteration = 0U; iteration < 4000U; ++iteration) {
        int ready = 0;
        PEER_TRY(cnet_client_poll(&f.client, 1U, &events));
        if (!accepted) {
            PEER_TRY(cnet_listener_wait(&f.listener, 0U, &ready));
            if (ready) {
                cnet_observer observer =
                    tr_raft_cnet_channel_observer(f.server_channel);
                PEER_TRY(cnet_listener_accept_tls(
                    &f.listener, &f.server, &f.tls_server, &observer, &f.inbound));
                PEER_TRY(tr_raft_cnet_channel_attach(f.server_channel, f.inbound));
                accepted = 1;
            }
        }
        /* The second certified TLS connection has its own CNet
         * slot/generation, Channel instance and B Component generation.
         * A remains live until its terminal callback is drained. */
        if (mode == PEER_DSO_AB_PUBLICATION &&
            connected_b && !accepted_b) {
            ready = 0;
            PEER_TRY(cnet_listener_wait(&f.listener, 0U, &ready));
            if (ready) {
                cnet_observer observer =
                    tr_raft_cnet_channel_observer(f.server_channel_b);
                PEER_TRY(cnet_listener_accept_tls(
                    &f.listener, &f.server, &f.tls_server,
                    &observer, &f.inbound_b));
                PEER_TRY(tr_raft_cnet_channel_attach(
                    f.server_channel_b, f.inbound_b));
                accepted_b = 1;
            }
        }
        PEER_TRY(cnet_client_poll(&f.server, 1U, &events));
        PEER_TRY(tr_raft_cnet_channel_get_status(f.client_channel, &cs));
        PEER_TRY(tr_raft_cnet_channel_get_status(f.server_channel, &ss));

        if (mode != PEER_VALID && mode != PEER_STREAMS &&
            mode != PEER_ABORT_SG && mode != PEER_DSO_CALLBACK &&
            mode != PEER_DSO_AB_PUBLICATION) {
            if (ss.phase == TR_RAFT_CNET_CHANNEL_FAILED)
                break;
            continue;
        }

        if (cs.phase == TR_RAFT_CNET_CHANNEL_FAILED ||
            ss.phase == TR_RAFT_CNET_CHANNEL_FAILED) {
            result = SALTS_EPROTO;
            error_stage = "unexpected handshake failure";
            goto cleanup;
        }

        if (!sent &&
            cs.phase == TR_RAFT_CNET_CHANNEL_ACTIVE &&
            ss.phase == TR_RAFT_CNET_CHANNEL_ACTIVE) {
            if (mode == PEER_DSO_CALLBACK ||
                mode == PEER_DSO_AB_PUBLICATION) {
                /* The secondary OS thread only owns Component publication.
                 * It never polls/tears down a CNet Channel. It
                 * MUST NOT call CNet poll/stop or own a network callback. */
                PEER_TRY(cmeta_thread_create(
                    &dso.controller, peer_dso_controller, &dso));
                dso.controller_live = 1;
            }
            if (mode == PEER_ABORT_SG) {
                tr_raft_transport_payload_t data = {0};
                tr_raft_transport_payload_t snapshot = {0};
                tr_raft_cnet_channel_status_t before_close = {0};
                tr_raft_transport_reply_origin_t origin = {0};
                tr_raft_multicore_completion_t delayed = {0};
                uint8_t data_bytes[TR_RAFT_WIRE_MAX_DATA_CHUNK_BYTES];
                uint8_t snapshot_bytes[TR_RAFT_WIRE_MAX_SNAPSHOT_CHUNK_BYTES];

                memset(data_bytes, 0xa5, sizeof(data_bytes));
                memset(snapshot_bytes, 0x5a, sizeof(snapshot_bytes));
                data.group_id = 43U;
                data.kind = TR_RAFT_WIRE_PAYLOAD_DATA_CHUNK;
                data.data.data_chunk.from = 1U;
                data.data.data_chunk.to = 2U;
                data.data.data_chunk.term = 3U;
                data.data.data_chunk.stream_id = 19U;
                data.data.data_chunk.stream_size = sizeof(data_bytes);
                data.data.data_chunk.data_length = sizeof(data_bytes);
                data.data.data_chunk.data = data_bytes;
                data.data.data_chunk.done = true;

                snapshot.group_id = 43U;
                snapshot.kind = TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_CHUNK;
                snapshot.data.snapshot_chunk.from = 1U;
                snapshot.data.snapshot_chunk.to = 2U;
                snapshot.data.snapshot_chunk.term = 3U;
                snapshot.data.snapshot_chunk.snapshot_index = 19U;
                snapshot.data.snapshot_chunk.snapshot_term = 3U;
                snapshot.data.snapshot_chunk.snapshot_size = sizeof(snapshot_bytes);
                snapshot.data.snapshot_chunk.data_length = sizeof(snapshot_bytes);
                snapshot.data.snapshot_chunk.data = snapshot_bytes;
                snapshot.data.snapshot_chunk.done = true;
                snapshot.data.snapshot_chunk.has_configuration = true;
                snapshot.data.snapshot_chunk.configuration.phase =
                    TR_RAFT_CONF_FINAL;
                snapshot.data.snapshot_chunk.configuration.member_count = 1U;
                snapshot.data.snapshot_chunk.configuration.members[0].node_id = 2U;
                snapshot.data.snapshot_chunk.configuration.members[0].roles =
                    TR_RAFT_CONF_OLD_VOTER | TR_RAFT_CONF_NEW_VOTER;

                /* Two full 64KiB chunks are admitted but never progressed
                 * by this CNet Owner before close. CNet alone retains their
                 * buffers; neither chunk is a second TurboRaft-owned queue. */
                PEER_TRY(tr_raft_cnet_channel_send(f.client_channel, &data));
                PEER_TRY(tr_raft_cnet_channel_send(f.client_channel, &snapshot));
                PEER_TRY(tr_raft_cnet_channel_get_status(
                    f.client_channel, &before_close));
                if (before_close.sg_chunks_admitted != 2U ||
                    before_close.payloads_admitted != 2U ||
                    before_close.payload_writes_pending != 2U ||
                    before_close.payloads_completed != 0U ||
                    before_close.payloads_canceled != 0U) {
                    result = SALTS_EPROTO;
                    error_stage = "two SG logical writes must remain pending";
                    goto cleanup;
                }
                /* Borrowed memory may be overwritten immediately: each
                 * accepted send has already materialized canonical buffers. */
                memset(data_bytes, 0x3c, sizeof(data_bytes));
                memset(snapshot_bytes, 0xc3, sizeof(snapshot_bytes));

                /* The same client Owner has verified this mTLS Channel.
                 * Capture a receipt BEFORE host stop, then revoke it
                 * synchronously even though the terminal callback and
                 * outstanding CNet SG writes have not yet finished. */
                PEER_TRY(tr_raft_cnet_channel_capture_reply_origin(
                    f.client_channel, 43U, &origin));
                if (origin.host_module_generation != UINT64_C(90010001) ||
                    origin.channel_instance == 0U ||
                    origin.authenticated_peer_node_id != 2U ||
                    origin.connection_token == 0U) {
                    result = SALTS_EPROTO;
                    error_stage = "authenticated Channel ticket missing host epoch";
                    goto cleanup;
                }
                delayed.request_id = 77U;
                delayed.operation = TR_RAFT_MULTICORE_RECEIVE_CHUNK;
                delayed.result = SALTS_OK;
                delayed.reply_origin = origin;
                delayed.value.chunk.kind = TR_RAFT_WIRE_PAYLOAD_DATA_CHUNK;
                delayed.value.chunk.ack_valid = true;
                delayed.value.chunk.ack.data.from = 1U;
                delayed.value.chunk.ack.data.to = 2U;
                delayed.value.chunk.ack.data.term = 3U;
                delayed.value.chunk.ack.data.stream_id = 19U;
                delayed.value.chunk.ack.data.stream_size = sizeof(data_bytes);
                delayed.value.chunk.ack.data.next_offset = 32U;
                delayed.value.chunk.ack.data.accepted = true;

                PEER_TRY(tr_raft_cnet_channel_stop(f.client_channel));
                if (tr_raft_cnet_channel_send_chunk_completion(
                        f.client_channel, &delayed) != SALTS_ECANCELED ||
                    tr_raft_cnet_channel_capture_reply_origin(
                        f.client_channel, 43U, &origin) != SALTS_EBUSY ||
                    origin.host_module_generation != 0U ||
                    origin.channel_instance != 0U) {
                    result = SALTS_EPROTO;
                    error_stage = "host stop did not revoke pending ACK generation";
                    goto cleanup;
                }
                PEER_TRY(tr_raft_cnet_channel_get_status(
                    f.client_channel, &before_close));
                if (before_close.payloads_admitted != 2U) {
                    result = SALTS_EPROTO;
                    error_stage = "revoked ACK consumed another CNet send credit";
                    goto cleanup;
                }
                if (tr_raft_cnet_channel_send(f.client_channel, &data) !=
                        SALTS_EBUSY) {
                    result = SALTS_EPROTO;
                    error_stage = "closed SG Channel admitted another write";
                    goto cleanup;
                }
                sent = 1;
                break; /* stop() itself may advance writes during draining */
            }
            tr_raft_transport_payload_t request = {0};
            tr_raft_transport_payload_t response = {0};
            request.group_id = 42U;
            request.kind = TR_RAFT_WIRE_PAYLOAD_RAFT;
            request.data.raft.type = TR_RAFT_MSG_HEARTBEAT_REQUEST;
            request.data.raft.from = 1U;
            request.data.raft.to = 2U;
            request.data.raft.term = 3U;
            response.group_id = 42U;
            response.kind = TR_RAFT_WIRE_PAYLOAD_RAFT;
            response.data.raft.type = TR_RAFT_MSG_HEARTBEAT_RESPONSE;
            response.data.raft.from = 2U;
            response.data.raft.to = 1U;
            response.data.raft.term = 3U;
            PEER_TRY(raft_transport.enqueue(raft_transport.context,
                                             &request.data.raft));
            PEER_TRY(tr_raft_cnet_channel_send(f.server_channel, &response));
            if (mode == PEER_STREAMS) {
                tr_raft_transport_payload_t data = {0};
                tr_raft_transport_payload_t snapshot = {0};
                size_t i;
                data.group_id = 43U;
                data.kind = TR_RAFT_WIRE_PAYLOAD_DATA_CHUNK;
                data.data.data_chunk.from = 1U;
                data.data.data_chunk.to = 2U;
                data.data.data_chunk.term = 3U;
                data.data.data_chunk.stream_id = 9U;
                data.data.data_chunk.stream_size = 3U;
                data.data.data_chunk.data = (const uint8_t *)"abc";
                data.data.data_chunk.data_length = 3U;
                data.data.data_chunk.done = true;
                memset(data.data.data_chunk.stream_digest, 0xa4,
                       sizeof(data.data.data_chunk.stream_digest));

                snapshot.group_id = 43U;
                snapshot.kind = TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_CHUNK;
                snapshot.data.snapshot_chunk.from = 1U;
                snapshot.data.snapshot_chunk.to = 2U;
                snapshot.data.snapshot_chunk.term = 3U;
                snapshot.data.snapshot_chunk.snapshot_index = 19U;
                snapshot.data.snapshot_chunk.snapshot_term = 3U;
                snapshot.data.snapshot_chunk.snapshot_size = 3U;
                snapshot.data.snapshot_chunk.data = (const uint8_t *)"xyz";
                snapshot.data.snapshot_chunk.data_length = 3U;
                snapshot.data.snapshot_chunk.done = true;
                snapshot.data.snapshot_chunk.has_configuration = true;
                snapshot.data.snapshot_chunk.configuration.phase =
                    TR_RAFT_CONF_FINAL;
                snapshot.data.snapshot_chunk.configuration.member_count = 1U;
                snapshot.data.snapshot_chunk.configuration.members[0].node_id = 2U;
                snapshot.data.snapshot_chunk.configuration.members[0].roles =
                    TR_RAFT_CONF_OLD_VOTER | TR_RAFT_CONF_NEW_VOTER;
                for (i = 0U;
                     i < sizeof(snapshot.data.snapshot_chunk.snapshot_digest);
                     ++i)
                    snapshot.data.snapshot_chunk.snapshot_digest[i] =
                        (uint8_t)(0x50U + i);

                PEER_TRY(tr_raft_cnet_channel_send(f.client_channel, &data));
                PEER_TRY(tr_raft_cnet_channel_send(f.client_channel, &snapshot));
            }
            sent = 1;
        }
        /* Only the original certified CNet Owner creates/attaches B's
         * transport after generation B has published and the A callback
         * returned. The controller never touches socket/Channel state. */
        if (mode == PEER_DSO_AB_PUBLICATION && sent &&
            !connected_b && f.server_sink.count == 1U &&
            atomic_load_explicit(&dso.b_ready, memory_order_acquire)) {
            client_channel_b = client_channel;
            server_channel_b = server_channel;
            client_channel_b.host_module_generation =
                salts_component_plugin_scope_generation_id(&dso.scope_b);
            server_channel_b.host_module_generation =
                salts_component_plugin_scope_generation_id(&dso.scope_b);
            client_channel_b.payload_context = &f.client_sink_b;
            server_channel_b.payload_context = &f.server_sink_b;
            PEER_TRY(tr_raft_cnet_channel_create(
                &client_channel_b, &f.client_channel_b));
            PEER_TRY(tr_raft_cnet_channel_create(
                &server_channel_b, &f.server_channel_b));
            dso.verified_channel_b = f.server_channel_b;
            options.observer =
                tr_raft_cnet_channel_observer(f.client_channel_b);
            PEER_TRY(cnet_connect(&f.client, &options, &f.outbound_b));
            PEER_TRY(tr_raft_cnet_channel_attach(
                f.client_channel_b, f.outbound_b));
            connected_b = 1;
        }
        if (mode == PEER_DSO_AB_PUBLICATION && connected_b && accepted_b &&
            !sent_b) {
            tr_raft_transport_payload_t request = {0}, response = {0};
            PEER_TRY(tr_raft_cnet_channel_get_status(
                f.client_channel_b, &cs_b));
            PEER_TRY(tr_raft_cnet_channel_get_status(
                f.server_channel_b, &ss_b));
            if (cs_b.phase == TR_RAFT_CNET_CHANNEL_FAILED ||
                ss_b.phase == TR_RAFT_CNET_CHANNEL_FAILED) {
                result = SALTS_EPROTO;
                error_stage = "second provider TLS authentication failed";
                goto cleanup;
            }
            if (cs_b.phase == TR_RAFT_CNET_CHANNEL_ACTIVE &&
                ss_b.phase == TR_RAFT_CNET_CHANNEL_ACTIVE) {
                request.group_id = response.group_id = 42U;
                request.kind = response.kind = TR_RAFT_WIRE_PAYLOAD_RAFT;
                request.data.raft.type = TR_RAFT_MSG_HEARTBEAT_REQUEST;
                request.data.raft.from = 1U;
                request.data.raft.to = 2U;
                request.data.raft.term = 3U;
                response.data.raft.type = TR_RAFT_MSG_HEARTBEAT_RESPONSE;
                response.data.raft.from = 2U;
                response.data.raft.to = 1U;
                response.data.raft.term = 3U;
                PEER_TRY(tr_raft_cnet_channel_send(
                    f.client_channel_b, &request));
                PEER_TRY(tr_raft_cnet_channel_send(
                    f.server_channel_b, &response));
                sent_b = 1;
            }
        }
        if (sent && f.client_sink.count == 1U &&
            f.server_sink.count == (mode == PEER_STREAMS ? 3U : 1U) &&
            (mode != PEER_DSO_AB_PUBLICATION ||
             (sent_b && f.client_sink_b.count == 1U &&
              f.server_sink_b.count == 1U)))
            break;
    }

    if (mode == PEER_ABORT_SG) {
        PEER_TRY(tr_raft_cnet_channel_get_status(f.client_channel, &cs));
        if (!sent || cs.sg_chunks_admitted != 2U ||
            cs.payloads_admitted != 2U ||
            cs.payload_writes_pending != 2U ||
            cs.payloads_completed != 0U ||
            cs.payloads_canceled != 0U) {
            result = SALTS_EPROTO;
            error_stage = "pending SG send must retain both logical credits";
        }
    } else if (mode == PEER_VALID || mode == PEER_STREAMS ||
               mode == PEER_DSO_CALLBACK || mode == PEER_DSO_AB_PUBLICATION) {
        const size_t expected_server = mode == PEER_STREAMS ? 3U : 1U;
        /* Prove actual CNet logical write completions, not merely terminal
         * cancellation balancing the ledger after a successful receive. */
        for (iteration = 0U; iteration < 1000U; ++iteration) {
            PEER_TRY(cnet_client_poll(&f.client, 1U, &events));
            PEER_TRY(cnet_client_poll(&f.server, 1U, &events));
            PEER_TRY(tr_raft_cnet_channel_get_status(f.client_channel, &cs));
            PEER_TRY(tr_raft_cnet_channel_get_status(f.server_channel, &ss));
            if (cs.payloads_completed == expected_server &&
                ss.payloads_completed == 1U)
                break;
        }
        if (iteration == 1000U) {
            result = SALTS_ETIMEDOUT;
            error_stage = "authenticated CNet on_send never settled logical payloads";
            goto cleanup;
        }
        PEER_TRY(tr_raft_cnet_channel_get_status(f.client_channel, &cs));
        PEER_TRY(tr_raft_cnet_channel_get_status(f.server_channel, &ss));
        if (cs.payloads_admitted != expected_server ||
            ss.payloads_admitted != 1U ||
            cs.sg_chunks_admitted != (mode == PEER_STREAMS ? 2U : 0U) ||
            ss.sg_chunks_admitted != 0U ||
            cs.payloads_canceled != 0U || ss.payloads_canceled != 0U ||
            cs.payloads_admitted != cs.payloads_completed +
                cs.payloads_canceled + cs.payload_writes_pending ||
            ss.payloads_admitted != ss.payloads_completed +
                ss.payloads_canceled + ss.payload_writes_pending) {
            result = SALTS_EPROTO;
            error_stage = "live TLS logical-send credit conservation";
            goto cleanup;
        }
        if (cs.phase != TR_RAFT_CNET_CHANNEL_ACTIVE ||
            ss.phase != TR_RAFT_CNET_CHANNEL_ACTIVE ||
            f.client_sink.count != 1U || f.server_sink.count != expected_server ||
            f.client_sink.violation || f.server_sink.violation ||
            f.client_sink.raft_count != 1U || f.server_sink.raft_count != 1U ||
            f.server_sink.data_count != (mode == PEER_STREAMS ? 1U : 0U) ||
            f.server_sink.snapshot_count != (mode == PEER_STREAMS ? 1U : 0U) ||
            cs.payloads_received != 1U ||
            ss.payloads_received != expected_server ||
            cs.handshake_packets_sent != 2U ||
            ss.handshake_packets_sent != 2U) {
            result = SALTS_EPROTO;
            error_stage = "bidirectional verified handshakes and Raft";
        }
        if (result == SALTS_OK &&
            (mode == PEER_DSO_CALLBACK || mode == PEER_DSO_AB_PUBLICATION)) {
            if (dso.network_callbacks != 1U ||
                dso.captured_origin.host_module_generation !=
                    salts_component_plugin_scope_generation_id(&dso.scope) ||
                dso.captured_origin.channel_instance == 0U ||
                dso.captured_origin.authenticated_peer_node_id != 1U ||
                dso.captured_origin.group_id != 42U ||
                dso.captured_origin.connection_token == 0U ||
                atomic_load_explicit(
                    &dso.module->callback_entered, memory_order_acquire) != 1U ||
                atomic_load_explicit(
                    &dso.module->callback_completed, memory_order_acquire) != 1U) {
                result = SALTS_EPROTO;
                error_stage = "verified CNet callback did not enter DSO once";
            }
        }
        if (result == SALTS_OK && mode == PEER_DSO_AB_PUBLICATION) {
            /* B has a physically distinct certified TLS connection and
             * Component provider, not only a Plugin callback on another
             * worker. A's old Channel/scope still exist until terminal. */
            if (!connected_b || !accepted_b || !sent_b ||
                f.client_channel_b == NULL || f.server_channel_b == NULL ||
                !atomic_load_explicit(
                    &dso.b_ready, memory_order_acquire) ||
                dso.generation.state !=
                    SALTS_COMPONENT_PLUGIN_GENERATION_DRAINING ||
                dso.network_callbacks_b != 1U ||
                dso.captured_origin_b.host_module_generation !=
                    salts_component_plugin_scope_generation_id(&dso.scope_b) ||
                dso.captured_origin_b.host_module_generation !=
                    UINT64_C(90010002) ||
                dso.captured_origin_b.channel_instance == 0U ||
                dso.captured_origin_b.authenticated_peer_node_id != 1U ||
                dso.captured_origin_b.group_id != 42U ||
                dso.captured_origin_b.connection_token == 0U ||
                dso.captured_origin_b.channel_instance ==
                    dso.captured_origin.channel_instance ||
                atomic_load_explicit(
                    &dso.module_b->callback_entered, memory_order_acquire) != 1U ||
                atomic_load_explicit(
                    &dso.module_b->callback_completed, memory_order_acquire) != 1U ||
                f.client_sink_b.violation || f.server_sink_b.violation ||
                f.client_sink_b.count != 1U || f.server_sink_b.count != 1U) {
                result = SALTS_EPROTO;
                error_stage = "distinct Provider B TLS callback/generation not qualified";
                goto cleanup;
            }
            for (iteration = 0U; iteration < 1000U; ++iteration) {
                PEER_TRY(cnet_client_poll(&f.client, 1U, &events));
                PEER_TRY(cnet_client_poll(&f.server, 1U, &events));
                PEER_TRY(tr_raft_cnet_channel_get_status(
                    f.client_channel_b, &cs_b));
                PEER_TRY(tr_raft_cnet_channel_get_status(
                    f.server_channel_b, &ss_b));
                if (cs_b.payloads_completed == 1U &&
                    ss_b.payloads_completed == 1U)
                    break;
            }
            if (iteration == 1000U ||
                cs_b.phase != TR_RAFT_CNET_CHANNEL_ACTIVE ||
                ss_b.phase != TR_RAFT_CNET_CHANNEL_ACTIVE ||
                cs_b.payloads_received != 1U ||
                ss_b.payloads_received != 1U ||
                cs_b.payloads_admitted != 1U ||
                ss_b.payloads_admitted != 1U ||
                cs_b.payloads_canceled != 0U ||
                ss_b.payloads_canceled != 0U) {
                result = SALTS_EPROTO;
                error_stage = "Provider B authenticated TLS send/receive incomplete";
                goto cleanup;
            }
            {
                tr_raft_multicore_completion_t old_ack = {0};
                const size_t admitted_before = ss_b.payloads_admitted;
                old_ack.request_id = UINT64_C(99);
                old_ack.operation = TR_RAFT_MULTICORE_RECEIVE_CHUNK;
                old_ack.result = SALTS_OK;
                old_ack.reply_origin = dso.captured_origin;
                old_ack.value.chunk.kind = TR_RAFT_WIRE_PAYLOAD_DATA_CHUNK;
                old_ack.value.chunk.ack_valid = true;
                old_ack.value.chunk.ack.data.accepted = true;
                if (tr_raft_cnet_channel_send_chunk_completion(
                        f.server_channel_b, &old_ack) != SALTS_ECANCELED ||
                    tr_raft_cnet_channel_send_chunk_completion(
                        f.server_channel, &(tr_raft_multicore_completion_t){
                            .request_id = UINT64_C(100),
                            .operation = TR_RAFT_MULTICORE_RECEIVE_CHUNK,
                            .result = SALTS_OK,
                            .reply_origin = dso.captured_origin_b,
                            .value.chunk.kind = TR_RAFT_WIRE_PAYLOAD_DATA_CHUNK,
                            .value.chunk.ack_valid = true
                        }) != SALTS_ECANCELED ||
                    tr_raft_cnet_channel_get_status(
                        f.server_channel_b, &ss_b) != SALTS_OK ||
                    ss_b.payloads_admitted != admitted_before) {
                    result = SALTS_EPROTO;
                    error_stage = "stale A/B cross-generation TLS ACK consumed credits";
                    goto cleanup;
                }
            }
        }
        if (result == SALTS_OK &&
            tr_raft_cnet_channel_destroy(f.client_channel) != SALTS_EBUSY) {
            result = SALTS_EPROTO;
            error_stage = "destroy before terminal";
        }
    } else if (ss.phase != TR_RAFT_CNET_CHANNEL_FAILED ||
               ss.last_error != SALTS_EPROTO ||
               ss.payloads_received != 0U ||
               f.client_sink.count != 0U || f.server_sink.count != 0U) {
        result = SALTS_EPROTO;
        error_stage = "negative identity or cluster gate";
    }

cleanup:
    if (dso.controller_live) {
        const int joined = cmeta_thread_join(&dso.controller);
        cmeta_thread_destroy(&dso.controller);
        dso.controller_live = 0;
        if (result == SALTS_OK &&
            (joined != SALTS_OK ||
             dso.controller_result != SALTS_OK ||
             dso.unload_while_callback != CMETA_PLUGIN_BUSY ||
             dso.close_while_callback != SALTS_COMPONENT_PLUGIN_OK ||
             dso.retired_by_controller !=
                 (dso.ab_publication ? &dso.generation_b : &dso.generation) ||
             (dso.ab_publication &&
              (dso.drain_a_while_b_published != SALTS_COMPONENT_PLUGIN_BUSY ||
               dso.unload_b_before_drain != CMETA_PLUGIN_BUSY ||
               !atomic_load_explicit(
                   &dso.b_ready, memory_order_acquire) ||
               dso.network_callbacks_b != 1U)) ||
             dso.drain_while_callback != SALTS_COMPONENT_PLUGIN_BUSY ||
             !dso.controller_closed_publication ||
             dso.stop_result != CMETA_PLUGIN_OK ||
             dso.unload_after_stop != CMETA_PLUGIN_BUSY)) {
            result = SALTS_EPROTO;
            error_stage = "in-flight CNet on_payload let Salts Plugin unload";
        }
    }
    if (f.client_channel != NULL)
        (void)tr_raft_cnet_channel_stop(f.client_channel);
    if (f.server_channel != NULL)
        (void)tr_raft_cnet_channel_stop(f.server_channel);
    if (f.client_channel_b != NULL)
        (void)tr_raft_cnet_channel_stop(f.client_channel_b);
    if (f.server_channel_b != NULL)
        (void)tr_raft_cnet_channel_stop(f.server_channel_b);
    if (f.client_open) {
        int close_result = cnet_client_stop(&f.client, 2000U);
        if (close_result != SALTS_OK) safe_to_unload = 0;
        if (result == SALTS_OK && close_result != SALTS_OK) {
            result = close_result;
            error_stage = "client stop";
        }
    }
    if (f.server_open) {
        int close_result = cnet_client_stop(&f.server, 2000U);
        if (close_result != SALTS_OK) safe_to_unload = 0;
        if (result == SALTS_OK && close_result != SALTS_OK) {
            result = close_result;
            error_stage = "server stop";
        }
    }
    /* Terminal callbacks, not socket-close requests, settle every locally
     * admitted write. This includes DATA and SNAPSHOT in the TLS streams case,
     * without interpreting local completion as remote fsync/delivery. */
    if (f.client_channel != NULL && f.server_channel != NULL &&
        f.client_open && f.server_open && result == SALTS_OK) {
        tr_raft_cnet_channel_status_t end_client = {0};
        tr_raft_cnet_channel_status_t end_server = {0};
        int left = tr_raft_cnet_channel_get_status(
            f.client_channel, &end_client);
        int right = tr_raft_cnet_channel_get_status(
            f.server_channel, &end_server);
        if (left != SALTS_OK || right != SALTS_OK ||
            !end_client.terminal || !end_server.terminal ||
            end_client.payload_writes_pending != 0U ||
            end_server.payload_writes_pending != 0U ||
            end_client.sg_chunks_admitted >
                end_client.payloads_admitted ||
            end_server.sg_chunks_admitted >
                end_server.payloads_admitted ||
            end_client.payloads_admitted !=
                end_client.payloads_completed + end_client.payloads_canceled ||
            end_server.payloads_admitted !=
                end_server.payloads_completed + end_server.payloads_canceled) {
            result = SALTS_EPROTO;
            error_stage = "CNet terminal write ledger must drain exactly once";
        } else if (mode == PEER_ABORT_SG &&
                   (end_client.sg_chunks_admitted != 2U ||
                    end_client.payloads_admitted != 2U ||
                    end_client.payloads_completed > 2U ||
                    end_client.payloads_canceled > 2U ||
                    end_client.payloads_completed +
                        end_client.payloads_canceled != 2U)) {
            /* cnet_client_stop() progresses native writes while draining.
             * Previously pending SG writes may finish successfully OR be
             * canceled by the terminal callback. Both are terminal exactly
             * once, and neither can be replayed or grant duplicate credit. */
            fprintf(stderr, "SG terminal settled completed=%llu canceled=%llu pending=%zu\n",
                    (unsigned long long)end_client.payloads_completed,
                    (unsigned long long)end_client.payloads_canceled,
                    end_client.payload_writes_pending);
            result = SALTS_EPROTO;
            error_stage = "SG shutdown lost or double-settled logical writes";
        }
    }
    if (mode == PEER_DSO_AB_PUBLICATION &&
        f.client_channel_b != NULL && f.server_channel_b != NULL &&
        result == SALTS_OK) {
        tr_raft_cnet_channel_status_t end_b_client = {0};
        tr_raft_cnet_channel_status_t end_b_server = {0};
        if (tr_raft_cnet_channel_get_status(
                f.client_channel_b, &end_b_client) != SALTS_OK ||
            tr_raft_cnet_channel_get_status(
                f.server_channel_b, &end_b_server) != SALTS_OK ||
            !end_b_client.terminal || !end_b_server.terminal ||
            end_b_client.payload_writes_pending != 0U ||
            end_b_server.payload_writes_pending != 0U ||
            end_b_client.payloads_admitted !=
                end_b_client.payloads_completed + end_b_client.payloads_canceled ||
            end_b_server.payloads_admitted !=
                end_b_server.payloads_completed + end_b_server.payloads_canceled) {
            safe_to_unload = 0;
            result = SALTS_EPROTO;
            error_stage = "Provider B did not fully drain both TLS terminals";
        }
    }
    if (f.client_channel != NULL) {
        int close_result = tr_raft_cnet_channel_destroy(f.client_channel);
        if (close_result != SALTS_OK) safe_to_unload = 0;
        if (result == SALTS_OK && close_result != SALTS_OK)
            result = close_result;
    }
    if (f.server_channel != NULL) {
        int close_result = tr_raft_cnet_channel_destroy(f.server_channel);
        if (close_result != SALTS_OK) safe_to_unload = 0;
        if (result == SALTS_OK && close_result != SALTS_OK)
            result = close_result;
    }
    if (f.client_channel_b != NULL) {
        int close_result = tr_raft_cnet_channel_destroy(f.client_channel_b);
        if (close_result != SALTS_OK) safe_to_unload = 0;
        if (result == SALTS_OK && close_result != SALTS_OK)
            result = close_result;
    }
    if (f.server_channel_b != NULL) {
        int close_result = tr_raft_cnet_channel_destroy(f.server_channel_b);
        if (close_result != SALTS_OK) safe_to_unload = 0;
        if (result == SALTS_OK && close_result != SALTS_OK)
            result = close_result;
    }
    if ((mode == PEER_DSO_CALLBACK ||
         mode == PEER_DSO_AB_PUBLICATION) && !safe_to_unload) {
        /* A live on_payload might still execute code in the DSO. Fail
         * closed without unloading or dropping the active Plugin lease. */
        fprintf(stderr, "CNet terminal barrier incomplete: DSO remains leased\n");
        return SALTS_EBUSY;
    }
    if (f.client_open) (void)cnet_client_destroy(&f.client);
    if (f.server_open) (void)cnet_client_destroy(&f.server);
    if (f.listener_open) {
        (void)cnet_listener_close(&f.listener);
        (void)cnet_listener_destroy(&f.listener);
    }
    if (f.tls_open) (void)cnet_tls_server_destroy(&f.tls_server);
    if (mode == PEER_DSO_CALLBACK || mode == PEER_DSO_AB_PUBLICATION) {
        const int cleanup_status = dso.ab_publication
            ? peer_dso_finish_ab(&dso) : peer_dso_finish(&dso);
        if (result == SALTS_OK && cleanup_status != SALTS_OK) {
            result = cleanup_status;
            error_stage = "post-CNet-terminal Plugin lease/scope drain";
        }
    }
#undef PEER_TRY

    if (result != SALTS_OK)
        fprintf(stderr, "CNet peer channel mode=%d stage=%s result=%d "
                "client-phase=%d server-phase=%d client-last=%d server-last=%d "
                "client-received=%zu server-received=%zu\n",
                mode, error_stage, result,
                (int)cs.phase, (int)ss.phase, cs.last_error, ss.last_error,
                f.client_sink.count, f.server_sink.count);
    return result;
}

spec("ACE 2.3 CNet owner-bound authenticated Raft channel")
{
    it("adopts a verified mTLS peer, completes reciprocal ACKs, and exchanges Raft")
    {
        check_equal(peer_case_run(PEER_VALID), SALTS_OK);
    }

    it("sends group-aware DATA and SNAPSHOT chunks over the same mTLS channel")
    {
        check_equal(peer_case_run(PEER_STREAMS), SALTS_OK);
    }

    it("settles two pending 64KiB TLS SG chunks exactly once on terminal close")
    {
        check_equal(peer_case_run(PEER_ABORT_SG), SALTS_OK);
    }

    it("drains a real mTLS on_payload DSO callback before Plugin unload")
    {
        check_equal(peer_case_run(PEER_DSO_CALLBACK), SALTS_OK);
    }

    it("hot-publishes separate A and B DSOs onto two certified TLS Channels")
    {
        check_equal(peer_case_run(PEER_DSO_AB_PUBLICATION), SALTS_OK);
    }

    it("rejects a different cluster before delivering any Raft payload")
    {
        check_equal(peer_case_run(PEER_FOREIGN_CLUSTER), SALTS_OK);
    }

    it("rejects a forged Raft Node ID despite a valid client certificate")
    {
        check_equal(peer_case_run(PEER_FORGED_NODE), SALTS_OK);
    }

    it("rejects a CA-valid but unauthorized client certificate")
    {
        check_equal(peer_case_run(PEER_UNAUTHORIZED_CERT), SALTS_OK);
    }
}
