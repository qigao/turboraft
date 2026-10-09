#include <turboraft/raft_component.h>

#include <salts/component.h>
#include <cmeta/object_interface.h>
#include <cmeta_error.h>

#include <stdlib.h>

/* Exact, immutable capability declarations; no mutable global registry. */
cmeta_component(tr_raft_StorageAdapter,
    cmeta_provides(tr_raft_storage_source));
cmeta_component(tr_raft_TransportAdapter,
    cmeta_provides(tr_raft_transport_source));
cmeta_component(tr_raft_StateMachineAdapter,
    cmeta_provides(tr_raft_state_machine_source));
cmeta_component(tr_raft_RuntimeComponent,
    cmeta_requires(tr_raft_storage_source)
    cmeta_requires(tr_raft_transport_source)
    cmeta_requires(tr_raft_state_machine_source));

enum {
    TR_COMPONENT_STORAGE = 0,
    TR_COMPONENT_TRANSPORT,
    TR_COMPONENT_STATE_MACHINE,
    TR_COMPONENT_RUNTIME,
    TR_COMPONENT_COUNT
};
enum { TR_COMPONENT_DEPENDENCY_COUNT = 3 };

const cmeta_component_desc *tr_raft_component_descriptor(
    tr_raft_component_kind_t kind)
{
    switch (kind) {
    case TR_RAFT_COMPONENT_STORAGE:
        return cmeta_component_meta(tr_raft_StorageAdapter);
    case TR_RAFT_COMPONENT_TRANSPORT:
        return cmeta_component_meta(tr_raft_TransportAdapter);
    case TR_RAFT_COMPONENT_STATE_MACHINE:
        return cmeta_component_meta(tr_raft_StateMachineAdapter);
    case TR_RAFT_COMPONENT_RUNTIME:
        return cmeta_component_meta(tr_raft_RuntimeComponent);
    default:
        return NULL;
    }
}

struct tr_raft_component_domain {
    tr_raft_runtime_config_t config;
    tr_raft_runtime_t *runtime;

    /* Each ObjectRef truthfully describes one host-owned int identity. */
    int identities[TR_COMPONENT_COUNT];
    cmeta_object_lifecycle runtime_lifecycle;
    cmeta_object_interface_provider interface_provider;

    salts_component_provider_binding bindings[TR_COMPONENT_COUNT];
    salts_component_deployment deployments[TR_COMPONENT_COUNT];
    salts_component_instance instances[TR_COMPONENT_COUNT];
    salts_component_dependency dependencies[TR_COMPONENT_DEPENDENCY_COUNT];
    size_t activation_order[TR_COMPONENT_COUNT];
    salts_component_context components;
};

/* Setup-only: a typed copy of the existing callback record, no new vtable
 * dispatch on Raft entry application, WAL writes or transport hot path. */
static int tr_storage_snapshot(void *self, tr_raft_storage_t *out)
{
    if (self == NULL || out == NULL) return SALTS_EINVAL;
    *out = *(const tr_raft_storage_t *)self;
    return SALTS_OK;
}

static int tr_transport_snapshot(void *self, tr_raft_transport_t *out)
{
    if (self == NULL || out == NULL) return SALTS_EINVAL;
    *out = *(const tr_raft_transport_t *)self;
    return SALTS_OK;
}

static int tr_state_machine_snapshot(void *self, tr_raft_state_machine_t *out)
{
    if (self == NULL || out == NULL) return SALTS_EINVAL;
    *out = *(const tr_raft_state_machine_t *)self;
    return SALTS_OK;
}

CMETA_IMPLEMENTS(tr_raft_storage_source, tr_storage_source_impl, 0u,
    .snapshot = tr_storage_snapshot);
CMETA_IMPLEMENTS(tr_raft_transport_source, tr_transport_source_impl, 0u,
    .snapshot = tr_transport_snapshot);
CMETA_IMPLEMENTS(tr_raft_state_machine_source, tr_state_machine_source_impl, 0u,
    .snapshot = tr_state_machine_snapshot);

CMETA_OBJECT_INTERFACE_ADAPTER(tr_raft_storage_source);
CMETA_OBJECT_INTERFACE_ADAPTER(tr_raft_transport_source);
CMETA_OBJECT_INTERFACE_ADAPTER(tr_raft_state_machine_source);

static cmeta_status tr_component_project(
    void *context,
    const cmeta_object_ref *object,
    const cmeta_interface_desc *expected,
    cmeta_interface_projection *out)
{
    tr_raft_component_domain_t *domain =
        (tr_raft_component_domain_t *)context;

    if (domain == NULL || !cmeta_object_ref_valid(object) ||
        !cmeta_interface_desc_valid(expected) || out == NULL)
        return CMETA_INVALID_ARGUMENT;

    if (object->object == &domain->identities[TR_COMPONENT_STORAGE] &&
        cmeta_interface_desc_equal(expected, tr_raft_storage_source_interface())) {
        out->size = sizeof(*out);
        out->interface = tr_raft_storage_source_interface();
        out->self = &domain->config.storage;
        out->dispatch = &tr_storage_source_impl_vtable;
        return CMETA_OK;
    }
    if (object->object == &domain->identities[TR_COMPONENT_TRANSPORT] &&
        cmeta_interface_desc_equal(expected, tr_raft_transport_source_interface())) {
        out->size = sizeof(*out);
        out->interface = tr_raft_transport_source_interface();
        out->self = &domain->config.transport;
        out->dispatch = &tr_transport_source_impl_vtable;
        return CMETA_OK;
    }
    if (object->object == &domain->identities[TR_COMPONENT_STATE_MACHINE] &&
        cmeta_interface_desc_equal(expected, tr_raft_state_machine_source_interface())) {
        out->size = sizeof(*out);
        out->interface = tr_raft_state_machine_source_interface();
        out->self = &domain->config.state_machine;
        out->dispatch = &tr_state_machine_source_impl_vtable;
        return CMETA_OK;
    }
    return CMETA_TRAIT_MISSING;
}

static cmeta_status tr_adapter_create(
    void *provider_context,
    const cmeta_data_desc *config_data,
    const void *config_value,
    const salts_component_dependency *dependencies,
    size_t dependency_count,
    cmeta_object_ref *out_instance)
{
    (void)dependencies;
    if (provider_context == NULL || out_instance == NULL ||
        config_data != NULL || config_value != NULL ||
        dependency_count != 0u)
        return CMETA_INVALID_ARGUMENT;
    return cmeta_object_borrow(
        out_instance, provider_context, &cmeta_data_int, NULL);
}

static void tr_component_runtime_destroy(void *context, void *object)
{
    tr_raft_component_domain_t *domain =
        (tr_raft_component_domain_t *)context;
    if (domain == NULL ||
        object != &domain->identities[TR_COMPONENT_RUNTIME] ||
        domain->runtime == NULL)
        return;
    (void)tr_raft_runtime_destroy(domain->runtime);
    domain->runtime = NULL;
}

static cmeta_status tr_runtime_component_create(
    void *provider_context,
    const cmeta_data_desc *config_data,
    const void *config_value,
    const salts_component_dependency *dependencies,
    size_t dependency_count,
    cmeta_object_ref *out_instance)
{
    tr_raft_component_domain_t *domain =
        (tr_raft_component_domain_t *)provider_context;
    tr_raft_runtime_config_t config = {0};
    const salts_component_dependency *dependency = NULL;
    tr_raft_storage_source storage = tr_raft_storage_source_bind(NULL, NULL);
    tr_raft_transport_source transport = tr_raft_transport_source_bind(NULL, NULL);
    tr_raft_state_machine_source machine =
        tr_raft_state_machine_source_bind(NULL, NULL);
    cmeta_status status;
    int result;

    if (domain == NULL || out_instance == NULL ||
        config_data != NULL || config_value != NULL ||
        dependency_count != TR_COMPONENT_DEPENDENCY_COUNT ||
        domain->runtime != NULL)
        return CMETA_INVALID_ARGUMENT;

    if (salts_component_dependency_find(dependencies, dependency_count,
            tr_raft_storage_source_interface(), &dependency) != SALTS_COMPONENT_OK)
        return CMETA_CALLBACK_ERROR;
    status = tr_raft_storage_source_borrow_from_object(
        dependency->provider_instance, dependency->provider_interfaces, &storage);
    if (status != CMETA_OK) return status;

    if (salts_component_dependency_find(dependencies, dependency_count,
            tr_raft_transport_source_interface(), &dependency) != SALTS_COMPONENT_OK)
        return CMETA_CALLBACK_ERROR;
    status = tr_raft_transport_source_borrow_from_object(
        dependency->provider_instance, dependency->provider_interfaces, &transport);
    if (status != CMETA_OK) return status;

    if (salts_component_dependency_find(dependencies, dependency_count,
            tr_raft_state_machine_source_interface(), &dependency) != SALTS_COMPONENT_OK)
        return CMETA_CALLBACK_ERROR;
    status = tr_raft_state_machine_source_borrow_from_object(
        dependency->provider_instance, dependency->provider_interfaces, &machine);
    if (status != CMETA_OK) return status;

    config.core = domain->config.core;
    if (tr_raft_storage_source_snapshot(&storage, &config.storage) != SALTS_OK ||
        tr_raft_transport_source_snapshot(&transport, &config.transport) != SALTS_OK ||
        tr_raft_state_machine_source_snapshot(&machine, &config.state_machine) != SALTS_OK)
        return CMETA_CALLBACK_ERROR;

    result = tr_raft_runtime_create(&config, &domain->runtime);
    if (result != SALTS_OK) return CMETA_CALLBACK_ERROR;

    status = cmeta_object_borrow(
        out_instance, &domain->identities[TR_COMPONENT_RUNTIME],
        &cmeta_data_int, NULL);
    if (status == CMETA_OK)
        status = cmeta_object_take(out_instance, &domain->runtime_lifecycle);
    if (status != CMETA_OK) {
        cmeta_object_release(out_instance);
        (void)tr_raft_runtime_destroy(domain->runtime);
        domain->runtime = NULL;
    }
    return status;
}

static void tr_component_binding(
    salts_component_provider_binding *binding,
    const cmeta_component_desc *descriptor,
    void *provider_context,
    const cmeta_object_interface_provider *interfaces,
    salts_component_create_fn create)
{
    *binding = (salts_component_provider_binding){
        sizeof(*binding),
        SALTS_COMPONENT_PROVIDER_BINDING_ABI_VERSION,
        descriptor,
        provider_context,
        interfaces,
        create,
        NULL,
        NULL
    };
}

static bool tr_component_valid_config(const tr_raft_runtime_config_t *config)
{
    return config != NULL && config->core != NULL &&
           config->storage.begin != NULL &&
           config->storage.write_hard_state != NULL &&
           config->storage.truncate_log != NULL &&
           config->storage.append_log != NULL &&
           config->storage.write_commit_index != NULL &&
           config->storage.commit != NULL &&
           config->storage.rollback != NULL &&
           config->transport.enqueue != NULL &&
           config->state_machine.apply_batch != NULL;
}

int tr_raft_component_domain_create(
    const tr_raft_runtime_config_t *config,
    tr_raft_component_domain_t **out_domain)
{
    tr_raft_component_domain_t *domain;
    salts_component_status status;
    size_t i;

    if (out_domain == NULL) return SALTS_EINVAL;
    *out_domain = NULL;
    if (!tr_component_valid_config(config)) return SALTS_EINVAL;

    domain = (tr_raft_component_domain_t *)calloc(1u, sizeof(*domain));
    if (domain == NULL) return SALTS_ENOMEM;

    domain->config = *config;
    domain->components = (salts_component_context)SALTS_COMPONENT_CONTEXT_INIT;
    for (i = 0u; i < TR_COMPONENT_COUNT; ++i)
        domain->identities[i] = (int)i;

    domain->runtime_lifecycle = (cmeta_object_lifecycle){
        sizeof(cmeta_object_lifecycle), domain, NULL, NULL,
        tr_component_runtime_destroy
    };
    domain->interface_provider = (cmeta_object_interface_provider){
        sizeof(cmeta_object_interface_provider), domain, tr_component_project
    };

    tr_component_binding(&domain->bindings[TR_COMPONENT_STORAGE],
        cmeta_component_meta(tr_raft_StorageAdapter),
        &domain->identities[TR_COMPONENT_STORAGE],
        &domain->interface_provider, tr_adapter_create);
    tr_component_binding(&domain->bindings[TR_COMPONENT_TRANSPORT],
        cmeta_component_meta(tr_raft_TransportAdapter),
        &domain->identities[TR_COMPONENT_TRANSPORT],
        &domain->interface_provider, tr_adapter_create);
    tr_component_binding(&domain->bindings[TR_COMPONENT_STATE_MACHINE],
        cmeta_component_meta(tr_raft_StateMachineAdapter),
        &domain->identities[TR_COMPONENT_STATE_MACHINE],
        &domain->interface_provider, tr_adapter_create);
    tr_component_binding(&domain->bindings[TR_COMPONENT_RUNTIME],
        cmeta_component_meta(tr_raft_RuntimeComponent), domain,
        NULL, tr_runtime_component_create);
    for (i = 0u; i < TR_COMPONENT_COUNT; ++i)
        domain->deployments[i].provider = &domain->bindings[i];

    status = salts_component_context_init(
        &domain->components,
        domain->deployments, TR_COMPONENT_COUNT,
        NULL, 0u,
        domain->instances, TR_COMPONENT_COUNT,
        domain->dependencies, TR_COMPONENT_DEPENDENCY_COUNT,
        domain->activation_order, TR_COMPONENT_COUNT);
    if (status == SALTS_COMPONENT_OK)
        status = salts_component_context_resolve(&domain->components);
    if (status == SALTS_COMPONENT_OK)
        status = salts_component_context_start(&domain->components);

    if (status != SALTS_COMPONENT_OK) {
        /* Component already releases any partially returned owned ObjectRef. */
        free(domain);
        return status == SALTS_COMPONENT_CAPACITY_EXCEEDED
                   ? SALTS_ENOBUFS : SALTS_EPROTO;
    }
    *out_domain = domain;
    return SALTS_OK;
}

tr_raft_runtime_t *tr_raft_component_domain_runtime(
    tr_raft_component_domain_t *domain)
{
    if (domain == NULL ||
        domain->components.state != SALTS_COMPONENT_CONTEXT_ACTIVE)
        return NULL;
    return domain->runtime;
}

int tr_raft_component_domain_stop(tr_raft_component_domain_t *domain)
{
    if (domain == NULL) return SALTS_EINVAL;
    if (salts_component_context_stop(&domain->components) !=
        SALTS_COMPONENT_OK)
        return SALTS_EBUSY;
    return domain->runtime == NULL ? SALTS_OK : SALTS_EPROTO;
}

int tr_raft_component_domain_destroy(tr_raft_component_domain_t *domain)
{
    if (domain == NULL) return SALTS_OK;
    if (domain->components.state != SALTS_COMPONENT_CONTEXT_STOPPED)
        return SALTS_EBUSY;
    free(domain);
    return SALTS_OK;
}
