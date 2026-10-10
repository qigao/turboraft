#ifndef TURBORAFT_ACE23_DSO_EPOCH_FIXTURE_H
#define TURBORAFT_ACE23_DSO_EPOCH_FIXTURE_H

#include <stdatomic.h>
#include <stdbool.h>
#include <salts/component_plugin_abi.h>

/* Test-only schema: the host may observe these atomics ONLY while Salts
 * Plugin holds a live module lease. Never copy it into a production API. */
typedef struct tr_ace23_dso_callback_state {
    atomic_bool stop_requested;
    atomic_uint callback_entered;
    atomic_uint callback_completed;
} tr_ace23_dso_callback_state;

/* This exact Interface is provided by the Plugin-backed Component. The
 * authenticated CNet Owner binds it from an original live ComponentPlugin
 * Scope, then calls the DSO function through the borrowed typed vtable.
 * The Scope is the authority holding the dynamic module lease. */
#define TR_ACE23_DSO_CALLBACK_METHODS(X, I) \
    X(I, R0, int, invoke, _)

CMETA_INTERFACE(tr_ace23_dso_callback, TR_ACE23_DSO_CALLBACK_METHODS);
CMETA_OBJECT_INTERFACE_ADAPTER(tr_ace23_dso_callback);

#endif
