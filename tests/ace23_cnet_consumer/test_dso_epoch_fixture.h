#ifndef TURBORAFT_ACE23_DSO_EPOCH_FIXTURE_H
#define TURBORAFT_ACE23_DSO_EPOCH_FIXTURE_H

#include <stdatomic.h>
#include <stdbool.h>

/* Test-only schema: the host may observe these atomics ONLY while Salts
 * Plugin holds a live module lease. Never copy it into a production API. */
typedef struct tr_ace23_dso_callback_state {
    atomic_bool stop_requested;
    atomic_bool callback_entered;
    atomic_uint callback_completed;
} tr_ace23_dso_callback_state;

#endif
