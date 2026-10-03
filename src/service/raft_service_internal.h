#ifndef TURBORAFT_RAFT_SERVICE_INTERNAL_H
#define TURBORAFT_RAFT_SERVICE_INTERNAL_H

#include <turboraft/raft_service.h>

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef void *(*tr_raft_service_stage_alloc_fn)(
    void *context, size_t count, size_t size);
typedef void (*tr_raft_service_stage_free_fn)(
    void *context, void *memory);

/*
 * Test-only staged-message allocator seam.
 * This header is private and is never installed.
 *
 * Passing NULL/NULL restores the production calloc/free provider.
 * The provider may only be changed while no peer owns staged transport work.
 */
int tr_raft_service_set_stage_allocator_for_test(
    tr_raft_service_t *service,
    tr_raft_service_stage_alloc_fn allocate,
    tr_raft_service_stage_free_fn deallocate,
    void *context);

#ifdef __cplusplus
}
#endif

#endif
