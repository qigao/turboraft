#ifndef TURBORAFT_STL_STATUS_H
#define TURBORAFT_STL_STATUS_H

#include <turbostl/status.h>
#include <turbo_error.h>

static inline int tr_raft_stl_status_to_error(stl_status status)
{
    switch (status) {
    case STL_OK:
        return TURBO_OK;
    case STL_OUT_OF_MEMORY:
        return TURBO_ENOMEM;
    case STL_CAPACITY_EXCEEDED:
        return TURBO_ENOSPC;
    case STL_EMPTY:
    case STL_NOT_FOUND:
        return TURBO_ENOENT;
    case STL_INVALID_ARGUMENT:
    case STL_TYPE_MISMATCH:
    case STL_TRAIT_MISSING:
    default:
        return TURBO_EINVAL;
    }
}

#endif
