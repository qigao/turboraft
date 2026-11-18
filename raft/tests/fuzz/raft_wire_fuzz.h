#ifndef TURBORAFT_TESTS_RAFT_WIRE_FUZZ_H
#define TURBORAFT_TESTS_RAFT_WIRE_FUZZ_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

int tr_raft_wire_fuzz_one_input(const uint8_t *data, size_t size);
int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size);

#ifdef __cplusplus
}
#endif

#endif
