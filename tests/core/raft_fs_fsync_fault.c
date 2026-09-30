#define _GNU_SOURCE
#include <errno.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>
#include <sys/syscall.h>
#include <unistd.h>

static _Atomic int turboraft_armed_fsync_calls = 0;
static _Atomic int turboraft_fault_fired = 0;

int fsync(int fd)
{
    const char *armed = getenv("TURBORAFT_FS_TEST_ARMED");
    const char *phase = getenv("TURBORAFT_FS_TEST_FAIL_PHASE");
    int call = 0;
    int fail_call = 0;

    if (armed != NULL && strcmp(armed, "1") == 0) {
        call = atomic_fetch_add(&turboraft_armed_fsync_calls, 1) + 1;
        if (phase != NULL && strcmp(phase, "pre_publish") == 0) {
            fail_call = 1;
        } else if (phase != NULL && strcmp(phase, "post_publish") == 0) {
            fail_call = 2;
        }
        if (call == fail_call &&
            atomic_exchange(&turboraft_fault_fired, 1) == 0) {
            errno = EIO;
            return -1;
        }
    }

    return (int)syscall(SYS_fsync, fd);
}
