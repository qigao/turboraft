#define _GNU_SOURCE
#include <errno.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <sys/syscall.h>
#include <unistd.h>

static _Atomic int turboraft_test_fsync_calls = 0;

int fsync(int fd)
{
    const int call =
        atomic_fetch_add(&turboraft_test_fsync_calls, 1) + 1;
    const char *requested =
        getenv("TURBORAFT_FS_TEST_FAIL_FSYNC_CALL");
    const int fail_call = requested != NULL ? atoi(requested) : 0;

    if (call == fail_call) {
        errno = EIO;
        return -1;
    }
    return (int)syscall(SYS_fsync, fd);
}
