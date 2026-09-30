#define _GNU_SOURCE
#include <errno.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/syscall.h>
#include <unistd.h>

static _Atomic int turboraft_snapshot_publish_armed = 0;
static _Atomic int turboraft_fault_fired = 0;

static int fd_path_contains(int fd, const char *needle)
{
    char link_path[64];
    char target[512];
    ssize_t length;

    (void)snprintf(link_path, sizeof(link_path), "/proc/self/fd/%d", fd);
    length = readlink(link_path, target, sizeof(target) - 1U);
    if (length <= 0) {
        return 0;
    }
    target[length] = '\0';
    return strstr(target, needle) != NULL;
}

int fsync(int fd)
{
    const char *phase = getenv("TURBORAFT_FS_TEST_FAIL_PHASE");
    const int is_snapshot_stage =
        fd_path_contains(fd, ".snapshot.1.1.tmp");

    if (phase != NULL && strcmp(phase, "pre_publish") == 0 &&
        is_snapshot_stage &&
        atomic_exchange(&turboraft_fault_fired, 1) == 0) {
        errno = EIO;
        return -1;
    }

    if (phase != NULL && strcmp(phase, "post_publish") == 0) {
        if (atomic_load(&turboraft_snapshot_publish_armed) != 0 &&
            atomic_exchange(&turboraft_fault_fired, 1) == 0) {
            errno = EIO;
            return -1;
        }
        if (is_snapshot_stage) {
            atomic_store(&turboraft_snapshot_publish_armed, 1);
        }
    }

    return (int)syscall(SYS_fsync, fd);
}
