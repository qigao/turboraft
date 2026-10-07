#ifndef TURBORAFT_MULTICORE_METRICS_H
#define TURBORAFT_MULTICORE_METRICS_H

#include <stdint.h>
/* Benchmark-only process accounting adapter. Salts exposes system information
 * but no public per-process CPU/peak RSS query. Values are process-wide. */
#if defined(_WIN32)
#include <windows.h>
#include <psapi.h>
static int multicore_process_metrics(uint64_t *cpu_ns, uint64_t *peak_bytes)
{
    FILETIME created, exited, kernel, user;
    ULARGE_INTEGER k, u;
    PROCESS_MEMORY_COUNTERS memory = {0};
    memory.cb = sizeof(memory);
    if (!GetProcessTimes(GetCurrentProcess(), &created, &exited, &kernel, &user) ||
        !GetProcessMemoryInfo(GetCurrentProcess(), &memory, sizeof(memory))) return -1;
    k.LowPart = kernel.dwLowDateTime; k.HighPart = kernel.dwHighDateTime;
    u.LowPart = user.dwLowDateTime; u.HighPart = user.dwHighDateTime;
    *cpu_ns = (k.QuadPart + u.QuadPart) * UINT64_C(100);
    *peak_bytes = (uint64_t)memory.PeakWorkingSetSize;
    return 0;
}
#else
#include <sys/resource.h>
static int multicore_process_metrics(uint64_t *cpu_ns, uint64_t *peak_bytes)
{
    struct rusage usage;
    if (getrusage(RUSAGE_SELF, &usage) != 0) return -1;
    *cpu_ns = ((uint64_t)usage.ru_utime.tv_sec + (uint64_t)usage.ru_stime.tv_sec) * UINT64_C(1000000000) +
        ((uint64_t)usage.ru_utime.tv_usec + (uint64_t)usage.ru_stime.tv_usec) * UINT64_C(1000);
#if defined(__APPLE__)
    *peak_bytes = (uint64_t)usage.ru_maxrss;
#else
    *peak_bytes = (uint64_t)usage.ru_maxrss * 1024U;
#endif
    return 0;
}
#endif
#endif
