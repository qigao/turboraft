# Build diagnostics

TurboRaft keeps diagnostic builds in separate build trees. Sanitizer findings
and MSVC analysis warnings are build failures; no diagnostic preset silently
falls back to an ordinary build.

| Preset | Platform | Diagnostic |
|---|---|---|
| `win-dev-user` | Windows/MSVC | AddressSanitizer |
| `win-analyze-user` | Windows/MSVC | `/W4 /WX /analyze` for project sources |
| `linux-dev-user` | Linux/Clang or GCC | AddressSanitizer |
| `linux-ubsan-user` | Linux/Clang or GCC | UndefinedBehaviorSanitizer |
| `linux-tsan-user` | Linux/Clang or GCC | ThreadSanitizer |

Configure, build, and test one diagnostic tree with the same preset name:

```powershell
cmake --preset win-analyze-user --fresh
cmake --build --preset win-analyze-user
ctest --preset win-analyze-user --output-on-failure
```

```sh
cmake --preset linux-tsan-user --fresh
cmake --build --preset linux-tsan-user
ctest --preset linux-tsan-user --output-on-failure
```

TSan is intentionally incompatible with ASan and UBSan in one binary. UBSan
and TSan are unavailable with MSVC. Unsupported or conflicting combinations
fail during configure instead of producing an uninstrumented binary.

The multiprocess chaos test uses subprocesses and may need a larger CI timeout
under sanitizers. A sanitizer job must still run the complete CTest suite; a
targeted test run is only a local diagnosis aid.

MSVC system and installed dependency headers are marked external. Their SDK
annotation warnings are not promoted by `/WX`; TurboRaft source warnings remain
fatal.

Test-only targets suppress C6262 because deterministic fixed-capacity fixtures
intentionally use up to 278 KiB of the test process stack. Library and service
targets do not suppress this warning; a production stack allocation above the
MSVC analyzer threshold still fails the build.
