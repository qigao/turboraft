#if defined(WIN32) || defined(WIN64) || defined(_WIN32)
#include <io.h>

#define close _close
#define read _read
#define unlink _unlink
#define write _write
#endif
