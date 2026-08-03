#ifndef TURBORAFT_TEST_RAFT_CORONET_MTLS_SUPPORT_H
#define TURBORAFT_TEST_RAFT_CORONET_MTLS_SUPPORT_H

#include <turbo_error.h>

#include <string.h>

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#define TR_TEST_INVALID_SOCKET INVALID_SOCKET
#define tr_test_close_socket closesocket
typedef SOCKET tr_test_socket_t;
#else
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#define TR_TEST_INVALID_SOCKET (-1)
#define tr_test_close_socket close
typedef int tr_test_socket_t;
#endif

static int tr_test_reserve_loopback_port(unsigned short *out_port)
{
    struct sockaddr_in address;
#ifdef _WIN32
    int address_size = (int) sizeof(address);
    static int socket_runtime_ready = 0;
#else
    socklen_t address_size = (socklen_t) sizeof(address);
#endif
    tr_test_socket_t socket_handle;
    int reuse_address = 1;
    int result = TURBO_EIO;

    if (out_port == NULL) {
        return TURBO_EINVAL;
    }
#ifdef _WIN32
    if (!socket_runtime_ready) {
        WSADATA socket_runtime;
        if (WSAStartup(MAKEWORD(2, 2), &socket_runtime) != 0) {
            return TURBO_EIO;
        }
        socket_runtime_ready = 1;
    }
#endif
    socket_handle = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (socket_handle == TR_TEST_INVALID_SOCKET) {
        return TURBO_EIO;
    }
    setsockopt(socket_handle, SOL_SOCKET, SO_REUSEADDR,
               (const char *) &reuse_address, sizeof(reuse_address));
    memset(&address, 0, sizeof(address));
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = htons(0U);
    if (bind(socket_handle, (const struct sockaddr *) &address,
             sizeof(address)) == 0 &&
        listen(socket_handle, 1) == 0 &&
        getsockname(socket_handle, (struct sockaddr *) &address,
                    &address_size) == 0) {
        *out_port = ntohs(address.sin_port);
        result = *out_port == 0U ? TURBO_EPROTO : TURBO_OK;
    }
    tr_test_close_socket(socket_handle);
    return result;
}

#endif
