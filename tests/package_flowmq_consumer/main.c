#include <turboraft/raft_flowmq_peer_service.h>

#include <stddef.h>
#include <stdint.h>

_Static_assert(TR_RAFT_FLOWMQ_MAX_CERTIFICATES_PER_PEER == 4U,
               "peer certificate policy bound changed");

int main(void)
{
    tr_raft_flowmq_peer_config_t peer = {0};
    tr_raft_flowmq_peer_service_status_t status = {0};
    int (*create_fn)(
        const tr_raft_flowmq_peer_service_config_t *,
        tr_raft_flowmq_peer_service_t **) =
        tr_raft_flowmq_peer_service_create;
    int (*status_fn)(
        const tr_raft_flowmq_peer_service_t *,
        tr_raft_flowmq_peer_service_status_t *) =
        tr_raft_flowmq_peer_service_get_status;

    peer.client_certificate_sha256 = NULL;
    peer.client_certificate_sha256_count = 0U;
    status.tls_identity_rejections = 0U;
    return create_fn == NULL || status_fn == NULL ? 1 : 0;
}
