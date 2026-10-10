#include <turboraft/raft_multicore_chunk_receivers.h>

#include <cmeta_error.h>

#include <string.h>

int tr_raft_multicore_chunk_receivers_handle(
    const tr_raft_multicore_chunk_receivers_t *receivers,
    const tr_raft_transport_payload_t *payload,
    tr_raft_multicore_chunk_result_t *out)
{
    int result;

    if (out == NULL) return SALTS_EINVAL;
    memset(out, 0, sizeof(*out));
    if (receivers == NULL || payload == NULL)
        return SALTS_EINVAL;

    if (payload->kind == TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_CHUNK) {
        tr_raft_snapshot_receive_result_t received = {0};
        if (receivers->snapshot == NULL) return SALTS_ENOTSUP;
        out->kind = payload->kind;
        result = tr_raft_snapshot_receiver_handle(
            receivers->snapshot, &payload->data.snapshot_chunk, &received);
        if (result != SALTS_OK) return result;
        /* ACCEPTED means this offset has actually been validated/consumed,
         * not that Snapshot has been installed or fsync has occurred. */
        out->ack_valid = received.ack.accepted;
        out->durable_or_installed = received.installed;
        out->ack.snapshot = received.ack;
        return SALTS_OK;
    }

    if (payload->kind == TR_RAFT_WIRE_PAYLOAD_DATA_CHUNK) {
        tr_raft_data_stream_receive_result_t received = {0};
        if (receivers->data == NULL) return SALTS_ENOTSUP;
        out->kind = payload->kind;
        result = tr_raft_data_stream_receiver_handle(
            receivers->data, &payload->data.data_chunk, &received);
        if (result != SALTS_OK) return result;
        /* Only the existing receiver/sink commit may assert durability.
         * A duplicate committed chunk can report durable=true even though
         * this invocation does not call commit a second time. */
        out->ack_valid = received.ack.accepted;
        out->durable_or_installed = received.ack.durable;
        out->ack.data = received.ack;
        return SALTS_OK;
    }

    return SALTS_ENOTSUP;
}
