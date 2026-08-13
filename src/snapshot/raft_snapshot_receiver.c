#include <turboraft/raft_snapshot_receiver.h>

#include <openssl/sha.h>
#include <turbo_error.h>

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

struct tr_raft_snapshot_receiver {
    tr_raft_node_id_t self_id;
    size_t max_snapshot_bytes;
    tr_raft_snapshot_install_fn install;
    void *install_context;
    tr_raft_snapshot_stream_sink_t stream;
    tr_raft_node_id_t leader_id;
    tr_raft_term_t leader_term;
    tr_raft_index_t snapshot_index;
    tr_raft_term_t snapshot_term;
    tr_raft_conf_t configuration;
    uint64_t snapshot_size;
    uint64_t next_offset;
    uint8_t digest[TR_RAFT_WIRE_SNAPSHOT_DIGEST_SIZE];
    uint8_t *data;
    SHA256_CTX sha256;
    bool stream_started;
    bool active;
    bool completed;
};

static void tr_snapshot_receiver_clear_transfer(
    tr_raft_snapshot_receiver_t *receiver)
{
    if (receiver->stream_started) {
        receiver->stream.abort(receiver->stream.context);
        receiver->stream_started = false;
    }
    free(receiver->data);
    receiver->data = NULL;
    receiver->leader_id = 0U;
    receiver->leader_term = 0U;
    receiver->snapshot_index = 0U;
    receiver->snapshot_term = 0U;
    memset(&receiver->configuration, 0, sizeof(receiver->configuration));
    receiver->snapshot_size = 0U;
    receiver->next_offset = 0U;
    memset(receiver->digest, 0, sizeof(receiver->digest));
    receiver->active = false;
    receiver->completed = false;
}

static bool tr_snapshot_receiver_chunk_valid(
    const tr_raft_snapshot_receiver_t *receiver,
    const tr_raft_snapshot_chunk_t *chunk)
{
    if (chunk == NULL || chunk->from == 0U || chunk->to != receiver->self_id ||
        chunk->term == 0U || chunk->snapshot_index == 0U ||
        chunk->snapshot_term == 0U || chunk->snapshot_term > chunk->term ||
        chunk->snapshot_size > receiver->max_snapshot_bytes ||
        chunk->snapshot_offset > chunk->snapshot_size ||
        chunk->data_length > TR_RAFT_WIRE_MAX_SNAPSHOT_CHUNK_BYTES ||
        (chunk->data_length != 0U && chunk->data == NULL) ||
        chunk->data_length > chunk->snapshot_size - chunk->snapshot_offset ||
        (chunk->snapshot_offset == 0U &&
         (!chunk->has_configuration ||
          tr_raft_conf_validate(&chunk->configuration) != TURBO_OK)) ||
        (chunk->snapshot_offset != 0U && chunk->has_configuration)) {
        return false;
    }
    return chunk->done ==
               (chunk->snapshot_offset + chunk->data_length ==
                chunk->snapshot_size) &&
           (chunk->done || chunk->data_length != 0U);
}

static bool tr_snapshot_receiver_identity_matches(
    const tr_raft_snapshot_receiver_t *receiver,
    const tr_raft_snapshot_chunk_t *chunk)
{
    size_t index;

    if (chunk->snapshot_offset == 0U) {
        if (!chunk->has_configuration ||
            receiver->configuration.phase != chunk->configuration.phase ||
            receiver->configuration.transition_id !=
                chunk->configuration.transition_id ||
            receiver->configuration.member_count !=
                chunk->configuration.member_count) {
            return false;
        }
        for (index = 0U;
             index < receiver->configuration.member_count; ++index) {
            if (receiver->configuration.members[index].node_id !=
                    chunk->configuration.members[index].node_id ||
                receiver->configuration.members[index].roles !=
                    chunk->configuration.members[index].roles) {
                return false;
            }
        }
    }
    return receiver->leader_id == chunk->from &&
           receiver->leader_term == chunk->term &&
           receiver->snapshot_index == chunk->snapshot_index &&
           receiver->snapshot_term == chunk->snapshot_term &&
           receiver->snapshot_size == chunk->snapshot_size &&
           memcmp(receiver->digest, chunk->snapshot_digest,
                  sizeof(receiver->digest)) == 0;
}

static int tr_snapshot_receiver_start(
    tr_raft_snapshot_receiver_t *receiver,
    const tr_raft_snapshot_chunk_t *chunk)
{
    uint8_t *data = NULL;

    if (receiver->stream.begin == NULL && chunk->snapshot_size != 0U) {
        data = (uint8_t *) malloc((size_t) chunk->snapshot_size);
        if (data == NULL) {
            return TURBO_ENOMEM;
        }
    }
    tr_snapshot_receiver_clear_transfer(receiver);
    receiver->leader_id = chunk->from;
    receiver->leader_term = chunk->term;
    receiver->snapshot_index = chunk->snapshot_index;
    receiver->snapshot_term = chunk->snapshot_term;
    receiver->configuration = chunk->configuration;
    receiver->snapshot_size = chunk->snapshot_size;
    memcpy(receiver->digest, chunk->snapshot_digest,
           sizeof(receiver->digest));
    receiver->data = data;
    receiver->active = true;
    receiver->completed = false;
    if (SHA256_Init(&receiver->sha256) != 1) {
        tr_snapshot_receiver_clear_transfer(receiver);
        return TURBO_EPROTO;
    }
    if (receiver->stream.begin != NULL) {
        int result = receiver->stream.begin(
            receiver->stream.context, receiver->leader_term,
            receiver->snapshot_index, receiver->snapshot_term,
            &receiver->configuration, receiver->snapshot_size);

        if (result != TURBO_OK) {
            tr_snapshot_receiver_clear_transfer(receiver);
            return result;
        }
        receiver->stream_started = true;
    }
    return TURBO_OK;
}

static void tr_snapshot_receiver_prepare_ack(
    const tr_raft_snapshot_receiver_t *receiver,
    const tr_raft_snapshot_chunk_t *chunk,
    tr_raft_snapshot_receive_result_t *result)
{
    memset(result, 0, sizeof(*result));
    result->ack.from = receiver->self_id;
    result->ack.to = chunk->from;
    result->ack.term = chunk->term;
    result->ack.snapshot_index = chunk->snapshot_index;
    result->ack.snapshot_size = chunk->snapshot_size;
    memcpy(result->ack.snapshot_digest, chunk->snapshot_digest,
           sizeof(result->ack.snapshot_digest));
}

int tr_raft_snapshot_receiver_create(
    const tr_raft_snapshot_receiver_config_t *config,
    tr_raft_snapshot_receiver_t **out_receiver)
{
    tr_raft_snapshot_receiver_t *receiver;

    if (out_receiver == NULL) {
        return TURBO_EINVAL;
    }
    *out_receiver = NULL;
    if (config == NULL || config->self_id == 0U ||
        config->max_snapshot_bytes == 0U ||
        config->max_snapshot_bytes > TR_RAFT_WIRE_MAX_SNAPSHOT_BYTES) {
        return TURBO_EINVAL;
    }
    if ((config->install == NULL) == (config->stream.begin == NULL) ||
        (config->stream.begin != NULL &&
         (config->stream.write == NULL || config->stream.commit == NULL ||
          config->stream.abort == NULL))) {
        return TURBO_EINVAL;
    }
    receiver = (tr_raft_snapshot_receiver_t *) calloc(1U, sizeof(*receiver));
    if (receiver == NULL) {
        return TURBO_ENOMEM;
    }
    receiver->self_id = config->self_id;
    receiver->max_snapshot_bytes = config->max_snapshot_bytes;
    receiver->install = config->install;
    receiver->install_context = config->install_context;
    receiver->stream = config->stream;
    *out_receiver = receiver;
    return TURBO_OK;
}

void tr_raft_snapshot_receiver_destroy(
    tr_raft_snapshot_receiver_t *receiver)
{
    if (receiver == NULL) {
        return;
    }
    tr_snapshot_receiver_clear_transfer(receiver);
    free(receiver);
}

void tr_raft_snapshot_receiver_reset(
    tr_raft_snapshot_receiver_t *receiver)
{
    if (receiver != NULL) {
        tr_snapshot_receiver_clear_transfer(receiver);
    }
}

int tr_raft_snapshot_receiver_handle(
    tr_raft_snapshot_receiver_t *receiver,
    const tr_raft_snapshot_chunk_t *chunk,
    tr_raft_snapshot_receive_result_t *out_result)
{
    uint8_t calculated_digest[SHA256_DIGEST_LENGTH];
    int result;

    if (receiver == NULL || chunk == NULL || out_result == NULL) {
        return TURBO_EINVAL;
    }
    tr_snapshot_receiver_prepare_ack(receiver, chunk, out_result);
    if (!tr_snapshot_receiver_chunk_valid(receiver, chunk)) {
        return TURBO_EPROTO;
    }
    if (!receiver->active) {
        if (chunk->snapshot_offset != 0U) {
            return TURBO_EPROTO;
        }
        result = tr_snapshot_receiver_start(receiver, chunk);
        if (result != TURBO_OK) {
            return result;
        }
    } else if (!tr_snapshot_receiver_identity_matches(receiver, chunk)) {
        if (chunk->snapshot_offset != 0U ||
            chunk->snapshot_index <= receiver->snapshot_index ||
            chunk->term < receiver->leader_term) {
            out_result->ack.next_offset = receiver->next_offset;
            return TURBO_EPROTO;
        }
        result = tr_snapshot_receiver_start(receiver, chunk);
        if (result != TURBO_OK) {
            return result;
        }
    }
    if (receiver->completed) {
        out_result->ack.next_offset = receiver->next_offset;
        out_result->ack.accepted = true;
        return TURBO_OK;
    }
    if (chunk->snapshot_offset < receiver->next_offset) {
        out_result->ack.next_offset = receiver->next_offset;
        out_result->ack.accepted = true;
        return TURBO_OK;
    }
    if (chunk->snapshot_offset != receiver->next_offset) {
        out_result->ack.next_offset = receiver->next_offset;
        return TURBO_EPROTO;
    }
    if (chunk->data_length != 0U) {
        if (SHA256_Update(&receiver->sha256, chunk->data,
                          chunk->data_length) != 1) {
            out_result->ack.accepted = false;
            tr_snapshot_receiver_clear_transfer(receiver);
            return TURBO_EPROTO;
        }
        if (receiver->stream_started) {
            result = receiver->stream.write(receiver->stream.context,
                                            receiver->next_offset,
                                            chunk->data,
                                            chunk->data_length);
            if (result != TURBO_OK) {
                out_result->ack.accepted = false;
                tr_snapshot_receiver_clear_transfer(receiver);
                return result;
            }
        } else {
            memcpy(receiver->data + receiver->next_offset, chunk->data,
                   chunk->data_length);
        }
    }
    receiver->next_offset += chunk->data_length;
    out_result->ack.next_offset = receiver->next_offset;
    out_result->ack.accepted = true;
    if (!chunk->done) {
        return TURBO_OK;
    }
    if (SHA256_Final(calculated_digest, &receiver->sha256) != 1 ||
        memcmp(calculated_digest, receiver->digest,
               sizeof(receiver->digest)) != 0) {
        out_result->ack.accepted = false;
        out_result->ack.next_offset = 0U;
        tr_snapshot_receiver_clear_transfer(receiver);
        return TURBO_EPROTO;
    }
    result = receiver->stream_started
                 ? receiver->stream.commit(receiver->stream.context)
                 : receiver->install(
                       receiver->install_context, receiver->leader_term,
                       receiver->snapshot_index, receiver->snapshot_term,
                       &receiver->configuration, receiver->data,
                       (size_t) receiver->snapshot_size);
    if (result != TURBO_OK) {
        out_result->ack.accepted = false;
        out_result->ack.next_offset = 0U;
        tr_snapshot_receiver_clear_transfer(receiver);
        return result;
    }
    out_result->installed = true;
    receiver->stream_started = false;
    free(receiver->data);
    receiver->data = NULL;
    receiver->completed = true;
    return TURBO_OK;
}
