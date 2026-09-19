#include "raft_group_queue.h"

#include <turboraft/raft_transport.h>

#include <salts_error.h>
#include <tinytest.h>

#include <string.h>

static void release_owned(tr_raft_owned_transport_payload_t *owned)
{
    if (owned != NULL) {
        memset(owned, 0, sizeof(*owned));
    }
}

static tr_raft_owned_transport_payload_t owned_raft(
    tr_raft_group_id_t group_id,
    uint64_t term)
{
    tr_raft_owned_transport_payload_t owned;

    memset(&owned, 0, sizeof(owned));
    owned.payload.group_id = group_id;
    owned.payload.kind = TR_RAFT_WIRE_PAYLOAD_RAFT;
    owned.payload.data.raft.type = TR_RAFT_MSG_HEARTBEAT_REQUEST;
    owned.payload.data.raft.from = 1U;
    owned.payload.data.raft.to = 2U;
    owned.payload.data.raft.term = term;
    return owned;
}

spec("raft bounded group queue")
{
    it("enforces per-group and global item capacity")
    {
        tr_raft_group_queue_t queue;
        tr_raft_group_queue_config_t config = {
            .max_groups = 2U,
            .total_item_capacity = 4U,
            .total_data_bytes = 16U,
            .per_group_item_capacity = 3U,
            .per_group_data_bytes = 8U,
            .release = release_owned,
        };
        tr_raft_owned_transport_payload_t item;

        memset(&queue, 0, sizeof(queue));
        check_equal(tr_raft_group_queue_init(&queue, &config), SALTS_OK);

        item = owned_raft(10U, 1U);
        check_equal(tr_raft_group_queue_enqueue(&queue, &item, 0U), SALTS_OK);
        item = owned_raft(10U, 2U);
        check_equal(tr_raft_group_queue_enqueue(&queue, &item, 0U), SALTS_OK);
        item = owned_raft(10U, 3U);
        check_equal(tr_raft_group_queue_enqueue(&queue, &item, 0U), SALTS_OK);
        item = owned_raft(10U, 4U);
        check_equal(tr_raft_group_queue_enqueue(&queue, &item, 0U),
                    SALTS_ENOSPC);

        item = owned_raft(20U, 5U);
        check_equal(tr_raft_group_queue_enqueue(&queue, &item, 0U), SALTS_OK);
        item = owned_raft(20U, 6U);
        check_equal(tr_raft_group_queue_enqueue(&queue, &item, 0U),
                    SALTS_ENOSPC);

        check_equal(tr_raft_group_queue_size(&queue), 4U);
        check_equal(tr_raft_group_queue_active_groups(&queue), 2U);
        check_equal(tr_raft_group_queue_clear(&queue), SALTS_OK);
        tr_raft_group_queue_destroy(&queue);
    }

    it("bounds active groups and reuses a drained slot")
    {
        tr_raft_group_queue_t queue;
        tr_raft_group_queue_config_t config = {
            .max_groups = 2U,
            .total_item_capacity = 4U,
            .total_data_bytes = 16U,
            .per_group_item_capacity = 2U,
            .per_group_data_bytes = 8U,
            .release = release_owned,
        };
        tr_raft_owned_transport_payload_t item;
        tr_raft_group_queue_token_t token;
        const tr_raft_owned_transport_payload_t *front = NULL;

        memset(&queue, 0, sizeof(queue));
        check_equal(tr_raft_group_queue_init(&queue, &config), SALTS_OK);

        item = owned_raft(10U, 1U);
        check_equal(tr_raft_group_queue_enqueue(&queue, &item, 0U), SALTS_OK);
        item = owned_raft(20U, 2U);
        check_equal(tr_raft_group_queue_enqueue(&queue, &item, 0U), SALTS_OK);
        item = owned_raft(30U, 3U);
        check_equal(tr_raft_group_queue_enqueue(&queue, &item, 0U),
                    SALTS_ENOSPC);

        check_equal(tr_raft_group_queue_peek_next(&queue, &token, &front),
                    SALTS_OK);
        check_equal(front->payload.group_id, 10U);
        check_equal(tr_raft_group_queue_pop(&queue, token, &item, NULL),
                    SALTS_OK);

        item = owned_raft(30U, 3U);
        check_equal(tr_raft_group_queue_enqueue(&queue, &item, 0U), SALTS_OK);
        check_equal(tr_raft_group_queue_active_groups(&queue), 2U);

        check_equal(tr_raft_group_queue_clear(&queue), SALTS_OK);
        tr_raft_group_queue_destroy(&queue);
    }

    it("schedules active groups in deterministic round robin order")
    {
        tr_raft_group_queue_t queue;
        tr_raft_group_queue_config_t config = {
            .max_groups = 3U,
            .total_item_capacity = 8U,
            .total_data_bytes = 32U,
            .per_group_item_capacity = 4U,
            .per_group_data_bytes = 16U,
            .release = release_owned,
        };
        tr_raft_owned_transport_payload_t item;
        tr_raft_group_queue_token_t token;
        const tr_raft_owned_transport_payload_t *front = NULL;
        const tr_raft_group_id_t expected_groups[] = {
            10U, 20U, 10U, 20U, 10U
        };
        const uint64_t expected_terms[] = {1U, 4U, 2U, 5U, 3U};
        size_t index;

        memset(&queue, 0, sizeof(queue));
        check_equal(tr_raft_group_queue_init(&queue, &config), SALTS_OK);

        item = owned_raft(10U, 1U);
        check_equal(tr_raft_group_queue_enqueue(&queue, &item, 0U), SALTS_OK);
        item = owned_raft(10U, 2U);
        check_equal(tr_raft_group_queue_enqueue(&queue, &item, 0U), SALTS_OK);
        item = owned_raft(10U, 3U);
        check_equal(tr_raft_group_queue_enqueue(&queue, &item, 0U), SALTS_OK);
        item = owned_raft(20U, 4U);
        check_equal(tr_raft_group_queue_enqueue(&queue, &item, 0U), SALTS_OK);
        item = owned_raft(20U, 5U);
        check_equal(tr_raft_group_queue_enqueue(&queue, &item, 0U), SALTS_OK);

        for (index = 0U;
             index < sizeof(expected_groups) / sizeof(expected_groups[0]);
             ++index) {
            check_equal(tr_raft_group_queue_peek_next(
                            &queue, &token, &front),
                        SALTS_OK);
            check_equal(front->payload.group_id, expected_groups[index]);
            check_equal(front->payload.data.raft.term, expected_terms[index]);
            check_equal(tr_raft_group_queue_pop(
                            &queue, token, &item, NULL),
                        SALTS_OK);
        }

        check_equal(tr_raft_group_queue_size(&queue), 0U);
        check_equal(tr_raft_group_queue_active_groups(&queue), 0U);
        tr_raft_group_queue_destroy(&queue);
    }

    it("enforces global and per-group retained byte budgets")
    {
        tr_raft_group_queue_t queue;
        tr_raft_group_queue_config_t config = {
            .max_groups = 2U,
            .total_item_capacity = 8U,
            .total_data_bytes = 10U,
            .per_group_item_capacity = 4U,
            .per_group_data_bytes = 6U,
            .release = release_owned,
        };
        tr_raft_owned_transport_payload_t item;

        memset(&queue, 0, sizeof(queue));
        check_equal(tr_raft_group_queue_init(&queue, &config), SALTS_OK);

        item = owned_raft(10U, 1U);
        check_equal(tr_raft_group_queue_enqueue(&queue, &item, 4U), SALTS_OK);
        item = owned_raft(10U, 2U);
        check_equal(tr_raft_group_queue_enqueue(&queue, &item, 3U),
                    SALTS_ENOSPC);

        item = owned_raft(20U, 3U);
        check_equal(tr_raft_group_queue_enqueue(&queue, &item, 6U), SALTS_OK);
        item = owned_raft(20U, 4U);
        check_equal(tr_raft_group_queue_enqueue(&queue, &item, 1U),
                    SALTS_ENOSPC);

        check_equal(tr_raft_group_queue_data_bytes(&queue), 10U);
        check_equal(tr_raft_group_queue_clear(&queue), SALTS_OK);
        tr_raft_group_queue_destroy(&queue);
    }
}
