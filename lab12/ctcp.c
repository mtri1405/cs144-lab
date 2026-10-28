/******************************************************************************
 * ctcp.c
 * ------
 * Implementation of cTCP done here. This is the only file you need to change.
 * Look at the following files for references and useful functions:
 *   - ctcp.h: Headers for this file.
 *   - ctcp_linked_list.h: Linked list functions for managing a linked list.
 *   - ctcp_sys.h: Connection-related structs and functions, cTCP segment
 *                 definition.
 *   - ctcp_utils.h: Checksum computation, getting the current time.
 *
 *****************************************************************************/

/******************************************************************************
 * ctcp.c
 * Reliable cTCP implementation.
 *****************************************************************************/

#include "ctcp.h"
#include "ctcp_linked_list.h"
#include "ctcp_sys.h"
#include "ctcp_utils.h"

// #define DEBUG
#define CTCP_MAX_RETRIES 5

typedef struct sent_segment {
    ctcp_segment_t *seg;
    size_t len;
    long last_sent;       /* milliseconds */
    int retries;
    bool retry_exhausted; /* Keep state alive; do not destroy on timeout alone. */
} sent_segment_t;

struct ctcp_state {
    struct ctcp_state *next;
    struct ctcp_state **prev;

    conn_t *conn;
    linked_list_t *segments;

    ctcp_config_t *ctcp_cfg;
    uint32_t next_seqno;

    bool fin_sent;
    bool fin_received;
    bool fin_ack_received;

    linked_list_t *unoutput_segments;
    size_t unoutput_bytes;
    linked_list_t *sent_unacked;
    uint32_t next_expected_seqno;
};

static ctcp_state_t *state_list;

static ctcp_segment_t *ctcp_segment_create(
    ctcp_state_t *state, uint32_t flags, uint32_t ackno,
    void *data, size_t data_len);

static void ctcp_remove_sent_node(ctcp_state_t *state, ll_node_t *node)
{
    sent_segment_t *ss;

    if (state == NULL || node == NULL) {
        return;
    }

    ss = (sent_segment_t *)node->object;
    if (ss != NULL) {
        free(ss->seg);
        free(ss);
    }
    ll_remove(state->sent_unacked, node);
}

/*
 * A connection is destroyed only after:
 *   1. We sent FIN.
 *   2. Our FIN was acknowledged.
 *   3. We received the peer's FIN.
 *   4. No locally-sent segments remain unacknowledged.
 *
 * Returns true if state was destroyed. Callers must not use state afterwards.
 */
static bool ctcp_maybe_destroy(ctcp_state_t *state)
{
    if (state == NULL) {
        return false;
    }

    if (state->fin_sent &&
        state->fin_ack_received &&
        state->fin_received &&
        ll_length(state->sent_unacked) == 0) {
        ctcp_destroy(state);
        return true;
    }

    return false;
}

ctcp_state_t *ctcp_init(conn_t *conn, ctcp_config_t *cfg)
{
    ctcp_state_t *state;

    if (conn == NULL || cfg == NULL) {
        return NULL;
    }

    state = calloc(1, sizeof(*state));
    if (state == NULL) {
        return NULL;
    }

    state->segments = ll_create();
    state->unoutput_segments = ll_create();
    state->sent_unacked = ll_create();

    if (state->segments == NULL ||
        state->unoutput_segments == NULL ||
        state->sent_unacked == NULL) {
        if (state->segments != NULL) {
            ll_destroy(state->segments);
        }
        if (state->unoutput_segments != NULL) {
            ll_destroy(state->unoutput_segments);
        }
        if (state->sent_unacked != NULL) {
            ll_destroy(state->sent_unacked);
        }
        free(state);
        return NULL;
    }

    state->conn = conn;
    state->ctcp_cfg = cfg;
    state->next_seqno = 1;
    state->next_expected_seqno = 1;
    state->fin_sent = false;
    state->fin_received = false;
    state->fin_ack_received = false;
    state->unoutput_bytes = 0;

    state->next = state_list;
    state->prev = &state_list;
    if (state_list != NULL) {
        state_list->prev = &state->next;
    }
    state_list = state;

    return state;
}

void ctcp_destroy(ctcp_state_t *state)
{
    ll_node_t *node;

    if (state == NULL) {
        return;
    }

    /* Unlink first so the global list never points at freed state. */
    if (state->next != NULL) {
        state->next->prev = state->prev;
    }
    if (state->prev != NULL) {
        *state->prev = state->next;
    }

    conn_remove(state->conn);

    node = ll_front(state->segments);
    while (node != NULL) {
        ll_node_t *next = node->next;
        free(node->object);
        ll_remove(state->segments, node);
        node = next;
    }
    ll_destroy(state->segments);

    node = ll_front(state->unoutput_segments);
    while (node != NULL) {
        ll_node_t *next = node->next;
        free(node->object);
        ll_remove(state->unoutput_segments, node);
        node = next;
    }
    ll_destroy(state->unoutput_segments);

    node = ll_front(state->sent_unacked);
    while (node != NULL) {
        ll_node_t *next = node->next;
        sent_segment_t *ss = (sent_segment_t *)node->object;
        if (ss != NULL) {
            free(ss->seg);
            free(ss);
        }
        ll_remove(state->sent_unacked, node);
        node = next;
    }
    ll_destroy(state->sent_unacked);

    /*
     * This preserves the ownership convention in the original implementation:
     * ctcp_init() receives a cfg that this state owns. If cfg is shared or
     * managed by the caller, remove this free and manage cfg at its owner.
     */
    free(state->ctcp_cfg);
    free(state);

    /* Do not end the whole client while another cTCP state is still active. */
    if (state_list == NULL) {
        end_client();
    }
}

void ctcp_read(ctcp_state_t *state)
{
    uint8_t buf[MAX_SEG_DATA_SIZE];
    int bytes_read;

    if (state == NULL || state->fin_sent) {
        return;
    }

    /* Stop-and-wait: wait until all sent data/FIN has been acknowledged. */
    if (ll_length(state->sent_unacked) > 0) {
        return;
    }

    bytes_read = conn_input(state->conn, buf, sizeof(buf));

    if (bytes_read == 0) {
        return;
    }

    if (bytes_read > 0) {
        size_t payload_len = (size_t)bytes_read;
        size_t segment_len = sizeof(ctcp_segment_t) + payload_len;
        ctcp_segment_t *segment =
            ctcp_segment_create(state, 0, 0, buf, payload_len);
        sent_segment_t *ss;

        if (segment == NULL) {
            return;
        }

        ss = calloc(1, sizeof(*ss));
        if (ss == NULL) {
            free(segment);
            return;
        }

        if (conn_send(state->conn, segment, segment_len) == -1) {
            free(segment);
            free(ss);
            return;
        }

        ss->seg = segment;
        ss->len = segment_len;
        ss->last_sent = current_time();
        ss->retries = 0;
        ss->retry_exhausted = false;
        ll_add(state->sent_unacked, ss);

        state->next_seqno += (uint32_t)payload_len;
        return;
    }

    if (bytes_read == -1) {
        size_t segment_len = sizeof(ctcp_segment_t);
        ctcp_segment_t *segment =
            ctcp_segment_create(state, TH_FIN, 0, NULL, 0);
        sent_segment_t *ss;

        if (segment == NULL) {
            return;
        }

        ss = calloc(1, sizeof(*ss));
        if (ss == NULL) {
            free(segment);
            return;
        }

        if (conn_send(state->conn, segment, segment_len) == -1) {
            free(segment);
            free(ss);
            return;
        }

        ss->seg = segment;
        ss->len = segment_len;
        ss->last_sent = current_time();
        ss->retries = 0;
        ss->retry_exhausted = false;
        ll_add(state->sent_unacked, ss);

        /* FIN consumes one sequence number, even with no payload. */
        state->next_seqno += 1;
        state->fin_sent = true;
    }
}

void ctcp_receive(ctcp_state_t *state, ctcp_segment_t *segment, size_t len)
{
    uint16_t received_checksum;
    uint32_t flags;
    size_t declared_len;
    size_t data_len;
    uint32_t seqno;

    if (state == NULL || segment == NULL) {
        free(segment);
        return;
    }

    /* Validate the fixed header before accessing fields beyond it. */
    if (len < sizeof(ctcp_segment_t)) {
        free(segment);
        return;
    }

    declared_len = ntohs(segment->len);
    if (declared_len < sizeof(ctcp_segment_t) || declared_len != len) {
        free(segment);
        return;
    }

    received_checksum = segment->cksum;
    segment->cksum = 0;
    if (received_checksum != cksum(segment, len)) {
        free(segment);
        return;
    }
    segment->cksum = received_checksum;

    flags = segment->flags;
    data_len = len - sizeof(ctcp_segment_t);
    seqno = ntohl(segment->seqno);

    /*
     * Process ACK independently from FIN/data. A segment may legally carry
     * both ACK and FIN; do not return before processing its FIN.
     */
    if (flags & TH_ACK) {
        uint32_t ackno = ntohl(segment->ackno);
        ll_node_t *node = ll_front(state->sent_unacked);

#ifdef DEBUG
        fprintf(stderr, "[ACK RX] ackno=%u, outstanding=%u\n",
                ackno, ll_length(state->sent_unacked));
#endif

        while (node != NULL) {
            ll_node_t *next = node->next;
            sent_segment_t *ss = (sent_segment_t *)node->object;

            if (ss != NULL && ss->seg != NULL) {
                uint32_t sent_seqno = ntohl(ss->seg->seqno);
                uint32_t total_len = ntohs(ss->seg->len);

                if (total_len >= sizeof(ctcp_segment_t)) {
                    uint32_t payload_len =
                        total_len - (uint32_t)sizeof(ctcp_segment_t);
                    uint32_t seq_len = payload_len;
                    bool is_fin = (ss->seg->flags & TH_FIN) != 0;

                    if (is_fin) {
                        seq_len += 1;
                    }

                    /*
                     * Basic cumulative ACK comparison. Sequence-number
                     * wraparound is not handled by this simplified protocol.
                     */
                    if (seq_len > 0 && ackno >= sent_seqno + seq_len) {
                        if (is_fin) {
                            state->fin_ack_received = true;
#ifdef DEBUG
                            fprintf(stderr, "[ACK] FIN acknowledged\n");
#endif
                        }
                        ctcp_remove_sent_node(state, node);
                    }
                }
            }

            node = next;
        }

        /* A pure ACK has no receive-side data/FIN to process. */
        if (data_len == 0 && !(flags & TH_FIN)) {
            free(segment);
            (void)ctcp_maybe_destroy(state);
            return;
        }
    }

#ifdef DEBUG
    fprintf(stderr, "[RX] seq=%u flags=%u data_len=%zu\n",
            seqno, flags, data_len);
#endif

    /* Process in-order data and FIN. */
    if (data_len > 0 || (flags & TH_FIN)) {
        if (seqno == state->next_expected_seqno) {
            if (data_len > 0) {
                ctcp_segment_t *copy =
                    calloc(1, sizeof(ctcp_segment_t) + data_len);

                if (copy != NULL) {
                    memcpy(copy, segment, len);
                    ll_add(state->unoutput_segments, copy);
                    state->unoutput_bytes += data_len;
                    state->next_expected_seqno += (uint32_t)data_len;
                } else {
                    /*
                     * Do not advance expected sequence number when we could
                     * not retain the payload; the sender can retransmit it.
                     */
                }
            }

            /*
             * FIN follows any payload in the same segment. Only consume it
             * once, when it is at the expected sequence number.
             */
            if ((flags & TH_FIN) &&
                seqno + (uint32_t)data_len == state->next_expected_seqno &&
                !state->fin_received) {
                state->next_expected_seqno += 1;
                state->fin_received = true;
                conn_output(state->conn, NULL, 0);
#ifdef DEBUG
                fprintf(stderr, "[FIN RX] peer FIN accepted; next_expected=%u\n",
                        state->next_expected_seqno);
#endif
            }
        }
    }

    /* Always send a cumulative ACK for data/FIN or out-of-order segments. */
    {
        ctcp_segment_t *ack_segment =
            ctcp_segment_create(state, TH_ACK,
                                state->next_expected_seqno, NULL, 0);
        if (ack_segment != NULL) {
            if (conn_send(state->conn, ack_segment,
                          sizeof(ctcp_segment_t)) == -1) {
                free(ack_segment);
            }
        }
    }

    ctcp_output(state);
    free(segment);

    /* May destroy state; do not access it after this call returns true. */
    (void)ctcp_maybe_destroy(state);
}

void ctcp_output(ctcp_state_t *state)
{
    size_t buf_space;
    size_t bytes_outputted = 0;

    if (state == NULL) {
        return;
    }

    buf_space = conn_bufspace(state->conn);
    if (buf_space == 0) {
        return;
    }

    while (bytes_outputted < buf_space) {
        ll_node_t *node = ll_front(state->unoutput_segments);
        ctcp_segment_t *segment;
        size_t segment_data_len;
        size_t bytes_to_copy;

        if (node == NULL) {
            break;
        }

        segment = (ctcp_segment_t *)node->object;
        segment_data_len =
            ntohs(segment->len) - sizeof(ctcp_segment_t);

        /*
         * This implementation assumes conn_output() consumes all bytes
         * passed and the framework calls ctcp_output() when buffer space
         * becomes available. For partial-segment output, the queue needs an
         * explicit per-segment output offset.
         */
        bytes_to_copy = segment_data_len;
        if (bytes_to_copy > buf_space - bytes_outputted) {
            bytes_to_copy = buf_space - bytes_outputted;
        }

        if (bytes_to_copy > 0) {
            conn_output(state->conn, segment->data, bytes_to_copy);
            bytes_outputted += bytes_to_copy;
            state->unoutput_bytes -= bytes_to_copy;
        }

        if (bytes_to_copy == segment_data_len) {
            ll_remove(state->unoutput_segments, node);
            free(segment);
        } else {
            /*
             * Preserve the remainder by moving it to the start of the
             * segment's payload and updating its declared length.
             */
            memmove(segment->data, segment->data + bytes_to_copy,
                    segment_data_len - bytes_to_copy);
            segment->len = htons(sizeof(ctcp_segment_t) +
                                 segment_data_len - bytes_to_copy);
            break;
        }
    }
}

void ctcp_timer(void)
{
    long now = current_time();
    ctcp_state_t *state = state_list;

    while (state != NULL) {
        ctcp_state_t *next_state = state->next;
        ll_node_t *node = ll_front(state->sent_unacked);
        while (node != NULL) {
            ll_node_t *next_node = node->next;
            sent_segment_t *ss = (sent_segment_t *)node->object;

            if (ss != NULL &&
                !ss->retry_exhausted &&
                now - ss->last_sent >=
                    (long)state->ctcp_cfg->rt_timeout) {
                if (ss->retries >= CTCP_MAX_RETRIES) {
                    /*
                     * Do not destroy the connection on timeout: the required
                     * close policy is to destroy only after both FIN directions
                     * have completed. Keep the segment tracked so a late ACK
                     * can still complete shutdown.
                     */
                    ss->retry_exhausted = true;
#ifdef DEBUG
                    fprintf(stderr,
                            "[TIMEOUT] seq=%u retries exhausted; keeping state alive\n",
                            ntohl(ss->seg->seqno));
#endif
                } else {
                    ss->retries++;
                    ss->last_sent = now;
#ifdef DEBUG
                    fprintf(stderr, "[RETX] seq=%u retry=%d\n",
                            ntohl(ss->seg->seqno), ss->retries);
#endif
                    if (conn_send(state->conn, ss->seg, ss->len) == -1) {
#ifdef DEBUG
                        fprintf(stderr, "[RETX] conn_send failed\n");
#endif
                    }
                }
            }

            node = next_node;
        }

        /*
         * No destruction here. Timeout alone does not mean both endpoints
         * completed the FIN/ACK shutdown handshake.
         */
        state = next_state;
    }
}

static ctcp_segment_t *ctcp_segment_create(
    ctcp_state_t *state, uint32_t flags, uint32_t ackno,
    void *data, size_t data_len)
{
    size_t segment_len = sizeof(ctcp_segment_t) + data_len;
    ctcp_segment_t *segment;

    if (state == NULL || segment_len > UINT16_MAX) {
        return NULL;
    }

    segment = calloc(1, segment_len);
    if (segment == NULL) {
        return NULL;
    }

    segment->seqno = htonl(state->next_seqno);
    segment->ackno = htonl(ackno);
    segment->len = htons((uint16_t)segment_len);
    segment->flags = flags;
    segment->window = htons(state->ctcp_cfg->recv_window);

    if (data != NULL && data_len > 0) {
        memcpy(segment->data, data, data_len);
    }

    segment->cksum = 0;
    segment->cksum = cksum(segment, segment_len);
    return segment;
}
