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

#include "ctcp.h"
#include "ctcp_linked_list.h"
#include "ctcp_sys.h"
#include "ctcp_utils.h"



/**
 * Connection state.
 *
 * Stores per-connection information such as the current sequence number,
 * unacknowledged packets, etc.
 *
 * You should add to this to store other fields you might need.
 */
struct ctcp_state {
  struct ctcp_state *next;  /* Next in linked list */
  struct ctcp_state **prev; /* Prev in linked list */

  conn_t *conn;             /* Connection object -- needed in order to figure
                               out destination when sending */
  linked_list_t *segments;  /* Linked list of segments sent to this connection.
                               It may be useful to have multiple linked lists
                               for unacknowledged segments, segments that
                               haven't been sent, etc. Lab 1 uses the
                               stop-and-wait protocol and therefore does not
                               necessarily need a linked list. You may remove
                               this if this is the case for you */

  /* FIXME: Add other needed fields. */
  ctcp_config_t *ctcp_cfg;
  uint32_t next_seqno;
  bool fin_sent;
};

/**
 * Linked list of connection states. Go through this in ctcp_timer() to
 * resubmit segments and tear down connections.
 */
static ctcp_state_t *state_list;

/* FIXME: Feel free to add as many helper functions as needed. Don't repeat
          code! Helper functions make the code clearer and cleaner. */
ctcp_segment_t *ctcp_segment_create(ctcp_state_t *state, uint32_t flag, void *data, size_t len);

ctcp_state_t *ctcp_init(conn_t *conn, ctcp_config_t *cfg) {
  /* Connection could not be established. */
  if (conn == NULL) {
    return NULL;
  }

  /* Established a connection. Create a new state and update the linked list
     of connection states. */
  ctcp_state_t *state = calloc(sizeof(ctcp_state_t), 1);
  state->segments = ll_create();

  state->next = state_list;
  state->prev = &state_list;
  if (state_list)
    state_list->prev = &state->next;
  state_list = state;

  /* Set fields. */
  state->conn = conn;
  /* FIXME: Do any other initialization here. */
  state->ctcp_cfg = cfg;
  state->next_seqno = 0;
  state->fin_sent = false;


  return state;
}

void ctcp_destroy(ctcp_state_t *state) {  /* Update linked list. */
  if (state->next)
    state->next->prev = state->prev;

  *state->prev = state->next;
  conn_remove(state->conn);

  /* FIXME: Do any other cleanup here. */
  ll_node_t *node = ll_front(state->segments);
  while (node != NULL) {
    ll_node_t *next = node->next;
    free(node->object);
    ll_remove(state->segments, node);
    node = next;
  }
  ll_destroy(state->segments);
  free(state->ctcp_cfg);


  free(state);
  end_client();
}



void ctcp_read(ctcp_state_t *state) {
  /* FIXME */
  uint8_t buf[MAX_SEG_DATA_SIZE];
  int bytes_read = conn_input(state->conn, buf, sizeof(buf));

  if (bytes_read == 0) {
    return;
  } else if (bytes_read > 0) {
    ctcp_segment_t *segment = ctcp_segment_create(state, 0, buf, (size_t) bytes_read);
    size_t segment_len = sizeof(ctcp_segment_t) + (size_t) bytes_read;
    if (segment == NULL) {
      return;
    }

    if (conn_send(state->conn, segment, segment_len) == -1) {
      free(segment);
      return;
    }
    ll_add(state->segments, segment);
    state->next_seqno += (uint32_t) bytes_read;
  } else if (bytes_read == -1) {
    if (state->fin_sent) {
      return;
    }

    ctcp_segment_t *segment = calloc(1, sizeof(ctcp_segment_t));
    if (segment == NULL) {
      return;
    }

    segment = ctcp_segment_create(state, TH_FIN, 0, 0);


    if (conn_send(state->conn, segment, sizeof(ctcp_segment_t)) == -1) {
      free(segment);
      return;
    }
    ll_add(state->segments, segment);
    state->fin_sent = true;
  }

}

void ctcp_receive(ctcp_state_t *state, ctcp_segment_t *segment, size_t len) {
  /* FIXME */
  // Send ACK for received segment
  ctcp_segment_t *ack_segment = ctcp_segment_create(state, TH_ACK, NULL, 0);
  if (ack_segment == NULL) {
    return;
  }

  if (conn_send(state->conn, ack_segment, sizeof(ctcp_segment_t)) == -1) {
    free(ack_segment);
    return;
  }

  
  ctcp_output(state);
}

void ctcp_output(ctcp_state_t *state) {
  /* FIXME */
}

void ctcp_timer() {
  /* FIXME */
}


ctcp_segment_t *ctcp_segment_create(ctcp_state_t *state, uint32_t flag, void *data, size_t len) {

  size_t segment_len = sizeof(ctcp_segment_t) + len;
  ctcp_segment_t *segment = calloc(1, segment_len);
  if (segment == NULL) {
    return NULL;
  }

  segment->seqno = htonl(state->next_seqno);
  segment->ackno = 0;
  segment->len = htons(segment_len);
  segment->flags = flag;
  segment->window = htons(state->ctcp_cfg->recv_window);
  memcpy(segment->data, data, len);
  segment->cksum = cksum(segment, segment_len);

  return segment;
}