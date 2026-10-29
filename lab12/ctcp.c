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

// User Include
#include <inttypes.h>

#define MAX_BUF_SIZE MAX_SEG_DATA_SIZE
#define SEGMENT_HDR_SIZE sizeof(ctcp_segment_t)

// Enable for debug
#define DEBUG
char buf_in[MAX_BUF_SIZE];
char buf_out[MAX_BUF_SIZE];

typedef enum conn_state
{
  DATA_TRANSFER,
  WAIT_ACK_FIN,
  WAIT_LAST_FIN,
  TEAR_DOWN
} conn_state_t;

typedef struct cached_segment {
  ctcp_segment_t *segment;
  size_t segment_len;
  long long send_time;  // Thời gian gửi gần nhất (ms)
  int retrans_count;    // Số lần đã gửi lại
} cached_segment_t;
/**
 * Connection state.
 *
 * Stores per-connection information such as the current sequence number,
 * unacknowledged packets, etc.
 *
 * You should add to this to store other fields you might need.
 */
struct ctcp_state
{
  struct ctcp_state *next;  /* Next in linked list */
  struct ctcp_state **prev; /* Prev in linked list */

  conn_t *conn;            /* Connection object -- needed in order to figure
                             out destination when sending */
  linked_list_t *segments; /* Linked list of segments sent to this connection.
                             It may be useful to have multiple linked lists
                             for unacknowledged segments, segments that
                             haven't been sent, etc. Lab 1 uses the
                             stop-and-wait protocol and therefore does not
                             necessarily need a linked list. You may remove
                             this if this is the case for you */

  /* FIXME: Add other needed fields. */
  ctcp_config_t *cfg;
  uint32_t seqno;
  uint32_t next_seqno;
  uint32_t ackno;
  conn_state_t conn_status;
  bool FIN_receive;
};

/**
 * Linked list of connection states. Go through this in ctcp_timer() to
 * resubmit segments and tear down connections.
 */
static ctcp_state_t *state_list;

/* FIXME: Feel free to add as many helper functions as needed. Don't repeat
          code! Helper functions make the code clearer and cleaner. */

static int _segment_send(ctcp_state_t *state, uint32_t flags, void *data, size_t len);

static void perr(char *str)
{
  fprintf(stderr, "Error: %s\n", str);
  // exit(0);
}

static void _segment_hton(ctcp_segment_t *segment)
{
  segment->seqno = htonl(segment->seqno);
  segment->ackno = htonl(segment->ackno);
  segment->len = htons(segment->len);
  segment->flags = htonl(segment->flags);
  segment->window = htons(segment->window);
}

static void _segment_ntoh(ctcp_segment_t *segment)
{
  segment->seqno = ntohl(segment->seqno);
  segment->ackno = ntohl(segment->ackno);
  segment->len = ntohs(segment->len);
  segment->flags = ntohl(segment->flags);
  segment->window = ntohs(segment->window);
}

#ifdef DEBUG
static void _print_segment(ctcp_segment_t *segment)
{
  fprintf(stderr, "Seq num: %zu\n", (long)segment->seqno);
  fprintf(stderr, "ACK No: %zu\n", (long)segment->ackno);
  fprintf(stderr, "Len: %d\n", (int)segment->len);
  fprintf(stderr, "Flag: %zu\n", (long)segment->flags);
  fprintf(stderr, "Window: %d\n", (int)segment->window);
  fprintf(stderr, "cksum: %d\n", (int)segment->cksum);
}
#endif

static int _is_segment_valid(ctcp_segment_t *segment, size_t len)
{
  if (ntohs(segment->len) > len)
  {
    perr("segment len error");
    return -1;
  }
  int32_t sum = segment->cksum;
  segment->cksum = 0;
  if (cksum(segment, ntohs(segment->len)) != sum)
  {
    perr("cksum err");
    return -1;
  }
  return 0;
}

void _segment_ack_handle(ctcp_state_t *state, ctcp_segment_t *segment)
{
  ll_node_t *node = ll_front(state->segments);
  while (node != NULL)
  {
    cached_segment_t *cached = (cached_segment_t*)node->object;
    ctcp_segment_t *cached_seg = cached->segment;
    uint32_t next_seq = cached_seg->seqno + cached_seg->len - SEGMENT_HDR_SIZE;
    if (cached_seg->flags & FIN) next_seq++;
    if (next_seq == segment->ackno)
    {
      ll_remove(state->segments, node);
      free(cached_seg);
      free(cached);
      #ifdef DEBUG
      perr("Find and Free Segment");
      #endif

      break;
    }
    node = node->next;
  }
  if (node == NULL)
  {
    perr("Cant Find Segment");
  }
}

ctcp_state_t *ctcp_init(conn_t *conn, ctcp_config_t *cfg)
{
  /* Connection could not be established. */
  if (conn == NULL)
  {
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
  state->seqno = 1;
  state->ackno = 1;
  /* FIXME: Do any other initialization here. */
  state->cfg = cfg;
  state->conn_status = DATA_TRANSFER;
  state->FIN_receive = false;
  return state;
}

void ctcp_destroy(ctcp_state_t *state)
{ /* Update linked list. */
  if (state->next)
    state->next->prev = state->prev;

  *state->prev = state->next;
  conn_remove(state->conn);

  /* FIXME: Do any other cleanup here. */

  free(state);
  end_client();
}

void ctcp_read(ctcp_state_t *state)
{
  /* FIXME */
  if (state->conn_status == WAIT_LAST_FIN) {return;}
  
  uint32_t flags = 0;
  memset(buf_in, 0, sizeof(buf_in));
  int bytes_read = conn_input(state->conn, buf_in, sizeof(buf_in));

  if (bytes_read > 0)
  {
    if (_segment_send(state, flags, buf_in, bytes_read) == -1)
    {
      perr("ctcp_read error sending ACK\n");
      return;
    }
  }
  else if (bytes_read == -1)
  {
    flags |= FIN;
    state->conn_status = WAIT_ACK_FIN;
    if (_segment_send(state, flags, NULL, 0) == -1)
    {
      perr("ctcp_read error sending FIN\n");
      return;
    }
  }
}

void ctcp_receive(ctcp_state_t *state, ctcp_segment_t *segment, size_t len)
{
  /* FIXME */
  // Send ACK for received segment
  if (_is_segment_valid(segment, len) == -1)
  {
    perr("Segment not valid\n");
    return;
  }
  _segment_ntoh(segment);

  #ifdef DEBUG
  perr("receive Segment");
  #endif

  switch (state->conn_status)
  {
    case DATA_TRANSFER:
      if (segment->flags & FIN)
      {
        #ifdef DEBUG
        perr("Read FIN");
        #endif
        // Receive FIN Segment
        state->ackno = segment->seqno + 1;
        // Send EOF to OUTPUT
        conn_output(state->conn, NULL, 0);
        // Confirm FIN
        state->FIN_receive = true;
        if (_segment_send(state, ACK | FIN, NULL, 0) == -1)
        {
          perr("FIN send FAIL\n");
        }
      }
      else if (segment->flags & ACK)
      {
        // Receive ACK Segment
        #ifdef DEBUG
        perr("Read ACK");
        #endif
        // Update status and remove segment from sending again.
        // Find and remove segment have been ACK using handle
        _segment_ack_handle(state, segment);
      }
      break;

    case WAIT_ACK_FIN:
      if (segment->flags & (ACK | FIN))
      {
        // Receive ACK FIN Segment
        #ifdef DEBUG
        perr("Read ACK FIN");
        #endif

        _segment_ack_handle(state, segment);
        if (state->FIN_receive){
          state->conn_status = TEAR_DOWN;
        } else {
          state->conn_status = WAIT_LAST_FIN;
        }
      }
      break;
    
    case WAIT_LAST_FIN:
      if (segment->flags & (FIN))
      {
        // Receive ACK Segment
        #ifdef DEBUG
        perr("Read LAST ACK FIN");
        #endif

        state->ackno = segment->seqno + 1;
        if (_segment_send(state, ACK, NULL, 0) == -1)
        {
          perr("ACK for last FIN send FAIL\n");
        }
        state->conn_status = TEAR_DOWN;
      }
      break;

    case TEAR_DOWN:
      free(segment);
      ctcp_destroy(state);
      return;

    default:
      break;
  }
  

  // Receive normal segment

  if (segment->flags == 0 && (state->conn_status == DATA_TRANSFER || state->conn_status == WAIT_LAST_FIN))
  {
    #ifdef DEBUG
    perr("Read Normal Segment");
    #endif
    memset(buf_out, 0, sizeof(buf_out));
    memcpy(buf_out, segment->data, segment->len - SEGMENT_HDR_SIZE);
    ctcp_output(state);
    state->ackno = segment->seqno + segment->len - SEGMENT_HDR_SIZE;
    if (_segment_send(state, ACK, NULL, 0) < 0)
    {
      perr("Cannot send ACK segment\n");
    }
  }
  // TEAR_DOWN Happend after switch-case
  if (state->conn_status == TEAR_DOWN)
  {
    free(segment);
    ctcp_destroy(state);
    return;
  }
  free(segment);
}

void ctcp_output(ctcp_state_t *state)
{
  /* FIXME */
  int avail_buf = conn_bufspace(state->conn);
  if (avail_buf == 0)
    return;
  if (avail_buf > sizeof(buf_out))
  {
    conn_output(state->conn, buf_out, sizeof(buf_out));
  }
}

void ctcp_timer()
{
  ctcp_state_t *state = state_list;
  long curr_time = current_time();

  while (state != NULL)
  {
    ctcp_state_t *next_state = state->next; // Lưu lại để phòng trường hợp state bị destroy
    ll_node_t *node = ll_front(state->segments);

    while (node != NULL)
    {
      cached_segment_t *cached = (cached_segment_t *)node->object;
      
      // Kiểm tra nếu đã quá thời gian timeout (rt_timeout)
      if (curr_time - cached->send_time >= state->cfg->rt_timeout)
      {
        cached->retrans_count++;

        // Nếu truyền lại quá 5 lần (tổng cộng 6 lần gửi), hủy kết nối theo yêu cầu đề bài
        if (cached->retrans_count > 5)
        {
          #ifdef DEBUG
          perr("Unresponsive connection. Tearing down.");
          #endif
          ctcp_destroy(state);
          break;
        }

        // Tạo bản copy để hton và gửi lại qua mạng
        size_t seg_len = cached->segment_len;
        ctcp_segment_t *send_seg = malloc(seg_len);
        memcpy(send_seg, cached->segment, seg_len);

        _segment_hton(send_seg);
        if (conn_send(state->conn, send_seg, seg_len) < 0)
        {
          perr("Timer retransmit send failed");
        }
        free(send_seg);

        // Cập nhật lại thời gian gửi mới cho lần timeout tiếp theo
        cached->send_time =current_time();
      }
      node = node->next;
    }
    state = next_state;
  }
}

static int _segment_send(ctcp_state_t *state, uint32_t flags, void *data, size_t len)
{

  size_t segment_len = SEGMENT_HDR_SIZE + len;
  ctcp_segment_t *segment = calloc(1, segment_len);
  if (segment == NULL)
  {
    return -1;
  }

  segment->seqno = state->seqno;

  if (flags & FIN)
  {
    state->seqno++;
  }
  else state->seqno += len; 
  segment->ackno = state->ackno;
  segment->len = segment_len;
  segment->flags = flags;
  segment->window = state->cfg->recv_window;
  memcpy(segment->data, data, len);
  segment->cksum = 0;

  // Add segment to ll
  if (len > 0 || (flags & FIN)){
    cached_segment_t *cached = malloc(sizeof(cached_segment_t));
    cached->segment_len = segment_len;
    cached->send_time = current_time();
    cached->retrans_count = 0;
    cached->segment = malloc(segment_len);
    memcpy(cached->segment, segment, segment_len);
    ll_add(state->segments, cached);

    #ifdef DEBUG
      _print_segment(segment);
  } else {
    perr("FIN Send");
    #endif

  }
  _segment_hton(segment);

  segment->cksum = cksum(segment, segment_len);
  if (conn_send(state->conn, segment, segment_len) < 0)
  {
    free(segment);
    return -1;
  }
  free(segment);
  return 0;
}