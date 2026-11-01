/******************************************************************************
 * ctcp.c
 * ------
 * cTCP Lab 1: stop-and-wait.
 *   - Tối đa 1 segment (data hoặc FIN) chưa được ACK tại một thời điểm.
 *   - Retransmit sau rt_timeout, tối đa 5 lần truyền lại (tổng 6 lần gửi),
 *     quá số đó thì xem như đối phương không phản hồi và đóng kết nối.
 *   - Flow control: chỉ ACK dữ liệu khi đã output xong ra conn_output().
 *   - Đóng kết nối khi: đã nhận FIN, đã gửi FIN và FIN được ACK, không còn
 *     dữ liệu nhận nào chưa output.
 *****************************************************************************/

#include "ctcp.h"
#include "ctcp_linked_list.h"
#include "ctcp_sys.h"
#include "ctcp_utils.h"

#define SEGMENT_HDR_SIZE sizeof(ctcp_segment_t)
#define MAX_RETRANSMIT 5

struct ctcp_state
{
  struct ctcp_state *next;  /* Next in linked list */
  struct ctcp_state **prev; /* Prev in linked list */

  conn_t *conn;      /* Connection object */
  ctcp_config_t *cfg;

  uint32_t seqno;    /* Seqno của byte kế tiếp sẽ gửi */
  uint32_t ackno;    /* Seqno của byte kế tiếp mong đợi nhận */

  /* Segment đang chờ ACK (stop-and-wait: tối đa 1). Lưu ở network order. */
  ctcp_segment_t *unacked;
  size_t unacked_len;
  uint32_t unacked_end;  /* ackno cần nhận được để coi là đã ACK */
  long sent_time;        /* Lần gửi gần nhất (ms) */
  int retx_count;        /* Số lần đã truyền lại */

  /* Dữ liệu đã nhận hợp lệ nhưng chưa output hết ra STDOUT */
  char pending[MAX_SEG_DATA_SIZE];
  size_t pending_len;
  size_t pending_off;

  bool fin_sent;
  bool fin_received;
};

static ctcp_state_t *state_list;

static void perr(const char *str)
{
  fprintf(stderr, "Error: %s\n", str);
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

/* Kiểm tra độ dài và checksum. Segment đang ở network order. */
static int _is_segment_valid(ctcp_segment_t *segment, size_t len)
{
  if (len < SEGMENT_HDR_SIZE)
    return -1;

  size_t seg_len = ntohs(segment->len);
  if (seg_len < SEGMENT_HDR_SIZE || seg_len > len)
    return -1;

  uint16_t recv_sum = segment->cksum;
  segment->cksum = 0;
  uint16_t calc_sum = cksum(segment, (uint16_t)seg_len);
  segment->cksum = recv_sum;
  if (calc_sum != recv_sum)
  {
    perr("checksum mismatch");
    return -1;
  }
  return 0;
}

/**
 * Tạo và gửi segment. Segment mang data hoặc FIN được lưu lại để chờ ACK
 * (và retransmit); ACK thuần thì gửi rồi bỏ.
 * flags ở host order.
 */
static int _segment_send(ctcp_state_t *state, uint32_t flags, const void *data, size_t len)
{
  size_t seg_len = SEGMENT_HDR_SIZE + len;
  ctcp_segment_t *seg = calloc(1, seg_len);
  if (seg == NULL)
    return -1;

  seg->seqno = state->seqno;
  seg->ackno = state->ackno;
  seg->len = (uint16_t)seg_len;
  seg->flags = flags;
  seg->window = state->cfg->recv_window;
  if (len > 0 && data != NULL)
    memcpy(seg->data, data, len);

  uint32_t consumed = (uint32_t)len + ((flags & FIN) ? 1 : 0);

  _segment_hton(seg);
  seg->cksum = 0;
  seg->cksum = cksum(seg, (uint16_t)seg_len);

  int r = conn_send(state->conn, seg, seg_len);

  if (consumed > 0)
  {
    /* Giữ lại để chờ ACK. Nếu gửi lỗi thì timer sẽ gửi lại. */
    state->unacked = seg;
    state->unacked_len = seg_len;
    state->unacked_end = state->seqno + consumed;
    state->seqno += consumed;
    state->sent_time = current_time();
    state->retx_count = 0;
    return 0;
  }

  free(seg);
  return (r < 0) ? -1 : 0;
}

static void _send_ack(ctcp_state_t *state)
{
  if (_segment_send(state, ACK, NULL, 0) < 0)
    perr("cannot send ACK");
}

ctcp_state_t *ctcp_init(conn_t *conn, ctcp_config_t *cfg)
{
  if (conn == NULL)
    return NULL;

  ctcp_state_t *state = calloc(1, sizeof(ctcp_state_t));
  if (state == NULL)
    return NULL;

  state->next = state_list;
  state->prev = &state_list;
  if (state_list)
    state_list->prev = &state->next;
  state_list = state;

  state->conn = conn;
  state->cfg = cfg;
  state->seqno = 1;
  state->ackno = 1;
  state->unacked = NULL;
  state->pending_len = 0;
  state->pending_off = 0;
  state->fin_sent = false;
  state->fin_received = false;
  return state;
}

void ctcp_destroy(ctcp_state_t *state)
{
  if (state->next)
    state->next->prev = state->prev;
  *state->prev = state->next;
  conn_remove(state->conn);

  free(state->unacked);
  free(state->cfg);
  free(state);
  end_client();
}

void ctcp_read(ctcp_state_t *state)
{
  /* Stop-and-wait: chỉ đọc/gửi tiếp khi không còn segment nào chờ ACK. */
  if (state->fin_sent || state->unacked != NULL)
    return;

  char buf[MAX_SEG_DATA_SIZE];
  size_t max_len = sizeof(buf);
  if (state->cfg->send_window > 0 && state->cfg->send_window < max_len)
    max_len = state->cfg->send_window;

  int n = conn_input(state->conn, buf, max_len);
  if (n > 0)
  {
    if (_segment_send(state, ACK, buf, (size_t)n) < 0)
      perr("cannot send data segment");
  }
  else if (n < 0)
  {
    /* EOF: gửi FIN */
    if (_segment_send(state, FIN | ACK, NULL, 0) < 0)
      perr("cannot send FIN");
    state->fin_sent = true;
  }
}

/* Xử lý segment mang dữ liệu */
static void _handle_data(ctcp_state_t *state, ctcp_segment_t *segment, size_t data_len)
{
  if (data_len > MAX_SEG_DATA_SIZE)
    return;

  if (segment->seqno == state->ackno)
  {
    /* Đúng thứ tự. Nếu đang còn dữ liệu chờ output thì bỏ qua (không ACK),
       bên gửi sẽ retransmit. */
    if (state->pending_len == 0)
    {
      memcpy(state->pending, segment->data, data_len);
      state->pending_len = data_len;
      state->pending_off = 0;
      ctcp_output(state); /* ACK được gửi khi đã output xong */
    }
  }
  else if (segment->seqno < state->ackno)
  {
    /* Segment trùng lặp: ACK lại để bên gửi tiến tiếp */
    _send_ack(state);
  }
}

/* Xử lý FIN (không kèm data) */
static void _handle_fin(ctcp_state_t *state, ctcp_segment_t *segment)
{
  if (segment->seqno == state->ackno)
  {
    if (state->pending_len != 0)
      return; /* chờ output xong dữ liệu trước đó */
    if (!state->fin_received)
    {
      state->fin_received = true;
      state->ackno++;
      conn_output(state->conn, NULL, 0); /* EOF */
    }
    _send_ack(state);
  }
  else if (segment->seqno < state->ackno)
  {
    _send_ack(state); /* FIN trùng lặp */
  }
}

void ctcp_receive(ctcp_state_t *state, ctcp_segment_t *segment, size_t len)
{
  if (_is_segment_valid(segment, len) < 0)
  {
    free(segment);
    return;
  }
  _segment_ntoh(segment);
  size_t data_len = segment->len - SEGMENT_HDR_SIZE;

  /* 1. Xử lý ACK cho segment đang chờ */
  if ((segment->flags & ACK) && state->unacked != NULL &&
      segment->ackno >= state->unacked_end)
  {
    free(state->unacked);
    state->unacked = NULL;
    state->unacked_len = 0;
    state->retx_count = 0;
  }

  /* 2. Xử lý data / FIN */
  if (data_len > 0)
    _handle_data(state, segment, data_len);
  else if (segment->flags & FIN)
    _handle_fin(state, segment);

  free(segment);

  /* 3. Kiểm tra điều kiện đóng kết nối */
  if (state->fin_sent && state->fin_received && state->unacked == NULL &&
      state->pending_len == 0)
  {
    ctcp_destroy(state);
    return;
  }

  /* 4. Có thể đã ACK xong, đọc tiếp input nếu có */
  ctcp_read(state);
}

void ctcp_output(ctcp_state_t *state)
{
  if (state->pending_len == 0)
    return;

  size_t space = conn_bufspace(state->conn);
  if (space == 0)
    return;

  size_t remain = state->pending_len - state->pending_off;
  size_t n = (remain < space) ? remain : space;

  int w = conn_output(state->conn, state->pending + state->pending_off, n);
  if (w <= 0)
    return;
  state->pending_off += (size_t)w;

  if (state->pending_off >= state->pending_len)
  {
    /* Đã output hết: tiến ackno và ACK cho bên gửi */
    state->ackno += (uint32_t)state->pending_len;
    state->pending_len = 0;
    state->pending_off = 0;
    _send_ack(state);
  }
}

void ctcp_timer()
{
  long now = current_time();
  ctcp_state_t *state = state_list;

  while (state != NULL)
  {
    ctcp_state_t *next_state = state->next; /* state có thể bị destroy */

    if (state->unacked != NULL &&
        now - state->sent_time >= state->cfg->rt_timeout)
    {
      if (state->retx_count >= MAX_RETRANSMIT)
      {
        perr("peer unresponsive, tearing down");
        ctcp_destroy(state);
      }
      else
      {
        if (conn_send(state->conn, state->unacked, state->unacked_len) < 0)
          perr("retransmit failed");
        state->retx_count++;
        state->sent_time = now;
      }
    }
    state = next_state;
  }
}