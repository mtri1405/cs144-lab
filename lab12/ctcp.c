/******************************************************************************
 * ctcp.c
 * ------
 * cTCP Lab 2: sliding window.
 *  
 *****************************************************************************/

#include "ctcp.h"
#include "ctcp_linked_list.h"
#include "ctcp_sys.h"
#include "ctcp_utils.h"

#define SEGMENT_HDR_SIZE sizeof(ctcp_segment_t)
#define MAX_RETRANSMIT 5

/* Cấu trúc lưu các segment đã gửi trong ll*/
typedef struct send_segment {
  ctcp_segment_t *segment;  // Segment ở network order
  size_t len;               // Header + Payload

  uint32_t start_seq;       // sequence number đầu tiên
  uint32_t end_seq;         // sequence number tiếp 

  long sent_time;           // Thời gian bắt đầu gửi
  int rext_count;           // Số lần thực hiện gửi lại
} send_segment_t;

/* Cấu trúc lưu các segment đã nhận trong ll*/
typedef struct recv_segment {
  ctcp_segment_t *segment;   // Segment ở dạng host 
  size_t len;                // Header + 

  uint32_t start_seq;
  uint32_t end_seq;
} recv_segment_t;

struct ctcp_state
{
  struct ctcp_state *next;  /* Next in linked list */
  struct ctcp_state **prev; /* Prev in linked list */

  conn_t *conn;      /* Connection object */
  ctcp_config_t *cfg;

  uint32_t seqno;    /* Seqno của byte kế tiếp sẽ gửi */
  uint32_t ackno;    /* Seqno của byte kế tiếp mong đợi nhận */

  uint32_t send_base;

  /* Flow control */
  uint16_t peer_window;

  /* Sliding Window queues */
  linked_list_t *send_queue;
  linked_list_t *recv_queue;

   /* Tổng số byte payload trong recv_queue */
  size_t recv_buffered;
  /* Dữ liệu đã nhận hợp lệ nhưng chưa output hết ra STDOUT */
  char pending[MAX_SEG_DATA_SIZE];
  size_t pending_len;
  size_t pending_off;

  /* Quản lý FIN */
  bool fin_sent;
  bool fin_received;
  bool fin_pending;
  uint32_t pending_fin_seq;
  bool fin_acked;
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

static uint16_t _recv_window_available(ctcp_state_t *state){
  return state->cfg->recv_window - state->recv_buffered;
}
/* Kiểm tra độ dài và checksum. Segment đang ở network order. */
static int _is_segment_valid(ctcp_segment_t *segment, size_t len)
{
  if (len < SEGMENT_HDR_SIZE || segment == )
    return -1;

  size_t seg_len = ntohs(segment->len);
  if (seg_len < SEGMENT_HDR_SIZE || seg_len > len 
    || seg_len > SEGMENT_HDR_SIZE + MAX_SEG_DATA_SIZE)
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

// Tạo segment, chưa gửi
static ctcp_segment_t *_segment_create(ctcp_state_t *state, uint32_t flags, const void *data, size_t len)
{
  if (len > MAX_SEG_DATA_SIZE ||
    (len > 0 && data == NULL))
{
  return NULL;
}
  size_t seg_len = SEGMENT_HDR_SIZE + len;
  ctcp_segment_t *seg = calloc(1, seg_len);
  if (seg == NULL){
    perr("Cant create segment");
    return NULL;
  }

  seg->seqno = state->seqno;
  seg->ackno = state->ackno;
  seg->len = (uint16_t)seg_len;
  seg->flags = flags;
  seg->window = _recv_window_available(state);
  if (len > 0 && data != NULL)
    memcpy(seg->data, data, len);

  _segment_hton(seg);
  seg->cksum = 0;
  seg->cksum = cksum(seg, (uint16_t)seg_len);

  return seg;
}

/**
 * Gửi segment. Segment mang data hoặc FIN được lưu lại để chờ ACK
 * (và retransmit); ACK thuần thì gửi rồi bỏ.
 * flags ở host order.
 */
static int _segment_send(ctcp_state_t *state, uint32_t flags, const void *data, size_t len)
{
  ctcp_segment_t *seg = _segment_create(state, flags, data, len);
  if (seg == NULL)
    return -1;
  size_t seg_len = SEGMENT_HDR_SIZE + len;

  // Quản lý gói tin và đưa vào
  if (len > 0 || (flags & FIN)){ 
    send_segment_t *entry = calloc(1, sizeof(send_segment_t));
    if (entry == NULL){
      free(seg);
      return -1;
    }
    // Tính số bytes của data (len) và FIN (1 byte) nếu có
    uint32_t consumed = (uint32_t)len + ((flags & FIN) ? 1 : 0);
    /*Change: tạo entry và lưu vào send_queue rồi mới gửi gói tin.
    * Reason: Nếu gửi gói tin trước mà cấp phát queue thất bại thì không quản lý được gói tin
    */
    entry->segment = seg;
    entry->len = seg_len;
    entry->start_seq = state->seqno;
    entry->end_seq = state->seqno + consumed;
    entry->rext_count = 0;
    entry->sent_time = current_time();
    ll_node_t *node = ll_add(state->send_queue, entry);
    if (node == NULL) {
      free(entry->segment);
      free(entry);
      return -1;
    }
     
    if (conn_send(state->conn, seg, seg_len) == -1){
      perr("segment send fail");
      ll_remove(state->send_queue, node);
      free(entry->segment);
      free(entry);
      return -1;
    }
    // Update seqno của state bằng số byte đã gửi
    state->seqno += consumed;
  } else {
    // Đây là gói tin ACK chỉ cần gửi ACK không cần lưu lại
    if (conn_send(state->conn, seg, seg_len) == -1){
      perr("segment send fail");
      free(seg);
      return -1;
    }
    free(seg);
  }

  return 0;
}

/* Helper gửi ack*/
static void _send_ack(ctcp_state_t *state)
{
  if (_segment_send(state, ACK, NULL, 0) < 0)
    perr("cannot send ACK");
}


/* Helper xử lý ack thu được */
static void _process_ack(ctcp_state_t *state, uint32_t ackno)
{
  if (ackno < state->send_base || ackno > state->seqno)
    return;

  ll_node_t *node = ll_front(state->send_queue);
  while (node != NULL) {
    ll_node_t *next = node->next;
    send_segment_t *entry = (send_segment_t *)node->object;
    if (entry->end_seq <= ackno) {
      if (ntohl(entry->segment->flags) & FIN)
        state->fin_acked = true;
      ll_remove(state->send_queue, node);
      free(entry->segment);
      free(entry);
    }
    node = next;
  }
  state->send_base = ackno;
}

static size_t _send_window_available(ctcp_state_t *state){
  uint32_t in_flight = state->seqno - state->send_base;
  size_t window = state->cfg->send_window;
  if (in_flight >= window) return 0;
  return window - in_flight;
}

static void _drain_recv_queue(ctcp_state_t *state);


/* Xử lý segment mang dữ liệu */
static void _handle_data(ctcp_state_t *state, ctcp_segment_t *segment, size_t data_len)
{
  if (data_len > MAX_SEG_DATA_SIZE)
    return;
  if (segment->seqno < state->ackno){
    _send_ack(state);
    return;
  }
  // Đúng dữ liệu đang đợi và không còn data chờ
  if (segment->seqno == state->ackno && state->pending_len == 0)
  {
    memcpy(state->pending, segment->data, data_len);
    state->pending_len = data_len;
    state->pending_off = 0;

    state->ackno += data_len;
    _send_ack(state);
    ctcp_output(state);
    _drain_recv_queue(state);
    return;
  }
  // Dữ liệu tới đúng nhưng vẫn còn data chưa được output hoặc đến sai thứ tự
  if (segment->seqno >= state->ackno &&
      segment->seqno + data_len <= state->ackno + state->cfg->recv_window)
  {
    recv_segment_t *entry = calloc(1, sizeof(recv_segment_t));
    if (entry == NULL)
    {
      perr("Allocated entry for recv queue fail");
      return;
    }
    entry->segment = calloc(1, segment->len);
    if (entry->segment == NULL)
    {
      perr("Segment Allocated fail");
    }
    memcpy(entry->segment, segment, segment->len);
    entry->len = segment->len;
    entry->start_seq = segment->seqno;
    entry->end_seq = segment->seqno + (uint32_t)data_len;

    // Dò trong danh sách queue để tìm vị trí thích hợp
    ll_node_t *curr = ll_front(state->recv_queue);
    while (curr != NULL &&
           ((recv_segment_t *)curr->object)->start_seq < entry->start_seq)
      curr = curr->next;

    // TH đã có gói tin bị trùng
    if (curr != NULL &&
            ((recv_segment_t *)curr->object)->start_seq == entry->start_seq)
    {
      free(entry->segment);
      free(entry);
      return;
    }

    ll_node_t *new_node;
    // TH không có start_seq lớn hơn, add vào cuối list
    if (curr == NULL)
      new_node = ll_add(state->recv_queue, entry);
    else if (curr->prev == NULL) // Node nằm ở vị trí đầu list
      new_node = ll_add_front(state->recv_queue, entry);
    else // Node nằm ở giữa 2 node khác
      new_node = ll_add_after(state->recv_queue, curr->prev, entry);

    // Sau khi lưu trữ gói tin thành công, tiến hành ACK gói tin

    if (new_node == NULL)
    {
      free(entry->segment);
      free(entry);
      return;
    }
    _send_ack(state);
    state->recv_buffered += data_len;
  }
}

static void _drain_recv_queue(ctcp_state_t *state)
{
  while (state->pending_len == 0){
    ll_node_t *node = ll_front(state->recv_queue);
    if (node == NULL) 
      return;
    recv_segment_t *entry = (recv_segment_t*)node->object;
    if (entry->start_seq != state->ackno)
      break;
    size_t data_len = entry->len - SEGMENT_HDR_SIZE;
    memcpy(state->pending, entry->segment->data, data_len);
    state->pending_len = data_len;
    state->pending_off = 0;
    state->recv_buffered -= data_len;
    // Update ackno
    state->ackno += data_len;
    ll_remove(state->recv_queue, node);
    free(entry->segment);
    free(entry);
    ctcp_output(state);
  }
  _send_ack(state);
}

/* Xử lý FIN (không kèm data) */
static void _handle_fin(ctcp_state_t *state, ctcp_segment_t *segment)
{
  if (segment->seqno == state->ackno)
  {
    if (state->pending_len != 0)
    {
      state->fin_pending = true;
      state->pending_fin_seq = segment->seqno;
      return;
    }
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

static void _handle_pending_fin(ctcp_state_t *state)
{
  if (state->fin_pending && state->pending_len == 0 &&
  state->pending_fin_seq == state->ackno){
    state->fin_pending = false;
    state->fin_received = true;
    state->ackno++;
    conn_output(state->conn, NULL, 0);
    _send_ack(state);
  }
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
  state->send_base = 1;
  state->send_queue = ll_create();
  state->recv_queue = ll_create();
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

  ll_node_t *node = ll_front(state->send_queue);
  while (node != NULL) {
    ll_node_t *next = node->next;
    send_segment_t *entry = (send_segment_t *)node->object;
    free(entry->segment);
    free(entry);
    node = next;
  }
  ll_destroy(state->send_queue);
  node = ll_front(state->recv_queue);
  while (node != NULL) {
    ll_node_t *next = node->next;
    recv_segment_t *entry = (recv_segment_t *)node->object;
    free(entry->segment);
    free(entry);
    node = next;
  }
  ll_destroy(state->recv_queue);
  free(state->cfg);
  free(state);
  end_client();
}

void ctcp_read(ctcp_state_t *state)
{
  /* Stop-and-wait: chỉ đọc/gửi tiếp khi không còn segment nào chờ ACK. 
  *  Update: sliding windows: Kiểm tra kích thước data, tạo và xử lý segment hợp lý.
  */
  if (state->fin_sent)
    return;

  size_t max_len = _send_window_available(state);
  if (max_len == 0)
    return;
  
  char *buf = malloc(max_len);
  if (buf == NULL) {
    perr ("Cannot allocate buffer for reading");
    return;
  }

  size_t offset = 0;
  // n is byte read
  int n = conn_input(state->conn, buf, max_len);
  if (n > 0)
  {
    // Split data into multiple segment
    while (offset < (size_t)n) {
      size_t chunk = (size_t)n - offset;
      if (chunk > MAX_SEG_DATA_SIZE)
        chunk = MAX_SEG_DATA_SIZE;
      if (_segment_send(state, ACK, buf + offset, chunk) < 0) {
        perr("cannot send data segment");
        return;
      }
      offset += chunk;
    }
  }
  else if (n < 0)
  {
    /* EOF: gửi FIN */
    if (_send_window_available(state) < 1)
      return;
    if (_segment_send(state, FIN | ACK, NULL, 0) < 0) {
      perr("cannot send FIN");
      return;
    }
    state->fin_sent = true;
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
  _process_ack(state, segment->ackno);

  /* 2. Xử lý data / FIN */
  if (data_len > 0)
    _handle_data(state, segment, data_len);
  else if (segment->flags & FIN)
    _handle_fin(state, segment);

  free(segment);

  /* 3. Kiểm tra điều kiện đóng kết nối */
  if (state->fin_received && state->fin_sent && state->fin_acked &&
      state->pending_len == 0 && ll_length(state->recv_queue) == 0 &&
      ll_length(state->send_queue) == 0) {
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
    /* Đã output hết: tiến hành tăng ackno và ACK cho bên gửi */
    state->pending_len = 0;
    state->pending_off = 0;
    _drain_recv_queue(state);
    _handle_pending_fin(state);
  }
}

void ctcp_timer()
{
  long now = current_time();
  ctcp_state_t *state = state_list;

  while (state != NULL)
  {
    ctcp_state_t *next_state = state->next; /* state có thể bị destroy */

    // Lấy thông tin segment chưa ack trong queue để kiểm tra và gửi lại
    linked_list_t *queue = state->send_queue;
    ll_node_t *node = ll_front(queue);
    while (node != NULL)
    {
      send_segment_t *entry = (send_segment_t *)node->object;
      if (now - entry->sent_time > state->cfg->rt_timeout)
      {
        if (entry->rext_count >= MAX_RETRANSMIT)
        {
          perr("Peer unresponsive, tearing down");
          ctcp_destroy(state);
          break;
        }
        else
        {
          if (conn_send(state->conn, entry->segment, entry->len) < 0){
            perr("retransmit failed");
          }
          entry->sent_time = current_time();
          entry->rext_count++;
        }
      }
      node = node->next;
    }
    state = next_state;
  }
}
