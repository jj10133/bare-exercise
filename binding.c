#include <assert.h>
#include <bare.h>
#include <js.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <utf.h>
#include <uv.h>

// Reused across every read on a given connection. Data is copied out into
// the growable response buffer before the next read fires, so reuse here
// is safe.
#define BARE_TCP_CAT_READ_BUFFER_SIZE 65536

typedef struct {
  uv_tcp_t handle;
  uv_timer_t timer;

  uv_connect_t connect_req;
  uv_write_t write_req;

  js_env_t *env;
  js_deferred_t *deferred;
  js_deferred_teardown_t *teardown;

  char *write_data;
  size_t write_len;

  char *response;
  size_t response_len;
  size_t response_cap;

  char read_buf[BARE_TCP_CAT_READ_BUFFER_SIZE];

  char error_code[32];
  char error_message[256];

  int pending_closes;
  bool closing;
  bool exiting;
} bare_tcp_cat_t;

static void
bare_tcp_cat_set_error(bare_tcp_cat_t *t, int uv_err) {
  if (t->error_message[0]) return; // first error wins

  // uv_err_name()/uv_strerror() return pointers into a thread-local static
  // buffer that later libuv calls can overwrite, so copy them out now
  // rather than holding onto the pointer for later use in settle().
  snprintf(t->error_code, sizeof(t->error_code), "%s", uv_err_name(uv_err));
  snprintf(t->error_message, sizeof(t->error_message), "%s", uv_strerror(uv_err));
}

static void
bare_tcp_cat_set_error_msg(bare_tcp_cat_t *t, const char *code, const char *message) {
  if (t->error_message[0]) return;

  snprintf(t->error_code, sizeof(t->error_code), "%s", code);
  snprintf(t->error_message, sizeof(t->error_message), "%s", message);
}

static bool
bare_tcp_cat_append(bare_tcp_cat_t *t, const char *data, size_t len) {
  if (t->response_len + len > t->response_cap) {
    size_t cap = t->response_cap == 0 ? 4096 : t->response_cap;

    while (cap < t->response_len + len)
      cap *= 2;

    char *next = realloc(t->response, cap);
    if (next == NULL) return false;

    t->response = next;
    t->response_cap = cap;
  }

  memcpy(t->response + t->response_len, data, len);
  t->response_len += len;

  return true;
}

static void
bare_tcp_cat_settle(bare_tcp_cat_t *t) {
  int err;

  if (!t->exiting) {
    js_env_t *env = t->env;

    js_handle_scope_t *scope;
    err = js_open_handle_scope(env, &scope);
    assert(err == 0);

    if (t->error_message[0]) {
      js_value_t *code;
      err = js_create_string_utf8(env, (utf8_t *) t->error_code, -1, &code);
      assert(err == 0);

      js_value_t *message;
      err = js_create_string_utf8(env, (utf8_t *) t->error_message, -1, &message);
      assert(err == 0);

      js_value_t *error;
      err = js_create_error(env, code, message, &error);
      assert(err == 0);

      err = js_reject_deferred(env, t->deferred, error);
      assert(err == 0);
    } else {
      void *data = NULL;

      js_value_t *arraybuffer;
      err = js_create_arraybuffer(env, t->response_len, &data, &arraybuffer);
      assert(err == 0);

      if (t->response_len > 0) memcpy(data, t->response, t->response_len);

      err = js_resolve_deferred(env, t->deferred, arraybuffer);
      assert(err == 0);
    }

    err = js_close_handle_scope(env, scope);
    assert(err == 0);
  }

  err = js_finish_deferred_teardown_callback(t->teardown);
  assert(err == 0);

  free(t->write_data);
  free(t->response);
  free(t);
}

static void
bare_tcp_cat_on_handle_close(uv_handle_t *handle) {
  bare_tcp_cat_t *t = (bare_tcp_cat_t *) handle->data;

  if (--t->pending_closes == 0) bare_tcp_cat_settle(t);
}

// Closes both handles exactly once. Safe to call from any callback,
// including re-entrantly from within the timer's own callback.
static void
bare_tcp_cat_close(bare_tcp_cat_t *t) {
  if (t->closing) return;
  t->closing = true;

  uv_read_stop((uv_stream_t *) &t->handle); // harmless if not reading

  uv_close((uv_handle_t *) &t->handle, bare_tcp_cat_on_handle_close);
  uv_close((uv_handle_t *) &t->timer, bare_tcp_cat_on_handle_close);
}

static void
bare_tcp_cat_on_timeout(uv_timer_t *handle) {
  bare_tcp_cat_t *t = (bare_tcp_cat_t *) handle->data;

  if (t->closing) return;

  // If we already received something, treat the timeout as "the peer
  // stopped sending more, return what we have" rather than a hard error -
  // many HTTP servers keep connections alive rather than closing them.
  if (t->response_len == 0) {
    bare_tcp_cat_set_error_msg(t, "ETIMEDOUT", "Timed out waiting for a response");
  }

  bare_tcp_cat_close(t);
}

static void
bare_tcp_cat_on_alloc(uv_handle_t *handle, size_t suggested_size, uv_buf_t *buf) {
  bare_tcp_cat_t *t = (bare_tcp_cat_t *) handle->data;

  buf->base = t->read_buf;
  buf->len = BARE_TCP_CAT_READ_BUFFER_SIZE;
}

static void
bare_tcp_cat_on_read(uv_stream_t *stream, ssize_t nread, const uv_buf_t *buf) {
  bare_tcp_cat_t *t = (bare_tcp_cat_t *) stream->data;

  if (t->closing) return;

  if (nread > 0) {
    if (!bare_tcp_cat_append(t, buf->base, (size_t) nread)) {
      bare_tcp_cat_set_error_msg(t, "ENOMEM", "Out of memory while buffering the response");
      bare_tcp_cat_close(t);
    }

    return;
  }

  if (nread == UV_EOF) {
    bare_tcp_cat_close(t); // success - peer closed, no error set
    return;
  }

  if (nread < 0) {
    bare_tcp_cat_set_error(t, (int) nread);
    bare_tcp_cat_close(t);
  }
}

static void
bare_tcp_cat_on_write(uv_write_t *req, int status) {
  bare_tcp_cat_t *t = (bare_tcp_cat_t *) req->data;

  if (t->closing) return;

  if (status < 0) {
    bare_tcp_cat_set_error(t, status);
    bare_tcp_cat_close(t);
    return;
  }

  int err = uv_read_start((uv_stream_t *) &t->handle, bare_tcp_cat_on_alloc, bare_tcp_cat_on_read);

  if (err < 0) {
    bare_tcp_cat_set_error(t, err);
    bare_tcp_cat_close(t);
  }
}

static void
bare_tcp_cat_on_connect(uv_connect_t *req, int status) {
  bare_tcp_cat_t *t = (bare_tcp_cat_t *) req->data;

  if (t->closing) return;

  if (status < 0) {
    bare_tcp_cat_set_error(t, status);
    bare_tcp_cat_close(t);
    return;
  }

  uv_buf_t buf = uv_buf_init(t->write_data, t->write_len);

  t->write_req.data = t;

  int err = uv_write(&t->write_req, (uv_stream_t *) &t->handle, &buf, 1, bare_tcp_cat_on_write);

  if (err < 0) {
    bare_tcp_cat_set_error(t, err);
    bare_tcp_cat_close(t);
  }
}

static void
bare_tcp_cat_on_teardown(js_deferred_teardown_t *handle, void *data) {
  bare_tcp_cat_t *t = (bare_tcp_cat_t *) data;

  t->exiting = true;

  bare_tcp_cat_close(t);
}

static js_value_t *
bare_tcp_cat(js_env_t *env, js_callback_info_t *info) {
  int err;

  size_t argc = 4;
  js_value_t *argv[4];

  err = js_get_callback_info(env, info, &argc, argv, NULL, NULL);
  assert(err == 0);

  assert(argc == 4);

  utf8_t host[INET6_ADDRSTRLEN];
  err = js_get_value_string_utf8(env, argv[0], host, INET6_ADDRSTRLEN, NULL);
  assert(err == 0);

  uint32_t port;
  err = js_get_value_uint32(env, argv[1], &port);
  assert(err == 0);

  void *write_data;
  size_t write_len;
  err = js_get_typedarray_info(env, argv[2], NULL, &write_data, &write_len, NULL, NULL);
  assert(err == 0);

  uint32_t timeout_ms;
  err = js_get_value_uint32(env, argv[3], &timeout_ms);
  assert(err == 0);

  bare_tcp_cat_t *t = calloc(1, sizeof(bare_tcp_cat_t));

  if (t == NULL) {
    err = js_throw_error(env, "ENOMEM", "Failed to allocate request");
    assert(err == 0);

    return NULL;
  }

  t->env = env;
  t->pending_closes = 2;

  if (write_len > 0) {
    t->write_data = malloc(write_len);

    if (t->write_data == NULL) {
      free(t);

      err = js_throw_error(env, "ENOMEM", "Failed to allocate request body");
      assert(err == 0);

      return NULL;
    }

    memcpy(t->write_data, write_data, write_len);
  }

  t->write_len = write_len;

  uv_loop_t *loop;
  err = js_get_env_loop(env, &loop);
  assert(err == 0);

  err = uv_tcp_init(loop, &t->handle);

  if (err < 0) {
    free(t->write_data);
    free(t);

    err = js_throw_error(env, uv_err_name(err), uv_strerror(err));
    assert(err == 0);

    return NULL;
  }

  t->handle.data = t;

  err = uv_timer_init(loop, &t->timer);
  assert(err == 0); // per libuv docs, only fails on a NULL loop/handle

  t->timer.data = t;

  err = js_add_deferred_teardown_callback(env, bare_tcp_cat_on_teardown, (void *) t, &t->teardown);
  assert(err == 0);

  js_value_t *promise;
  err = js_create_promise(env, &t->deferred, &promise);
  assert(err == 0);

  struct sockaddr_storage addr;

  int addr_err = uv_ip4_addr((char *) host, (int) port, (struct sockaddr_in *) &addr);

  if (addr_err < 0) {
    addr_err = uv_ip6_addr((char *) host, (int) port, (struct sockaddr_in6 *) &addr);
  }

  if (addr_err < 0) {
    bare_tcp_cat_set_error_msg(t, "EINVAL", "host must be a valid IPv4 or IPv6 address");
    bare_tcp_cat_close(t);

    return promise;
  }

  t->connect_req.data = t;

  err = uv_tcp_connect(&t->connect_req, &t->handle, (struct sockaddr *) &addr, bare_tcp_cat_on_connect);

  if (err < 0) {
    bare_tcp_cat_set_error(t, err);
    bare_tcp_cat_close(t);

    return promise;
  }

  if (timeout_ms > 0) {
    err = uv_timer_start(&t->timer, bare_tcp_cat_on_timeout, timeout_ms, 0);
    assert(err == 0);
  }

  return promise;
}

static js_value_t *
bare_tcp_cat_exports(js_env_t *env, js_value_t *exports) {
  int err;

#define V(name, fn) \
  { \
    js_value_t *val; \
    err = js_create_function(env, name, -1, fn, NULL, &val); \
    assert(err == 0); \
    err = js_set_named_property(env, exports, name, val); \
    assert(err == 0); \
  }

  V("tcpCat", bare_tcp_cat)
#undef V

  return exports;
}

BARE_MODULE(bare_addon_exercise, bare_tcp_cat_exports)
