# oxia-client-ffi

Exposes [`oxia-client`](../oxia-client) to C, C++ and other languages through a stable C ABI: for
C and C++ applications directly, and as the core another language's Oxia SDK can be built on. The
generated header is [`oxia_client_ffi.h`](./oxia_client_ffi.h);
[`examples/example.c`](./examples/example.c) is a complete program.

Two words for two things, since the crate is named for one and these docs mostly talk about the
other. The *FFI* — foreign function interface — is the mechanism: calling into this library from
another language, which is why the crate exists and what it is named after. The *ABI* —
application binary interface — is the contract that makes doing so safe: the exported symbols,
the struct layouts, and the ownership, threading and error-reporting rules below. The mechanism
is unremarkable; the contract is the part worth reading, and the part that has to stay stable
once another SDK compiles against it.

This crate is not published to crates.io. It is built as a shared library (`cdylib`), a static
library (`staticlib`) for linking into another SDK's artifact, and an `rlib` so that the tests can
drive the ABI directly.

## The contract

Six rules cover the whole surface.

**Bytes, not C strings.** Keys and values cross as a pointer and a length — `oxia_slice` in,
`oxia_bytes` out. An Oxia key is a protobuf `string`, so it is UTF-8 but may contain a NUL byte,
and a value is arbitrary binary; neither fits a `char *`. A key that is not valid UTF-8 is
rejected with `OXIA_ERR_INVALID_ARGUMENT`. Buffers handed back are NUL-terminated one past their
length, so a key without interior NULs can still be printed with `%s`.

```c
oxia_slice key = { (const uint8_t *)"greeting", 8 };
```

**Zero means default.** Every options struct is designed so that `= {0}` asks for the plain
operation with the client's defaults. A caller sets only the fields it cares about, and a field
added in a later version keeps its default for code that does not know about it.

```c
oxia_put_options options = {0};
options.key = key;
options.value = (oxia_slice){ (const uint8_t *)"hello", 5 };
```

A slice is unset when its `data` is `NULL`; a non-`NULL` `data` with a `len` of 0 is the empty
string, which is how `partition_key` tells "no partition key" from "the empty one". Numeric
fields take the client's default at 0. The exception to "zero is fine" is a *required* string —
a key, a range bound, a sequence's partition key: leaving one unset is
`OXIA_ERR_INVALID_ARGUMENT` with a message naming the field, rather than a silently empty
string on the wire. `exclude_value` is phrased negatively for the same reason:
a zeroed get returns the value.

**A code, and a message if you want it.** Every fallible function returns an `oxia_error_code` and
takes a final `char **error_message`. Pass `NULL` to ignore the detail. Otherwise a non-`OXIA_OK`
return sets it to a message owned by the caller, freed with `oxia_string_free` — which ignores
`NULL`, so it is safe to call unconditionally.

```c
char *message = NULL;
if (oxia_client_put(client, &options, &result, &message) != OXIA_OK) {
    fprintf(stderr, "put failed: %s\n", message);
    oxia_string_free(message);
}
```

Codes are stable: a new one is appended rather than renumbering the others, and an error the
library does not recognize arrives as `OXIA_ERR_OTHER` with its text in the message.
`oxia_error_is_retryable` answers whether resubmitting the operation might succeed.

One thing the ABI cannot paper over: a record stored with an *empty* value reads back with
`has_value` false, the same as a record that carries no value, because the server leaves the field
out of the response in both cases. `has_value` still separates "the value is here" from "I asked
not to be sent it" (`exclude_value`), which is what it is for.

**Out-parameters are written only on success.** Every error path sets them to `NULL` first, so a
caller can free unconditionally and an uninitialized local is never mistaken for a result.

**Nothing aborts.** Null pointers, non-UTF-8 keys and slices whose `data` is `NULL` while `len` is
not all come back as `OXIA_ERR_INVALID_ARGUMENT` with a message naming the argument. A panic
inside the client is caught at the boundary and returned as `OXIA_ERR_PANIC`; letting it unwind
into the caller would end the process.

**One destructor.** `oxia_client_free` is the only call that invalidates a client, and it closes
the client gracefully first. `oxia_client_close` flushes and shuts down, is idempotent, and leaves
the handle valid — every later operation on it returns `OXIA_ERR_CLOSED` — so closing and then
freeing, which is what C code naturally writes, is the supported order. Close explicitly when you
want to know that the final flush succeeded; `oxia_client_free` cannot report that.

## Threads

A client may be used from any thread. Operations submitted from several threads run concurrently
and batch together. Each operation comes in two forms:

| | Returns | Result |
|---|---|---|
| `oxia_client_put` | when the server has answered | owned through the out-parameter, freed by the caller |
| `oxia_client_put_async` | as soon as the operation is accepted | borrowed, passed to a callback |

A callback runs on a dispatcher thread belonging to the client — never on the thread that
submitted the operation, and never on the thread the client itself runs on. So a callback may
block, and may make blocking Oxia calls, without stalling the client or deadlocking. In exchange:

- callbacks are invoked one at a time, in the order the operations completed;
- each is invoked exactly once for every submission that returned `OXIA_OK`, including when the
  client is freed with the operation still in flight, which arrives as `OXIA_ERR_CLOSED`;
- the result and the message are borrowed for the duration of the call and freed as soon as it
  returns, so anything to keep must be copied out;
- `ctx` is passed back untouched and must be usable from the dispatcher thread;
- do not call `oxia_client_free` from a callback: it waits for the dispatcher to drain.

Under the hood the native client is single-threaded — it lives on a thread of its own in a
[compio](https://github.com/compio-rs/compio) runtime — and the blocking forms hand work to that
thread and wait. A caller does not have to know that, beyond the callback rules above.

## Backpressure

`max_in_flight_bytes` caps the bytes the client holds for the operations in flight — their keys
and values — and defaults to 64 MiB. Both call forms are bounded by it, in the way each can be:

| Over the cap | |
|---|---|
| `oxia_client_put` | waits for room, so the call takes longer |
| `oxia_client_put_async` | returns `OXIA_ERR_QUEUE_FULL`, without accepting the operation |

The blocking form gets this for nothing: it runs the operation on the client's thread and parks
the calling thread until the result arrives, so waiting for room is part of the wait it already
does. A caller that outruns the cluster is simply slowed to the cluster's pace. The asynchronous
form has to answer *before* the operation has been admitted, so refusing is the only thing it can
do — queue it instead and the memory just moves out of the client and into this library, which is
what it did before this cap existed.

On `OXIA_ERR_QUEUE_FULL`, retry later. The operation was not accepted, so **its callback does not
run**: that is the same contract as every other refusal — a submission returning `OXIA_OK` calls
back exactly once, anything else does not call back at all — and it is what keeps the refusal safe
for a wrapper that allocates a context per operation. Every *accepted* operation does call back,
so a completion is always on its way to free room; there is no separate "you may submit again"
signal to wait for and none is offered. `oxia_error_is_retryable` answers `true` for the code, but
the retry belongs after a callback has run rather than immediately — resubmitting in a tight loop
only spins.

Two details worth knowing before setting the cap tightly. An asynchronous operation is charged
against it twice, over different windows: once by this library, from the submission until its
callback has returned, and once by the client itself, from the moment the operation is queued
until the server answers. The two layers cannot see each other's accounting — the client's
limiter lives on the client's thread and a submission happens on a foreign thread — so a cap of
*n* bounds an asynchronous flood to roughly 2*n* rather than exactly *n*. And only keys and
values are counted, as in the Rust client: the fixed cost of an operation, its task and its
callback context, is not, so a flood of tiny operations is bounded less tightly than a flood of
large ones.

`0` asks for the default rather than for no limit, as every numeric field in the options does, so
a caller that wants the client unbounded sets a very large value.

## Streams

Notifications, sequence updates and the streaming forms of `list` and `range_scan` are **pulled,
not pushed**. Each is an opaque handle with the same three calls:

```c
oxia_notifications *watcher = NULL;
oxia_client_notifications(client, &(oxia_notifications_options){0}, &watcher, NULL);

for (;;) {
    oxia_notification *event = NULL;
    oxia_error_code code = oxia_notifications_next(watcher, 1000, &event, NULL);
    if (code == OXIA_STREAM_END) break;        // finished
    if (code == OXIA_ERR_TIMEOUT) continue;    // nothing within 1 s; not an error
    if (code != OXIA_OK) break;                // a real failure
    handle(event);
    oxia_notification_free(event);
}
oxia_notifications_close(watcher);   // ends it
oxia_notifications_free(watcher);    // releases the handle
```

`next` asks the client's thread for one item and waits for it; nothing is taken from the stream
until a caller asks. That is the point: a pushed stream would need somewhere to put items a slow
consumer has not taken, and an unbounded queue there would throw away the backpressure the
subscription already applies through its `buffer_size`. Pulling makes the consumer set the pace,
and keeps at most one item in flight inside this library. The cost is a round trip to the
client's thread per item, so `oxia_client_list` is faster than `oxia_client_list_stream` for a
range that fits in memory; the streaming forms are for ranges that do not.

| | |
|---|---|
| `timeout_ms` of 0 | waits indefinitely |
| `OXIA_ERR_TIMEOUT` | nothing arrived in time. Not an error: the request stays outstanding, so the item it was waiting for reaches the next call rather than being dropped |
| `OXIA_STREAM_END` | the stream is finished — its own end, `close`, or the client being freed |
| `close` | ends the stream. Idempotent, safe from any thread, and safe to call while another thread is blocked in `next`, which then returns `OXIA_STREAM_END`. The handle stays valid |
| `free` | releases the handle. No call on it may be in progress on another thread: `close` first, let `next` return, then free |

One thread at a time per handle; a second caller in `next` blocks until the first returns.

The handles and their items:

| Open with | Handle | `next` yields | Free the item with |
|---|---|---|---|
| `oxia_client_notifications` | `oxia_notifications` | `oxia_notification *` | `oxia_notification_free` |
| `oxia_client_sequence_updates` | `oxia_key_stream` | `oxia_bytes` | `oxia_bytes_free` |
| `oxia_client_list_stream` | `oxia_key_stream` | `oxia_bytes` | `oxia_bytes_free` |
| `oxia_client_range_scan_stream` | `oxia_record_stream` | `oxia_get_result *` | `oxia_get_result_free` |

Notifications are namespace-wide: a subscription sees every change to the namespace, not only the
ones this client made, and it starts from the moment it is opened rather than replaying history.
`OXIA_NOTIFICATION_MISSED` says the server's retention dropped notifications before this
subscription read them, so any record may have changed unseen and a consumer keeping state has to
re-read it. A notification type this library does not recognize arrives as
`OXIA_NOTIFICATION_UNKNOWN` rather than being dropped.

## What is not here yet

Everything the Rust client can do is here — every operation, every per-operation option, the
subscriptions and streaming reads, the whole `oxia_version`, and the client configuration
including timeouts, identity, bearer-token authentication, TLS, the batching limits and the
memory cap.

`auth_token_provider` and `meter_provider` have no C equivalent. A rotating token would need a
callback and metrics would need an exporter handle; neither is exposed.

## Versioning

Two numbers, for two different jobs.

`OXIA_ABI_VERSION` is a constant in the header and `oxia_abi_version()` returns what the library
was built with. A caller that loads the library separately from the header it compiled against —
which is every wrapper SDK shipping a prebuilt binary — compares them before its first call:

```c
if (oxia_abi_version() != OXIA_ABI_VERSION) {
    fprintf(stderr, "liboxia_client_ffi is ABI %u, this build expects %u\n",
            oxia_abi_version(), OXIA_ABI_VERSION);
    return 1;
}
```

Exact equality, not "at least": while this ABI is pre-1.0 there is no compatibility window to
rely on. `oxia_version_string()` is the crate's own version, which moves for every release whether
the ABI changed or not; report it in diagnostics, do not gate on it.

What bumps `OXIA_ABI_VERSION`: a changed signature, a renumbered error code, a reordered struct
— and the one that is easy to miss, **a field appended to one of the caller-allocated options
structs**. The caller allocates those (`oxia_put_options options = {0}`), so a library that knows
about a longer struct would read past the end of what an older caller gave it. Version 2 is that
case: `max_in_flight_bytes` mirrored into `oxia_client_options` grew the struct, and the number
moved with it. A new error code does not, by itself — `OXIA_ERR_QUEUE_FULL` was appended in the
same version — since an older caller simply does not recognize it.

This is also why the release does not yet publish version-pinned binaries: the first one it
publishes is the one wrapper SDKs pin to.

## Building

```bash
cargo build -p oxia-client-ffi --release
```

That produces `liboxia_client_ffi.{so,dylib}` and `liboxia_client_ffi.a` under `target/release/`.
TLS is on by default; `--no-default-features` drops it, and then setting any of the `tls_` options
fails with `OXIA_ERR_INVALID_ARGUMENT`.

The header is checked in and regenerated with [cbindgen](https://github.com/mozilla/cbindgen)
whenever the ABI changes. CI fails if it is out of date:

```bash
cargo install cbindgen
cbindgen --config cbindgen.toml --crate oxia-client-ffi --output oxia-client-ffi/oxia_client_ffi.h
```

### The example

```bash
cargo build -p oxia-client-ffi
cmake -S oxia-client-ffi/examples -B oxia-client-ffi/examples/build
cmake --build oxia-client-ffi/examples/build
docker run --rm -d -p 6648:6648 oxia/oxia:nightly-beta oxia standalone
./oxia-client-ffi/examples/build/example
```

## Testing

`cargo test -p oxia-client-ffi` runs both suites:

- the unit tests, which cover the conversions and the argument checking with no server;
- `tests/it.rs`, which calls the `extern "C"` entry points with the same raw pointers a C caller
  passes, against an Oxia server in Docker (`OXIA_IMAGE` and `OXIA_TAG` override the image).

CI additionally compiles the header as C99, C11, C17 and as C++11, C++17, C++20, and builds the
example, so that a change which breaks either language is caught.
