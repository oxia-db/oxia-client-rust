# oxia-client-ffi

A C ABI over [`oxia-client`](../oxia-client), for C and C++ applications and as the core
another language's Oxia SDK can be built on. The generated header is
[`oxia_client_ffi.h`](./oxia_client_ffi.h); [`examples/example.c`](./examples/example.c) is a
complete program.

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
fields take the client's default at 0. `exclude_value` is phrased negatively for the same reason:
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

## What is not here yet

Notifications, sequence updates and the streaming forms of `list` and `range_scan` are not
exposed: a stream needs an ABI of its own (an iterator handle, or a callback per item plus a
cancellation handle) and that is worth designing separately. Everything else the Rust client can
do is here — every operation, every per-operation option, the whole `oxia_version`, and the client
configuration including timeouts, identity, bearer-token authentication, TLS and the batching
limits.

`auth_token_provider` and `meter_provider` have no C equivalent. A rotating token would need a
callback and metrics would need an exporter handle; neither is exposed.

**The callback form has no backpressure.** Submitting is unbounded: the queue to the client's
thread, the set of operations in flight and the queue of completions waiting for the dispatcher
all grow as fast as a caller submits. The client's `max_write_batches_in_flight` and
`max_read_batches_in_flight` cap how many batches are outstanding per shard, so the bytes on the
wire are bounded, but operations past that limit simply wait — and the memory holding their keys,
values and callback contexts grows without limit. A caller that submits faster than the cluster
drains will run out of memory with no signal beforehand, and `oxia_client_free` will wait for all
of it to settle.

The blocking form does not have this problem: an operation in flight costs the caller a parked
thread, which caps concurrency by itself. The callback form exists to remove that cost, and
removes the flow control with it.

An in-flight limit — a count of accepted-but-undelivered operations, with a code for "the queue
is full" so a caller can apply its own flow control — is deliberately left until the Rust client's
own backpressure story is settled, so that this ABI mirrors it rather than inventing a second one.
Until then, a caller that submits without waiting should bound the work itself, by counting
submissions against completions.

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
