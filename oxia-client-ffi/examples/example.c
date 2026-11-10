// Copyright 2025-2026 The Oxia Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// A tour of the C ABI: the blocking form, the options, the callback form and
// the error handling. Run an Oxia standalone server first:
//
//     docker run --rm -p 6648:6648 oxia/oxia:nightly-beta oxia standalone
//
// then build as described in README.md and run ./example [service_address].

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "oxia_client_ffi.h"

// Counts the callbacks that have come back, so main can wait for them.
// Plain C has no portable condition variable, so this polls; a real wrapper
// would signal whatever its own runtime waits on.
#include <stdatomic.h>
static atomic_int outstanding;

#define SLICE(literal) \
    ((oxia_slice){ (const uint8_t *)(literal), sizeof(literal) - 1 })

#define BYTES(pointer, length) ((oxia_slice){ (const uint8_t *)(pointer), (length) })

// Reports a failure and frees the message. Returns the code, so a caller can
// write `if (failed("put", code, message)) return 1;`.
static int failed(const char *what, oxia_error_code code, char *message) {
    if (code == OXIA_OK) {
        return 0;
    }
    fprintf(stderr, "%s failed: code %d (%s)%s\n", what, (int)code,
            message ? message : "no detail",
            oxia_error_is_retryable(code) ? ", retryable" : "");
    oxia_string_free(message);
    return 1;
}

static void print_version(const char *what, const oxia_version *version) {
    printf("  %s: version_id=%lld modifications=%lld created=%llu modified=%llu",
           what, (long long)version->version_id,
           (long long)version->modifications_count,
           (unsigned long long)version->created_timestamp,
           (unsigned long long)version->modified_timestamp);
    if (version->is_ephemeral) {
        printf(" ephemeral session=%lld identity=%s", (long long)version->session_id,
               version->client_identity.data ? (const char *)version->client_identity.data
                                             : "(none)");
    }
    printf("\n");
}

// ---------------------------------------------------------------------------
// Callbacks. These run on the client's dispatcher thread, one at a time. The
// result is borrowed: it is freed as soon as the callback returns, so
// anything to keep has to be copied out.
// ---------------------------------------------------------------------------

static void on_put(oxia_error_code code, const char *message,
                   const oxia_put_result *result, void *ctx) {
    const char *label = (const char *)ctx;
    if (code == OXIA_OK) {
        printf("  async put %s stored %.*s at version %lld\n", label,
               (int)result->key.len, (const char *)result->key.data,
               (long long)result->version.version_id);
    } else {
        fprintf(stderr, "  async put %s failed: %s\n", label,
                message ? message : "no detail");
    }
    atomic_fetch_sub(&outstanding, 1);
}

int main(int argc, char **argv) {
    const char *address = argc > 1 ? argv[1] : "localhost:6648";

    // 0. A caller that links the library at run time rather than building it
    //    must check that it speaks the ABI this header describes.
    if (oxia_abi_version() != OXIA_ABI_VERSION) {
        fprintf(stderr, "liboxia_client_ffi is ABI %u, this build expects %u\n",
                oxia_abi_version(), OXIA_ABI_VERSION);
        return 1;
    }

    // 1. Connect. A zeroed options struct means "every default"; only
    //    service_address is required.
    oxia_client_options client_options = {0};
    client_options.service_address = BYTES(address, strlen(address));
    client_options.namespace_ = SLICE("default");
    client_options.identity = SLICE("c-example");
    client_options.request_timeout_ms = 10000;

    oxia_client *client = NULL;
    char *message = NULL;
    oxia_error_code code = oxia_client_new(&client_options, &client, &message);
    if (failed("connect", code, message)) {
        return 1;
    }
    char *library_version = NULL;
    oxia_version_string(&library_version);
    printf("connected to %s with oxia-client-ffi %s\n", address, library_version);
    oxia_string_free(library_version);

    // 2. A plain put, then the same key with a compare-and-swap.
    oxia_put_options put_options = {0};
    put_options.key = SLICE("example/greeting");
    put_options.value = SLICE("hello");

    oxia_put_result *put_result = NULL;
    message = NULL;
    code = oxia_client_put(client, &put_options, &put_result, &message);
    if (failed("put", code, message)) {
        oxia_client_free(client);
        return 1;
    }
    printf("put example/greeting\n");
    print_version("stored", &put_result->version);
    int64_t version_id = put_result->version.version_id;
    oxia_put_result_free(put_result);

    // The conditional write only applies if nobody else has written since.
    put_options.value = SLICE("hello again");
    put_options.has_expected_version_id = true;
    put_options.expected_version_id = version_id;
    put_result = NULL;
    message = NULL;
    code = oxia_client_put(client, &put_options, &put_result, &message);
    printf("conditional put: %s\n", code == OXIA_OK ? "applied" : "rejected");
    if (code == OXIA_OK) {
        oxia_put_result_free(put_result);
    } else {
        oxia_string_free(message);
    }

    // 3. A get. A value is bytes, and `has_value` tells an empty value apart
    //    from a record that carries none.
    oxia_get_options get_options = {0};
    get_options.key = SLICE("example/greeting");
    oxia_get_result *record = NULL;
    message = NULL;
    code = oxia_client_get(client, &get_options, &record, &message);
    if (failed("get", code, message)) {
        oxia_client_free(client);
        return 1;
    }
    printf("get example/greeting -> %s (%zu bytes)\n",
           record->has_value ? (const char *)record->value.data : "(no value)",
           record->value.len);
    print_version("read", &record->version);
    oxia_get_result_free(record);

    // 4. A binary value, with the NUL bytes a C string could not carry.
    const unsigned char blob[] = { 0x00, 0x01, 0xff, 0x00, 0x7f };
    oxia_put_options blob_options = {0};
    blob_options.key = SLICE("example/blob");
    blob_options.value = BYTES(blob, sizeof blob);
    put_result = NULL;
    message = NULL;
    code = oxia_client_put(client, &blob_options, &put_result, &message);
    if (failed("put blob", code, message)) {
        oxia_client_free(client);
        return 1;
    }
    oxia_put_result_free(put_result);
    get_options.key = SLICE("example/blob");
    record = NULL;
    if (oxia_client_get(client, &get_options, &record, NULL) == OXIA_OK) {
        printf("get example/blob -> %zu bytes:", record->value.len);
        for (size_t i = 0; i < record->value.len; i++) {
            printf(" %02x", record->value.data[i]);
        }
        printf("\n");
        oxia_get_result_free(record);
    }

    // 5. Three puts submitted at once. Each callback runs exactly once, so
    //    the counter reaches zero.
    atomic_store(&outstanding, 3);
    static const char *labels[] = { "one", "two", "three" };
    oxia_put_options async_options = {0};
    async_options.value = SLICE("v");
    const char *keys[] = { "example/async-1", "example/async-2", "example/async-3" };
    for (int i = 0; i < 3; i++) {
        async_options.key = BYTES(keys[i], strlen(keys[i]));
        message = NULL;
        code = oxia_client_put_async(client, &async_options, on_put,
                                     (void *)labels[i], &message);
        if (failed("submit async put", code, message)) {
            atomic_fetch_sub(&outstanding, 1);
        }
    }
    printf("submitted 3 asynchronous puts\n");
    while (atomic_load(&outstanding) > 0) {
        // Busy-wait; see the note on `outstanding` above.
    }

    // 6. List the keys written so far, then scan them with their values.
    // The range bounds sit at the same depth as the keys: Oxia's default key
    // sorting is hierarchical, so keys with fewer `/` sort first and a bound
    // at another depth would not cover them.
    oxia_range_options range = {0};
    range.min_key_inclusive = SLICE("example/");
    range.max_key_exclusive = SLICE("example/~");
    oxia_key_list *keys_found = NULL;
    message = NULL;
    code = oxia_client_list(client, &range, &keys_found, &message);
    if (failed("list", code, message)) {
        oxia_client_free(client);
        return 1;
    }
    printf("list example/ -> %zu keys\n", keys_found->len);
    for (size_t i = 0; i < keys_found->len; i++) {
        printf("  %.*s\n", (int)keys_found->keys[i].len,
               (const char *)keys_found->keys[i].data);
    }
    oxia_key_list_free(keys_found);

    oxia_record_list *records = NULL;
    message = NULL;
    code = oxia_client_range_scan(client, &range, &records, &message);
    if (failed("range scan", code, message)) {
        oxia_client_free(client);
        return 1;
    }
    printf("range scan example/ -> %zu records\n", records->len);
    oxia_record_list_free(records);

    // 7. A subscription. Notifications are namespace-wide, so this filters
    //    for the key it just wrote; a real consumer would act on all of them.
    oxia_notifications_options watch_options = {0};
    oxia_notifications *watcher = NULL;
    message = NULL;
    code = oxia_client_notifications(client, &watch_options, &watcher, &message);
    if (failed("subscribe", code, message)) {
        oxia_client_free(client);
        return 1;
    }
    oxia_put_options watched = {0};
    watched.key = SLICE("example/watched");
    watched.value = SLICE("v");
    put_result = NULL;
    if (oxia_client_put(client, &watched, &put_result, NULL) == OXIA_OK) {
        oxia_put_result_free(put_result);
    }
    for (int seen = 0; seen < 1;) {
        oxia_notification *event = NULL;
        message = NULL;
        // A 2 s wait rather than 0, so a lost notification cannot hang this
        // example for good.
        code = oxia_notifications_next(watcher, 2000, &event, &message);
        if (code == OXIA_ERR_TIMEOUT) {
            printf("no notification within 2s; giving up\n");
            break;
        }
        if (code == OXIA_STREAM_END) {
            printf("the subscription ended\n");
            break;
        }
        if (failed("notifications_next", code, message)) {
            break;
        }
        if (event->key.len == strlen("example/watched")
            && memcmp(event->key.data, "example/watched", event->key.len) == 0) {
            printf("notification: type %d for %.*s", (int)event->type_,
                   (int)event->key.len, (const char *)event->key.data);
            if (event->has_version_id) {
                printf(" at version %lld", (long long)event->version_id);
            }
            printf("\n");
            seen++;
        }
        oxia_notification_free(event);
    }
    // Closing ends the subscription; freeing releases the handle. Both are
    // needed, and in this order.
    oxia_notifications_close(watcher);
    oxia_notifications_free(watcher);

    // 8. The same list as above, pulled one key at a time. Use this when the
    //    range is too large to hold in memory; `oxia_client_list` is faster
    //    when it fits.
    oxia_key_stream *keys_stream = NULL;
    message = NULL;
    code = oxia_client_list_stream(client, &range, &keys_stream, &message);
    if (failed("list stream", code, message)) {
        oxia_client_free(client);
        return 1;
    }
    printf("streamed list of example/:\n");
    for (;;) {
        oxia_bytes key = {0};
        message = NULL;
        code = oxia_key_stream_next(keys_stream, 10000, &key, &message);
        if (code == OXIA_STREAM_END) {
            break;
        }
        if (failed("key_stream_next", code, message)) {
            break;
        }
        printf("  %.*s\n", (int)key.len, (const char *)key.data);
        oxia_bytes_free(key);
    }
    oxia_key_stream_free(keys_stream);

    // 9. A key that is not there is a code, not a crash.
    get_options.key = SLICE("example/absent");
    record = NULL;
    message = NULL;
    code = oxia_client_get(client, &get_options, &record, &message);
    printf("get example/absent -> code %d (%s)\n", (int)code,
           message ? message : "no detail");
    oxia_string_free(message);

    // 10. Clean up the keys, then close and free. Closing is optional —
    //    freeing closes too — but only closing can report a failure to
    //    flush.
    oxia_delete_range_options wipe = {0};
    wipe.min_key_inclusive = SLICE("example/");
    wipe.max_key_exclusive = SLICE("example/~");
    message = NULL;
    code = oxia_client_delete_range(client, &wipe, &message);
    if (failed("delete range", code, message)) {
        oxia_client_free(client);
        return 1;
    }
    printf("deleted example/*\n");

    message = NULL;
    code = oxia_client_close(client, &message);
    if (failed("close", code, message)) {
        oxia_client_free(client);
        return 1;
    }
    oxia_client_free(client);
    printf("closed\n");
    return 0;
}
