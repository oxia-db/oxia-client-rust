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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "../oxia_client_ffi.h"

int main() {
    printf("Initializing Oxia client...\n");

    // 1. Prepare client options
    COxiaClientOptions options;
    options.service_address = "http://127.0.0.1:6648";
    options.namespace_ = "default";

    // 2. Create the client
    OxiaClient* client = NULL;
    COxiaError err_new = oxia_client_new(options, &client);
    if (err_new != Ok) {
        printf("Failed to create client. Error code: %d\n", err_new);
        return 1;
    }
    printf("Client created successfully.\n");

    // 3. Put multiple keys
    const char* keys[] = {"example/key1", "example/key2", "example/key3"};
    const char* values[] = {"hello", "world", "oxia"};
    for (int i = 0; i < 3; i++) {
        COxiaPutResult* put_result = NULL;
        COxiaError err_put = oxia_client_put(client, keys[i],
            (const uint8_t*)values[i], strlen(values[i]), &put_result);
        if (err_put != Ok) {
            printf("Put failed for %s. Error code: %d\n", keys[i], err_put);
        } else {
            printf("Put successful. Key: %s, Version ID: %lld\n",
                   put_result->key, put_result->version_id);
            oxia_put_result_free(put_result);
        }
    }

    // 4. Get a key
    COxiaGetResult* get_result = NULL;
    COxiaError err_get = oxia_client_get(client, "example/key1", &get_result);
    if (err_get != Ok) {
        printf("Get failed. Error code: %d\n", err_get);
    } else {
        printf("Get successful. Key: %s, Value: %.*s, Version ID: %lld\n",
               get_result->key, (int)get_result->value_len,
               (const char*)get_result->value, get_result->version_id);
        oxia_get_result_free(get_result);
    }

    // 5. List keys in range
    COxiaListResult* list_result = NULL;
    COxiaError err_list = oxia_client_list(client, "example/", "example/~", &list_result);
    if (err_list != Ok) {
        printf("List failed. Error code: %d\n", err_list);
    } else {
        printf("List successful. Found %zu keys:\n", list_result->keys_len);
        for (size_t i = 0; i < list_result->keys_len; i++) {
            printf("  - %s\n", list_result->keys[i]);
        }
        oxia_list_result_free(list_result);
    }

    // 6. Delete a single key
    COxiaError err_del = oxia_client_delete(client, "example/key1");
    if (err_del != Ok) {
        printf("Delete failed. Error code: %d\n", err_del);
    } else {
        printf("Delete successful for example/key1\n");
    }

    // 7. Delete range
    COxiaError err_del_range = oxia_client_delete_range(client, "example/", "example/~");
    if (err_del_range != Ok) {
        printf("Delete range failed. Error code: %d\n", err_del_range);
    } else {
        printf("Delete range successful for example/*\n");
    }

    // 8. Shutdown the client
    COxiaError err_shutdown = oxia_client_shutdown(client);
    if (err_shutdown != Ok) {
        printf("Client shutdown failed. Error code: %d\n", err_shutdown);
    } else {
        printf("Client shutdown successfully.\n");
    }

    return 0;
}
