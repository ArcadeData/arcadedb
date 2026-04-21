/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.server.http;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class IdempotencyCacheTest {

  @Test
  void putAndGetSuccess() {
    final IdempotencyCache cache = new IdempotencyCache(60_000, 100);
    cache.putSuccess("req-1", 200, "{\"result\":\"ok\"}", null, "alice");

    final IdempotencyCache.CachedEntry entry = cache.get("req-1");
    assertThat(entry).isNotNull();
    assertThat(entry.statusCode).isEqualTo(200);
    assertThat(entry.body).isEqualTo("{\"result\":\"ok\"}");
    assertThat(entry.principal).isEqualTo("alice");
    assertThat(entry.binary).isNull();
  }

  @Test
  void non2xxNotCached() {
    final IdempotencyCache cache = new IdempotencyCache(60_000, 100);
    cache.putSuccess("req-2", 400, "Bad Request", null, "alice");
    assertThat(cache.get("req-2")).isNull();

    cache.putSuccess("req-3", 500, "Internal Error", null, "alice");
    assertThat(cache.get("req-3")).isNull();

    cache.putSuccess("req-4", 301, "Redirect", null, "alice");
    assertThat(cache.get("req-4")).isNull();
  }

  @Test
  void nullRequestIdIgnored() {
    final IdempotencyCache cache = new IdempotencyCache(60_000, 100);
    cache.putSuccess(null, 200, "body", null, "alice");
    assertThat(cache.size()).isEqualTo(0);

    assertThat(cache.get(null)).isNull();
    assertThat(cache.get("")).isNull();
  }

  @Test
  void ttlExpiration() throws Exception {
    final IdempotencyCache cache = new IdempotencyCache(50, 100);
    cache.putSuccess("req-5", 200, "body", null, "alice");
    assertThat(cache.get("req-5")).isNotNull();

    Thread.sleep(100);
    assertThat(cache.get("req-5")).isNull();
  }

  @Test
  void maxSizeEviction() {
    final IdempotencyCache cache = new IdempotencyCache(60_000, 3);
    cache.putSuccess("req-a", 200, "a", null, "alice");
    cache.putSuccess("req-b", 200, "b", null, "alice");
    cache.putSuccess("req-c", 200, "c", null, "alice");
    assertThat(cache.size()).isEqualTo(3);

    // adding a 4th entry should evict one
    cache.putSuccess("req-d", 200, "d", null, "alice");
    assertThat(cache.size()).isEqualTo(3);
  }

  @Test
  void cleanupExpired() throws Exception {
    final IdempotencyCache cache = new IdempotencyCache(50, 100);
    cache.putSuccess("req-x", 200, "x", null, "alice");
    cache.putSuccess("req-y", 200, "y", null, "alice");
    assertThat(cache.size()).isEqualTo(2);

    Thread.sleep(100);
    cache.cleanupExpired();
    assertThat(cache.size()).isEqualTo(0);
  }

  @Test
  void binaryResponseCached() {
    final IdempotencyCache cache = new IdempotencyCache(60_000, 100);
    final byte[] data = new byte[] { 1, 2, 3 };
    cache.putSuccess("req-bin", 200, null, data, "bob");

    final IdempotencyCache.CachedEntry entry = cache.get("req-bin");
    assertThat(entry).isNotNull();
    assertThat(entry.binary).isEqualTo(data);
    assertThat(entry.body).isNull();
  }

  @Test
  void headerConstant() {
    assertThat(IdempotencyCache.HEADER_REQUEST_ID).isEqualTo("X-Request-Id");
  }
}
