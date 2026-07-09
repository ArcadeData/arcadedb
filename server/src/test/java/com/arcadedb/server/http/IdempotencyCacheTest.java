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

  @Test
  void reserveDedupsConcurrentRetries() {
    final IdempotencyCache cache = new IdempotencyCache(60_000, 100);

    // First caller owns execution.
    final IdempotencyCache.Reservation first = cache.reserve("k");
    assertThat(first.isReserved()).isTrue();
    assertThat(first.isHit()).isFalse();
    assertThat(first.isInFlight()).isFalse();

    // A concurrent identical retry does NOT get a reservation: it sees the in-flight marker instead, so it
    // will not execute the write a second time.
    final IdempotencyCache.Reservation second = cache.reserve("k");
    assertThat(second.isReserved()).isFalse();
    assertThat(second.isInFlight()).isTrue();
    assertThat(second.entry()).isNotNull();

    // While pending, get() must not surface the incomplete marker as a replayable response.
    assertThat(cache.get("k")).isNull();

    // Winner settles the reservation; the retry can now replay the completed response.
    cache.complete("k", 200, "{\"ok\":true}", null, "alice");
    final IdempotencyCache.Reservation third = cache.reserve("k");
    assertThat(third.isHit()).isTrue();
    assertThat(third.entry().body).isEqualTo("{\"ok\":true}");
    assertThat(cache.get("k")).isNotNull();
  }

  @Test
  void awaitReleasedByComplete() throws Exception {
    final IdempotencyCache cache = new IdempotencyCache(60_000, 100);
    final IdempotencyCache.Reservation owner = cache.reserve("k");
    assertThat(owner.isReserved()).isTrue();

    final IdempotencyCache.Reservation waiter = cache.reserve("k");
    assertThat(waiter.isInFlight()).isTrue();

    final Thread completer = new Thread(() -> {
      try {
        Thread.sleep(50);
      } catch (final InterruptedException ignore) {
        Thread.currentThread().interrupt();
      }
      cache.complete("k", 200, "done", null, "alice");
    });
    completer.start();

    assertThat(waiter.entry().await(5_000)).isTrue();
    completer.join();
    assertThat(cache.get("k")).isNotNull();
    assertThat(cache.get("k").body).isEqualTo("done");
  }

  @Test
  void abortClearsReservationAndReleasesWaiters() {
    final IdempotencyCache cache = new IdempotencyCache(60_000, 100);
    final IdempotencyCache.Reservation owner = cache.reserve("k");
    assertThat(owner.isReserved()).isTrue();

    final IdempotencyCache.Reservation waiter = cache.reserve("k");
    assertThat(waiter.isInFlight()).isTrue();

    cache.abort("k");
    // Waiter is released immediately even though nothing was cached.
    assertThat(waiter.entry().await(5_000)).isTrue();
    assertThat(cache.get("k")).isNull();

    // After an abort the key is free again: a fresh retry can reserve it.
    assertThat(cache.reserve("k").isReserved()).isTrue();
  }

  @Test
  void completeIgnoresNon2xx() {
    final IdempotencyCache cache = new IdempotencyCache(60_000, 100);
    assertThat(cache.reserve("k").isReserved()).isTrue();
    cache.complete("k", 500, "boom", null, "alice");
    assertThat(cache.get("k")).isNull();
    // The marker is gone, so a retry may execute again.
    assertThat(cache.reserve("k").isReserved()).isTrue();
  }

  @Test
  void byteBoundEvictsOldestEntries() {
    // maxEntries is large; the 10-byte total budget is the binding constraint.
    final IdempotencyCache cache = new IdempotencyCache(60_000, 1_000, 10, 1_000);
    cache.putSuccess("a", 200, "12345", null, "u"); // 5 bytes
    cache.putSuccess("b", 200, "12345", null, "u"); // +5 = 10 (at limit)
    assertThat(cache.size()).isEqualTo(2);
    assertThat(cache.bytes()).isEqualTo(10);

    cache.putSuccess("c", 200, "12345", null, "u"); // would be 15 -> evict oldest until <= 10
    assertThat(cache.bytes()).isLessThanOrEqualTo(10);
    assertThat(cache.get("a")).isNull();       // oldest evicted
    assertThat(cache.get("c")).isNotNull();    // newest retained
  }

  @Test
  void perBodyCapSkipsOversizedResponse() {
    final IdempotencyCache cache = new IdempotencyCache(60_000, 1_000, 1_000_000, 4);
    cache.putSuccess("big", 200, "12345", null, "u"); // 5 bytes > 4 -> not cached
    assertThat(cache.get("big")).isNull();

    cache.putSuccess("ok", 200, "1234", null, "u");   // 4 bytes -> cached
    assertThat(cache.get("ok")).isNotNull();
  }

  @Test
  void completeRetainsBinaryBodyForReplay() {
    final IdempotencyCache cache = new IdempotencyCache(60_000, 100);
    assertThat(cache.reserve("bin").isReserved()).isTrue();
    final byte[] data = new byte[] { 9, 8, 7, 6 };
    cache.complete("bin", 200, null, data, "bob");

    final IdempotencyCache.CachedEntry entry = cache.get("bin");
    assertThat(entry).isNotNull();
    assertThat(entry.binary).isEqualTo(data);
    assertThat(entry.body).isNull();
  }

  @Test
  void cleanupExpiredStopsAtFirstLiveEntry() throws Exception {
    final IdempotencyCache cache = new IdempotencyCache(80, 100);
    cache.putSuccess("old", 200, "old", null, "u");
    Thread.sleep(100); // "old" now past its 80ms TTL
    cache.putSuccess("fresh", 200, "fresh", null, "u");

    cache.cleanupExpired();
    assertThat(cache.get("old")).isNull();
    assertThat(cache.get("fresh")).isNotNull();
  }

  @Test
  void reserveAfterCompleteReturnsHitWithinTtl() {
    final IdempotencyCache cache = new IdempotencyCache(60_000, 100);
    cache.putSuccess("k", 200, "body", null, "alice");
    final IdempotencyCache.Reservation r = cache.reserve("k");
    assertThat(r.isHit()).isTrue();
    assertThat(r.isReserved()).isFalse();
  }
}
