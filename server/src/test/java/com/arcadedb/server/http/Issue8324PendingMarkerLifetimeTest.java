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

/**
 * Issue #8324 at the cache level: a PENDING marker - the reservation of a request that is still executing - must
 * outlive the TTL and the entry-count eviction. Either one used to drop it, which released every waiter and let the
 * next identical retry reserve the key and run the request a second time next to the first. A request that
 * outlives the 60 s default TTL, such as a long {@code restore database} behind a follower's 504, is the one a
 * client is most likely to retry.
 */
class Issue8324PendingMarkerLifetimeTest {

  @Test
  void aPendingMarkerOutlivesTheTtl() throws Exception {
    final IdempotencyCache cache = new IdempotencyCache(50, 100);
    final IdempotencyCache.Reservation owner = cache.reserve("restore");
    assertThat(owner.isReserved()).isTrue();

    Thread.sleep(120); // well past the 50 ms TTL, the owner still executing

    final IdempotencyCache.Reservation retry = cache.reserve("restore");
    assertThat(retry.isInFlight())
        .as("a retry arriving after the TTL must still see the first execution in flight, not reserve a second one")
        .isTrue();
    assertThat(retry.isReserved()).isFalse();
    assertThat(retry.entry().await(10))
        .as("and must not have been released by the expiry")
        .isFalse();

    // The owner settles normally, and the retry after it replays the answer.
    cache.complete("restore", owner, 200, "{\"result\":\"ok\"}", null, "root");
    final IdempotencyCache.Reservation afterwards = cache.reserve("restore");
    assertThat(afterwards.isHit()).isTrue();
    assertThat(afterwards.entry().body).isEqualTo("{\"result\":\"ok\"}");
  }

  @Test
  void theCleanupSweepLeavesAPendingMarkerInPlace() throws Exception {
    final IdempotencyCache cache = new IdempotencyCache(50, 100);
    final IdempotencyCache.Reservation owner = cache.reserve("restore");
    cache.putSuccess("done", 200, "x", null, "root");

    Thread.sleep(120);
    cache.cleanupExpired();

    assertThat(cache.size()).as("the completed entry expires, the pending marker does not").isEqualTo(1);
    assertThat(cache.get("done")).isNull();
    final IdempotencyCache.Reservation retry = cache.reserve("restore");
    assertThat(retry.isInFlight()).isTrue();
    assertThat(retry.entry().await(10)).isFalse();

    cache.abort("restore", owner);
    assertThat(cache.size()).isZero();
  }

  @Test
  void aCleanupSweepStepsOverAPendingMarkerToExpireTheEntriesBehindIt() throws Exception {
    final IdempotencyCache cache = new IdempotencyCache(50, 100);
    cache.reserve("restore");      // oldest: at the head of the insertion order
    cache.putSuccess("done", 200, "x", null, "root");

    Thread.sleep(120);
    cache.cleanupExpired();

    assertThat(cache.get("done")).as("the sweep must not stop at the pending head").isNull();
    assertThat(cache.size()).isEqualTo(1);
  }

  @Test
  void theEntryCountEvictionNeverDropsAPendingMarker() {
    final IdempotencyCache cache = new IdempotencyCache(60_000, 2);
    final IdempotencyCache.Reservation owner = cache.reserve("restore");

    // Enough completed traffic to push the eldest entry - the pending marker - out several times over.
    for (int i = 0; i < 10; i++)
      cache.putSuccess("req-" + i, 200, "b", null, "root");

    final IdempotencyCache.Reservation retry = cache.reserve("restore");
    assertThat(retry.isInFlight())
        .as("eviction must step over a request that is still executing")
        .isTrue();
    assertThat(retry.entry().await(10)).isFalse();
    // The completed entries are still bounded: eviction removed the eldest completed one instead.
    assertThat(cache.size()).isEqualTo(2);
    assertThat(cache.get("req-9")).isNotNull();
    assertThat(cache.get("req-8")).isNull();

    cache.complete("restore", owner, 200, "ok", null, "root");
    assertThat(cache.get("restore")).isNotNull();
    assertThat(cache.size()).isEqualTo(2);
  }
}
