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
package com.arcadedb.server.ha.raft;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8686: the cached "every peer can read the tx-prepared-at-index section" answer must be served within its TTL, recomputed
 * after it, and never outlive a membership change - not even when the change lands while the answer is being computed.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8686TxPreparedAtCapabilityCacheTest {

  private final AtomicLong    clock    = new AtomicLong(1_000_000L);
  private final AtomicBoolean answer   = new AtomicBoolean(true);
  private final AtomicInteger computed = new AtomicInteger();

  private TxPreparedAtCapabilityCache cache() {
    return new TxPreparedAtCapabilityCache(() -> {
      computed.incrementAndGet();
      return answer.get();
    }, clock::get, 1_000L);
  }

  @Test
  void theAnswerIsServedWithinTheTtlAndRecomputedAfterIt() {
    final TxPreparedAtCapabilityCache cache = cache();

    assertThat(cache.isCapable()).isTrue();
    assertThat(cache.isCapable()).isTrue();
    assertThat(computed.get()).as("one computation for the two commits").isEqualTo(1);

    answer.set(false);
    clock.addAndGet(500);
    assertThat(cache.isCapable()).as("still the cached answer").isTrue();

    clock.addAndGet(600);
    assertThat(cache.isCapable()).as("expired, so asked again").isFalse();
    assertThat(computed.get()).isEqualTo(2);
  }

  @Test
  void aMembershipChangeDropsTheAnswerAtOnce() {
    final TxPreparedAtCapabilityCache cache = cache();
    assertThat(cache.isCapable()).isTrue();

    answer.set(false);
    cache.invalidate();

    assertThat(cache.isCapable()).as("a peer that just joined is asked again, not served the old yes").isFalse();
  }

  /** The race: the membership changes while the answer is being computed under the old one. */
  @Test
  void anAnswerComputedAcrossAMembershipChangeIsNotServedAgain() {
    final TxPreparedAtCapabilityCache[] holder = new TxPreparedAtCapabilityCache[1];
    final AtomicBoolean firstCall = new AtomicBoolean(true);
    holder[0] = new TxPreparedAtCapabilityCache(() -> {
      computed.incrementAndGet();
      final boolean old = answer.get();
      if (firstCall.getAndSet(false)) {
        holder[0].invalidate();
        answer.set(false);
      }
      return old;
    }, clock::get, 1_000L);

    assertThat(holder[0].isCapable()).as("this caller gets the answer it computed").isTrue();
    assertThat(holder[0].isCapable()).as("but the next one must not be served it: it predates the change").isFalse();
    assertThat(computed.get()).isEqualTo(2);
  }
}
