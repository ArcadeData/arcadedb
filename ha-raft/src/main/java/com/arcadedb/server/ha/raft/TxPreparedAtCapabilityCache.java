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

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;

/**
 * The per-commit answer to "may a transaction state the index it was prepared at" (issue #8686), cached for a short TTL because it
 * is asked on every commit and computing it walks the peers.
 * <p>
 * Every answer carries the membership epoch it was computed under, and a reader only accepts a cached answer whose epoch is still
 * the current one. So a membership change invalidates it by bumping the epoch, and an answer that was being computed while that
 * happened is born stale and is never served - there is no window between "compare the epoch" and "store the answer" to lose an
 * invalidation in.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class TxPreparedAtCapabilityCache {
  private record Answer(boolean capable, long computedAt, long epoch) {
  }

  private final BooleanSupplier      compute;
  private final LongSupplier         clock;
  private final long                 ttlMs;
  private final AtomicLong           epoch  = new AtomicLong();
  private volatile Answer            answer = new Answer(false, Long.MIN_VALUE / 2, -1L);

  /**
   * @param compute asks every peer; called only when the cached answer is missing, expired or from another epoch
   * @param clock   milliseconds, injectable for tests
   * @param ttlMs   how long an answer is served
   */
  TxPreparedAtCapabilityCache(final BooleanSupplier compute, final LongSupplier clock, final long ttlMs) {
    this.compute = compute;
    this.clock = clock;
    this.ttlMs = ttlMs;
  }

  boolean isCapable() {
    final long now = clock.getAsLong();
    final long currentEpoch = epoch.get();
    final Answer cached = answer;
    if (cached.epoch() == currentEpoch && now - cached.computedAt() <= ttlMs)
      return cached.capable();

    final boolean capable = compute.getAsBoolean();
    answer = new Answer(capable, now, currentEpoch);
    return capable;
  }

  /** The membership changed: every answer computed so far, and any still being computed, stops being served. */
  void invalidate() {
    epoch.incrementAndGet();
  }
}
