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
package com.arcadedb.utility;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Exponential backoff with full jitter for transaction-retry delays (issue #5587).
 * <p>
 * Before this class existed, every retry slept a flat {@code U(1, cap)} interval regardless of how many times
 * it had already lost: attempt 1 and attempt 100 drew from the same distribution, so a transaction's odds of
 * winning the next race never improved no matter how long it had already waited. That is the wrong shape under
 * contention - backoff exists to thin the crowd for the transactions that have waited longest.
 * <p>
 * The window for attempt {@code n} (zero-based) is {@code min(cap, base * 2^n)}, and the actual sleep is drawn
 * uniformly from {@code [1, window]} - full jitter, not equal or decorrelated jitter, because the failure mode
 * here is synchronised re-entry (many threads losing the same page race and retrying together) and full jitter
 * minimises the odds of the next collision. The cap keeps a long retry loop from turning into a multi-second
 * stall.
 * <p>
 * Shared by {@link com.arcadedb.database.LocalDatabase#transaction} (the programmatic / embedded retry loop)
 * and {@link com.arcadedb.query.sql.executor.RetryStep} (the SQL {@code COMMIT RETRY} statement) so the two
 * copies of this policy cannot drift from each other.
 */
public class RetryBackoff {
  private RetryBackoff() {
  }

  /**
   * Computes the upper bound (inclusive) in milliseconds of the backoff window for the given zero-based
   * attempt: {@code min(cap, base * 2^attempt)}.
   * <p>
   * A non-positive {@code capMs} disables backoff entirely and returns {@code 0}, matching the pre-#5587
   * behaviour of a disabled retry delay. A non-positive {@code baseMs} falls back to {@code 1} ms rather than
   * never widening the window. A negative {@code attempt} is treated as attempt {@code 0}. The exponent shift
   * is clamped so a very large attempt count saturates at {@code capMs} instead of overflowing {@code long}
   * arithmetic into a negative or wrapped value.
   *
   * @param attempt zero-based count of retries already performed
   * @param baseMs  the window's starting size, before doubling
   * @param capMs   the window's maximum size, and the whole backoff's on/off switch
   *
   * @return the inclusive upper bound in milliseconds for this attempt's random sleep, or {@code 0} if backoff
   * is disabled
   */
  public static long windowMs(final int attempt, final long baseMs, final long capMs) {
    if (capMs <= 0)
      return 0;

    final long effectiveBase = Math.max(1, baseMs);
    final int shift = Math.min(Math.max(attempt, 0), 62); // 1L << 63 would overflow into a negative value
    final long widened = effectiveBase << shift;

    if (widened <= 0 || widened > capMs) // overflowed past Long.MAX_VALUE, or simply past the cap
      return capMs;

    return widened;
  }

  /**
   * Sleeps a random duration in {@code [1, windowMs(attempt, baseMs, capMs)]} milliseconds. Returns
   * immediately, without sleeping, when the window is {@code 0} (backoff disabled). Restores the thread's
   * interrupt flag and returns early if interrupted during the sleep.
   *
   * @param attempt zero-based count of retries already performed
   * @param baseMs  the window's starting size, before doubling
   * @param capMs   the window's maximum size, and the whole backoff's on/off switch
   */
  public static void sleep(final int attempt, final long baseMs, final long capMs) {
    final long window = windowMs(attempt, baseMs, capMs);
    if (window <= 0)
      return;

    try {
      Thread.sleep(1 + ThreadLocalRandom.current().nextLong(window));
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
