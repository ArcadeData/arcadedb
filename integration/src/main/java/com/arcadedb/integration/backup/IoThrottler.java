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
package com.arcadedb.integration.backup;

import java.util.concurrent.locks.LockSupport;

/**
 * Optional read-side rate limiter for the backup, so a backup of a large database cannot saturate the production disk.
 * The cap is expressed in MB/s and applies to the bytes read from the database files, which is the I/O that competes
 * with the live workload (the archive is normally written to a different device, and it is anyway much smaller).
 * <p>
 * Implemented as an absolute-deadline limiter rather than a per-window token bucket: the deadline for the n-th byte is
 * {@code start + n / rate}, so the average rate over the whole backup is exactly the configured one and there is no
 * drift accumulated by rounding a window. A backup that fell behind (slow disk, contention) is allowed to catch up for
 * at most one second's worth of credit, which keeps the limiter from turning a transient stall into an unbounded burst.
 * <p>
 * Not thread safe by design: the backup reads its source files from a single thread and only the compression is
 * parallel, so the limiter is touched by that one reader.
 * <p>
 * The reader goes through {@link #throttle(long)} once per chunk it reads, which makes it the one place every archive writer
 * passes: an optional check is run there, with or without a rate cap, and between the slices of a park, so a backup that was
 * asked to stop ends at its next chunk and never sleeps through its whole deadline.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class IoThrottler {
  private static final long ONE_SECOND_NANOS = 1_000_000_000L;
  /** Byte count at which the baseline is moved forward, keeping {@code totalBytes * ONE_SECOND_NANOS} inside a long. */
  private static final long REBASE_BYTES     = 1L << 30;
  /** Longest a park lasts without running the check, when there is one: how late a request to stop is seen by a throttled read. */
  private static final long CHECK_SLICE_NANOS = 100_000_000L;

  private final long     bytesPerSecond;
  private final Runnable abortCheck;
  private       long     startNanos = -1;
  private       long     totalBytes = 0L;

  /**
   * @param maxMBPerSecond maximum read rate in MB/s. Values &lt;= 0 disable throttling entirely.
   */
  public IoThrottler(final int maxMBPerSecond) {
    this(maxMBPerSecond, null);
  }

  /**
   * @param maxMBPerSecond maximum read rate in MB/s. Values &lt;= 0 disable throttling entirely.
   * @param abortCheck     run on every {@link #throttle(long)} and between the slices of a park, or {@code null} for none. It
   *                       ends the backup by throwing.
   */
  public IoThrottler(final int maxMBPerSecond, final Runnable abortCheck) {
    this.bytesPerSecond = maxMBPerSecond > 0 ? maxMBPerSecond * 1024L * 1024L : 0L;
    this.abortCheck = abortCheck;
  }

  public boolean isEnabled() {
    return bytesPerSecond > 0;
  }

  /**
   * Accounts for {@code bytes} just read and parks the calling thread for as long as it takes to stay under the cap.
   */
  public void throttle(final long bytes) {
    if (abortCheck != null)
      abortCheck.run();

    if (bytesPerSecond <= 0)
      return;

    final long now = System.nanoTime();
    if (startNanos < 0)
      startNanos = now;

    totalBytes += bytes;

    final long deadline = startNanos + totalBytes * ONE_SECOND_NANOS / bytesPerSecond;
    final long delay = deadline - now;
    if (delay > 0)
      parkUntil(deadline);
    else if (-delay > ONE_SECOND_NANOS)
      // FELL MORE THAN ONE SECOND BEHIND: FORGET THE EXCESS CREDIT INSTEAD OF LETTING IT BE SPENT AS AN UNBOUNDED BURST
      startNanos += -delay - ONE_SECOND_NANOS;

    if (totalBytes >= REBASE_BYTES) {
      // MOVE THE BASELINE TO THE DEADLINE JUST COMPUTED AND RESTART THE BYTE COUNT. THIS KEEPS totalBytes * 1e9 INSIDE
      // A long (IT WOULD OVERFLOW PAST ~9.2GB, WHICH A BACKUP REACHES) SO THE DEADLINE STAYS EXACT INTEGER ARITHMETIC,
      // AND BECAUSE THE NEW BASELINE IS THE EXACT OLD DEADLINE RATHER THAN 'now', NO ROUNDING DRIFT ACCUMULATES
      startNanos = deadline;
      totalBytes = 0L;
    }
  }

  /**
   * {@link LockSupport#parkNanos} is allowed to return early for no reason at all, so a single call would let a read
   * through ahead of its deadline. The absolute-deadline arithmetic would absorb that on the next call, but looping is
   * cheap and makes the cap hold exactly rather than on average.
   * <p>
   * An interrupt breaks the loop instead of being spun through: the flag stays set for the caller (the backup's reader
   * thread) to act on, which is the opposite of what ignoring it and parking again would do.
   * <p>
   * With a check, the park is cut into slices of {@link #CHECK_SLICE_NANOS} and the check runs after each one.
   */
  private void parkUntil(final long deadlineNanos) {
    while (!Thread.currentThread().isInterrupted()) {
      final long remaining = deadlineNanos - System.nanoTime();
      if (remaining <= 0)
        return;
      if (abortCheck == null)
        LockSupport.parkNanos(remaining);
      else {
        LockSupport.parkNanos(Math.min(remaining, CHECK_SLICE_NANOS));
        abortCheck.run();
      }
    }
  }
}
