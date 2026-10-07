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
package com.arcadedb.engine;

import com.arcadedb.GlobalConfiguration;

import java.lang.ref.Cleaner;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The bytes the scans of the JVM have read ahead of the queries that consume them, shared by every bucket of every database
 * (issue #9404).
 * <p>
 * A scan reads its records in batches, and a record that spans several pages is assembled into a buffer of its own. Those buffers
 * belong to no query: they are what the scan holds between reading a batch and handing it over, so the query heap budget does not
 * see them. {@link GlobalConfiguration#QUERY_BATCH_MAX_BYTES} bounds one batch; this pool bounds the sum of all of them, so a
 * hundred queries scanning a hundred buckets cannot hold a hundred times the bound. A scan reads {@link #getAvailableBytes()} / a
 * share of what is left, so its batches get smaller as the pool fills - down to one record at a time - instead of failing.
 * <p>
 * It is a soft limit: a batch always holds at least one record, and the bytes are reserved after the batch is read, so the pool can
 * go past its limit by the records scans hold at once. A scan gives its bytes back when it has handed its batch over, and - for one
 * the caller abandoned half way - when the garbage collector finds it (the same {@link Cleaner} approach as the query heap budget).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class ScanReadAheadBudget {
  private static final AtomicLong RESERVED = new AtomicLong();
  private static final AtomicLong PEAK     = new AtomicLong();

  private static final Cleaner CLEANER = Cleaner.create(runnable -> {
    final Thread thread = new Thread(runnable, "ArcadeDB-ScanReadAhead-Cleaner");
    thread.setDaemon(true);
    return thread;
  });

  private ScanReadAheadBudget() {
  }

  /** The pool in bytes, or 0 when it is disabled. */
  public static long getLimitBytes() {
    final long megabytes = GlobalConfiguration.QUERY_SCAN_READ_AHEAD_MAX_RAM.getValueAsLong();
    if (megabytes <= 0)
      return 0L;
    return megabytes < Long.MAX_VALUE / (1024 * 1024) ? megabytes * 1024 * 1024 : Long.MAX_VALUE;
  }

  public static boolean isEnabled() {
    return GlobalConfiguration.QUERY_SCAN_READ_AHEAD_MAX_RAM.getValueAsLong() > 0;
  }

  /** The bytes the scans hold read ahead right now. */
  public static long getReservedBytes() {
    return RESERVED.get();
  }

  /** The most bytes the scans held read ahead at once since the JVM started. */
  public static long getPeakReservedBytes() {
    return PEAK.get();
  }

  /** What is left of the pool, {@link Long#MAX_VALUE} when it is disabled. */
  public static long getAvailableBytes() {
    final long limit = getLimitBytes();
    return limit <= 0 ? Long.MAX_VALUE : Math.max(0L, limit - RESERVED.get());
  }

  /**
   * What one scan holds of the pool. It registers with the {@link Cleaner} for its owner, so what the owner still holds when it is
   * abandoned goes back to the pool once the owner is unreachable.
   */
  public static final class Reservation implements Runnable {
    private final AtomicLong held = new AtomicLong();

    /** Adds {@code bytes} to what the owner holds. */
    public void reserve(final long bytes) {
      if (bytes <= 0)
        return;
      held.addAndGet(bytes);
      final long now = RESERVED.addAndGet(bytes);
      long peak;
      while (now > (peak = PEAK.get()) && !PEAK.compareAndSet(peak, now)) {
        // RETRY: ANOTHER RESERVATION MOVED THE PEAK MEANWHILE
      }
    }

    /** Gives back everything the owner holds. */
    public void release() {
      final long bytes = held.getAndSet(0L);
      if (bytes > 0)
        RESERVED.addAndGet(-bytes);
    }

    public long getHeldBytes() {
      return held.get();
    }

    @Override
    public void run() {
      release();
    }
  }

  /** A reservation for {@code owner}, given back to the pool when the owner is garbage collected. */
  public static Reservation newReservation(final Object owner) {
    final Reservation reservation = new Reservation();
    CLEANER.register(owner, reservation);
    return reservation;
  }
}
