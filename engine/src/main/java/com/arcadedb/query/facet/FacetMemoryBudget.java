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
package com.arcadedb.query.facet;

import com.arcadedb.GlobalConfiguration;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The counting memory all concurrent FACETS queries of the JVM may hold together (dense and hash counters, match bitmaps): a fair
 * {@link Semaphore} of KiB permits sized by {@code arcadedb.columnar.queryMemoryBudget}. {@link #instance()} resizes it when the
 * setting changed. A request is granted at once, waits up to {@code maxWaitMs} for others to release, or is refused (null) so the
 * caller switches to a smaller representation or reports the facet unavailable.
 */
public final class FacetMemoryBudget {
  private static final long           KIB      = 1024;
  private static final FacetMemoryBudget INSTANCE = new FacetMemoryBudget(GlobalConfiguration.COLUMNAR_QUERY_MEMORY_BUDGET.getValueAsLong());

  private final ResizableSemaphore semaphore;
  private volatile long   capacityKib;

  /** A fair semaphore that can take permits away, which {@link Semaphore#reducePermits} only allows to subclasses. */
  private static final class ResizableSemaphore extends Semaphore {
    ResizableSemaphore(final int permits) {
      super(permits, true);
    }

    void shrink(final int permits) {
      reducePermits(permits);
    }
  }

  /** One grant of the budget; closing it gives the memory back (idempotent). */
  public final class Reservation implements AutoCloseable {
    private final int           permits;
    private final long          bytes;
    private final boolean       waited;
    private final AtomicBoolean closed = new AtomicBoolean();

    private Reservation(final int permits, final long bytes, final boolean waited) {
      this.permits = permits;
      this.bytes = bytes;
      this.waited = waited;
    }

    public long bytes() {
      return bytes;
    }

    /** Whether the grant was not immediate. A caller that counts waits uses the {@code onWait} hook of {@link #reserve(long, long, Runnable)}, which also sees a refusal. */
    public boolean waited() {
      return waited;
    }

    @Override
    public void close() {
      if (closed.compareAndSet(false, true))
        semaphore.release(permits);
    }
  }

  FacetMemoryBudget(final long bytes) {
    capacityKib = toKib(bytes);
    semaphore = new ResizableSemaphore((int) capacityKib);
  }

  /** The JVM-wide budget, resized first when the setting changed since the last call. */
  public static FacetMemoryBudget instance() {
    final long wanted = toKib(GlobalConfiguration.COLUMNAR_QUERY_MEMORY_BUDGET.getValueAsLong());
    if (wanted != INSTANCE.capacityKib)
      INSTANCE.resize(wanted);
    return INSTANCE;
  }

  /**
   * Reserves {@code bytes} (rounded up to KiB), waiting up to {@code maxWaitMs} when the budget is short.
   *
   * @return the reservation, or null when refused: the request exceeds the whole capacity (no wait), the wait ran out or the thread
   *     was interrupted (its flag is kept)
   */
  public Reservation reserve(final long bytes, final long maxWaitMs) {
    return reserve(bytes, maxWaitMs, null);
  }

  /**
   * As {@link #reserve(long, long)}, telling the caller about a wait it would otherwise only see when the grant succeeds.
   *
   * @param onWait called once when the request was not granted at once and the thread is about to wait, whatever the wait ends in
   *               (a grant, a timeout or an interrupt): a refusal after a wait is as much a sign of contention as a late grant. Null
   *               to ignore. Never called for a request refused without waiting.
   */
  public Reservation reserve(final long bytes, final long maxWaitMs, final Runnable onWait) {
    final long kib = toKib(bytes);
    if (kib > capacityKib)
      return null;
    final int permits = (int) kib;
    try {
      // the timed form honors the fair queue; the untimed tryAcquire would barge past waiters
      if (semaphore.tryAcquire(permits, 0, TimeUnit.MILLISECONDS))
        return new Reservation(permits, bytes, false);
      if (maxWaitMs > 0) {
        if (onWait != null)
          onWait.run();
        if (semaphore.tryAcquire(permits, maxWaitMs, TimeUnit.MILLISECONDS))
          return new Reservation(permits, bytes, true);
      }
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    return null;
  }

  /** The bytes that can be reserved right now (never negative, even right after the budget shrank below what is held). */
  public long available() {
    return Math.max(0, semaphore.availablePermits()) * KIB;
  }

  boolean hasWaiters() {
    return semaphore.hasQueuedThreads();
  }

  private synchronized void resize(final long wantedKib) {
    final long delta = wantedKib - capacityKib;
    if (delta > 0)
      semaphore.release((int) delta);
    else if (delta < 0)
      semaphore.shrink((int) -delta);
    capacityKib = wantedKib;
  }

  private static long toKib(final long bytes) {
    return Math.min(Integer.MAX_VALUE, (Math.max(0, bytes) + KIB - 1) / KIB);
  }
}
