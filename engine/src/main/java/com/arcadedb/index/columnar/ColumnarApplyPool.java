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
package com.arcadedb.index.columnar;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.log.LogManager;
import com.arcadedb.utility.DedicatedThreadPool;

import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.logging.Level;

/**
 * JVM-wide dedicated executor that runs the apply passes of the COLUMNAR indexes (re-reading committed pages into the
 * projection). Lazy singleton; daemon threads. At most one pass per database runs at a time (a CAS flag in the
 * manager), so the pool is sized by {@code arcadedb.columnar.applyThreads}, not by the number of tenants.
 * <p>
 * <b>Why the queue is UNBOUNDED with no caller-runs policy</b> (a deliberate deviation from the "bounded queue +
 * caller-runs" rule of the {@code engine-concurrency} skill): a submitter is the ticker or a pass
 * that finishes with more work pending, never a user thread, and caller-runs would run a page re-read on a commit or
 * Raft apply thread, which is exactly what the asynchronous projection exists to avoid. Back-pressure is structural:
 * at most one queued pass per database, so the queue cannot grow past the number of open databases. Its
 * {@code PoolStats} report {@code queueCapacityRemaining=-1} and a caller-runs count that stays at zero;
 * {@code queueDepth} is the saturation signal ({@code pool=columnar_apply}).
 * <p>
 * <b>The ticker.</b> One JVM-wide daemon thread {@code ArcadeDB-ColumnarTicker}, created on the first
 * {@link #register(Runnable, LongSupplier)}, wakes up every {@link #BASE_PERIOD_MS} and calls each registered tick whose own interval has elapsed
 * since its last run. The interval comes from a supplier read at every check, so a per-database
 * {@code arcadedb.columnar.applyInterval} (DATABASE scope) is honored and a change takes effect on the next check. The pool knows no manager type: the per-database manager registers a tick that submits a
 * pass when it has pending work, and unregisters it on close, kill or drop.
 *
 * @see ColumnarMaintenancePool
 */
public final class ColumnarApplyPool extends DedicatedThreadPool {

  private static final class Holder {
    static final ColumnarApplyPool INSTANCE = new ColumnarApplyPool();
  }

  /** Wake-up period of the ticker: the finest interval a tick can have, whatever its supplier answers. */
  public static final long BASE_PERIOD_MS = 10L;

  /**
   * One registered tick. A tick runs under its own monitor with a live check, which is what lets {@link #unregister}
   * promise that a removed tick is not running and never starts again: it clears {@code live}, then waits for the
   * monitor.
   */
  private static final class Entry {
    final Runnable     tick;
    final LongSupplier intervalMs;
    volatile boolean   live = true;
    /** Guarded by the monitor. */
    long               lastRunNanos = System.nanoTime();
    /** Guarded by the monitor; 0 = never logged. */
    long               lastLogNanos;

    Entry(final Runnable tick, final LongSupplier intervalMs) {
      this.tick = tick;
      this.intervalMs = intervalMs;
    }

    synchronized void runIfLive() {
      if (!live)
        return;
      try {
        final long now = System.nanoTime();
        // A non-positive interval means "every base period"; a huge one is effectively never (the multiplication saturates)
        if (now - lastRunNanos < TimeUnit.MILLISECONDS.toNanos(intervalMs.getAsLong()))
          return;
        lastRunNanos = now;
        tick.run();
      } catch (final Throwable e) {
        // One failing tick, or a failing interval supplier (read as "not due"), must not stop the others, nor the schedule
        final long now = System.nanoTime();
        if (lastLogNanos == 0L || now - lastLogNanos >= LOG_THROTTLE_NANOS) {
          lastLogNanos = now;
          LogManager.instance().log(this, Level.SEVERE, "Columnar apply tick failed", e);
        }
      }
    }

    synchronized void retire() {
      live = false;
    }
  }

  private static final long   LOG_THROTTLE_NANOS = TimeUnit.MINUTES.toNanos(1);
  private static final Entry[] NO_ENTRIES = new Entry[0];

  private final Object                   registryLock = new Object();
  /** Copy-on-write: the ticker reads it without locking, writers replace it under {@link #registryLock}. */
  private volatile Entry[]               entries      = NO_ENTRIES;
  private ScheduledExecutorService       ticker;
  private boolean                       closed;

  private ColumnarApplyPool() {
    super("ArcadeDB-ColumnarApply-", Math.max(1, GlobalConfiguration.COLUMNAR_APPLY_THREADS.getValueAsInteger()),
        UNBOUNDED_QUEUE, SaturationPolicy.NONE, DedicatedThreadPool::plainWorker, "Columnar apply pool", null);
  }

  public static ColumnarApplyPool getInstance() {
    return Holder.INSTANCE;
  }

  /**
   * Registers a tick the ticker calls when {@code intervalMs} (read at every check, so it may change; the finest
   * effective value is {@link #BASE_PERIOD_MS}: a value <= 0 means every base period, a huge one effectively never;
   * a supplier that throws counts as "not due" and is logged, throttled) has elapsed since its last run. Registering the same instance twice
   * is a no-op. A tick must be quick (it submits work, it does not do it) and must not throw; one that does is
   * logged and does not stop the others.
   *
   * @throws IllegalStateException after {@link #close()}
   */
  public void register(final Runnable tick, final LongSupplier intervalMs) {
    synchronized (registryLock) {
      if (closed)
        throw new IllegalStateException("The columnar apply pool is closed");
      for (final Entry e : entries)
        if (e.tick == tick)
          return;
      final Entry[] next = Arrays.copyOf(entries, entries.length + 1);
      next[entries.length] = new Entry(tick, intervalMs);
      entries = next;
      if (ticker == null) {
        ticker = Executors.newSingleThreadScheduledExecutor(r -> {
          final Thread t = new Thread(r, "ArcadeDB-ColumnarTicker");
          t.setDaemon(true);
          return t;
        });
        ticker.scheduleWithFixedDelay(this::tickAll, BASE_PERIOD_MS, BASE_PERIOD_MS, TimeUnit.MILLISECONDS);
      }
    }
  }

  private void tickAll() {
    try {
      for (final Entry e : entries)
        e.runIfLive();
    } catch (final Throwable t) {
      // scheduleWithFixedDelay suppresses every later run after an escaped exception: the schedule must never die
      LogManager.instance().log(this, Level.SEVERE, "Columnar apply ticker pass failed", t);
    }
  }

  /**
   * Removes a tick. Idempotent. When this returns the tick is not running and is never called again, so a tick that
   * is running at the time of the call is waited for: do not call this while holding a lock the tick takes.
   */
  public void unregister(final Runnable tick) {
    Entry removed = null;
    synchronized (registryLock) {
      for (int i = 0; i < entries.length; i++)
        if (entries[i].tick == tick) {
          removed = entries[i];
          final Entry[] next = new Entry[entries.length - 1];
          System.arraycopy(entries, 0, next, 0, i);
          System.arraycopy(entries, i + 1, next, i, next.length - i);
          entries = next;
          break;
        }
    }
    // Outside the registry lock: waits for a running tick, which may itself register or unregister
    if (removed != null)
      removed.retire();
  }

  @Override
  public void close() {
    synchronized (registryLock) {
      closed = true;
      if (ticker != null)
        ticker.shutdownNow();
    }
    super.close();
  }
}
