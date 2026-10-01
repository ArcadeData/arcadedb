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

import com.arcadedb.exception.ArcadeDBException;

import java.util.concurrent.Callable;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Reader/writer lock context. A single {@link ReentrantReadWriteLock} makes every reader write the same shared state word
 * (plus a per-thread hold counter) on acquire and release, so concurrent readers - which never wait on each other - still
 * slow down as threads are added (#8838). This context stripes the lock instead: a reader locks only the stripe picked by its
 * thread id, so readers on different stripes touch different memory, while a writer takes every stripe, always in the same
 * order, and so excludes every reader. Readers vastly outnumber writers (close, drop, schema changes), so the writer pays
 * the cost of the stripes. Semantics per thread are those of {@link ReentrantReadWriteLock}: reads and writes are reentrant,
 * a writer may take the read lock (downgrade), a reader must not ask for the write lock.
 */
public class RWLockContext {
  private static final int MAX_STRIPES = 64;

  private final ReentrantReadWriteLock[] stripes;
  private final int                      stripeMask;
  private       boolean                  enableLocking = true;

  /**
   * Single lock, the cheapest choice for components whose readers are not hot (HTTP sessions, remote clients).
   */
  public RWLockContext() {
    this(1);
  }

  /**
   * @param stripeCount number of lock stripes, rounded up to a power of two and capped at {@link #MAX_STRIPES}
   */
  protected RWLockContext(final int stripeCount) {
    int n = 1;
    while (n < stripeCount && n < MAX_STRIPES)
      n <<= 1;
    stripes = new ReentrantReadWriteLock[n];
    for (int i = 0; i < n; i++)
      stripes[i] = new ReentrantReadWriteLock(true);
    stripeMask = n - 1;
  }

  /**
   * Stripe count that fits the CPUs of this JVM, fixed when the lock is built.
   */
  protected static int defaultStripeCount() {
    return Math.max(4, Runtime.getRuntime().availableProcessors());
  }

  protected ReentrantReadWriteLock.ReadLock readLock() {
    if (!enableLocking)
      return null;

    final ReentrantReadWriteLock.ReadLock rl = stripes[(int) Thread.currentThread().threadId() & stripeMask].readLock();
    rl.lock();
    return rl;
  }

  protected void readUnlock(final ReentrantReadWriteLock.ReadLock rl) {
    if (rl != null)
      rl.unlock();
  }

  protected ReentrantReadWriteLock.WriteLock[] writeLock() {
    if (!enableLocking)
      return null;

    final ReentrantReadWriteLock.WriteLock[] wl = new ReentrantReadWriteLock.WriteLock[stripes.length];
    // ALWAYS IN THE SAME ORDER, SO TWO WRITERS CANNOT DEADLOCK EACH OTHER
    int i = 0;
    try {
      for (; i < stripes.length; i++) {
        wl[i] = stripes[i].writeLock();
        wl[i].lock();
      }
    } catch (final Throwable t) {
      // NEVER LEAK THE STRIPES ALREADY TAKEN: THEIR READERS WOULD BLOCK FOREVER
      for (int k = i - 1; k >= 0; k--)
        wl[k].unlock();
      throw t;
    }
    return wl;
  }

  protected void writeUnlock(final ReentrantReadWriteLock.WriteLock[] wl) {
    if (wl != null)
      for (int i = wl.length - 1; i >= 0; i--)
        wl[i].unlock();
  }

  /**
   * Executes a callback in an shared lock.
   */
  public <RET> RET executeInReadLock(final Callable<RET> callable) {
    final ReentrantReadWriteLock.ReadLock rl = readLock();
    try {

      return callable.call();

    } catch (final RuntimeException e) {
      throw e;

    } catch (final Throwable e) {
      throw new ArcadeDBException("Error in execution in lock", e);

    } finally {
      readUnlock(rl);
    }
  }

  /**
   * Executes a callback in an exclusive lock.
   */
  public <RET> RET executeInWriteLock(final Callable<RET> callable) {
    final ReentrantReadWriteLock.WriteLock[] wl = writeLock();
    try {

      return callable.call();

    } catch (final RuntimeException e) {
      throw e;

    } catch (final Throwable e) {
      throw new ArcadeDBException("Error in execution in lock", e);

    } finally {
      writeUnlock(wl);
    }
  }

  protected void setLockingEnabled(final boolean enabled) {
    this.enableLocking = enabled;
  }

  protected boolean isLockingEnabled() {
    return enableLocking;
  }
}
