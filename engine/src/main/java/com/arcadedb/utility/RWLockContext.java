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
import java.util.concurrent.locks.*;

public class RWLockContext {
  private final ReentrantReadWriteLock lock          = new ReentrantReadWriteLock(true);
  private       boolean                enableLocking = true;

  protected ReentrantReadWriteLock.ReadLock readLock() {
    if (!enableLocking)
      return null;

    final ReentrantReadWriteLock.ReadLock rl = lock.readLock();
    rl.lock();
    return rl;
  }

  protected void readUnlock(final ReentrantReadWriteLock.ReadLock rl) {
    if (rl != null)
      rl.unlock();
  }

  protected ReentrantReadWriteLock.WriteLock writeLock() {
    if (!enableLocking)
      return null;

    final ReentrantReadWriteLock.WriteLock wl = lock.writeLock();
    wl.lock();
    return wl;
  }

  protected void writeUnlock(final ReentrantReadWriteLock.WriteLock wl) {
    if (wl != null)
      wl.unlock();
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
    final ReentrantReadWriteLock.WriteLock wl = writeLock();
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
