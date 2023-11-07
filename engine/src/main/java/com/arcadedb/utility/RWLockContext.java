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
  private final StampedLock lock = new StampedLock();

  protected long readLock() {
    if (lock.isWriteLocked() || lock.isReadLocked())
      return 0;

    return lock.readLock();
  }

  protected void readUnlock(final long stamp) {
    if (stamp != 0)
      lock.unlockRead(stamp);
  }

  protected long writeLock() {
    if (lock.isWriteLocked() || lock.isReadLocked())
      return 0;
    return lock.writeLock();
  }

  protected void writeUnlock(final long stamp) {
    if (stamp != 0)
      lock.unlockWrite(stamp);
  }

  /**
   * Executes a callback in an shared lock.
   */
  public <RET> RET executeInReadLock(final Callable<RET> callable) {
    final long stamp = readLock();
    try {

      return callable.call();

    } catch (final RuntimeException e) {
      throw e;

    } catch (final Throwable e) {
      throw new ArcadeDBException("Error in execution in lock", e);

    } finally {
      readUnlock(stamp);
    }
  }

  /**
   * Executes a callback in an exclusive lock.
   */
  public <RET> RET executeInWriteLock(final Callable<RET> callable) {
    final long stamp = writeLock();
    try {

      return callable.call();

    } catch (final RuntimeException e) {
      throw e;

    } catch (final Throwable e) {
      throw new ArcadeDBException("Error in execution in lock", e);

    } finally {
      writeUnlock(stamp);
    }
  }
}
