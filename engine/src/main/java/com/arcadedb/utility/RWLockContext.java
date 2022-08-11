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
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  protected void readLock() {
    lock.readLock().lock();
  }

  protected void readUnlock() {
    lock.readLock().unlock();
  }

  protected void writeLock() {
    lock.writeLock().lock();
  }

  protected void writeUnlock() {
    lock.writeLock().unlock();
  }

  /**
   * Executes a callback in an shared lock.
   */
  public <RET extends Object> RET executeInReadLock(final Callable<RET> callable) {
    readLock();
    try {

      return callable.call();

    } catch (RuntimeException e) {
      throw e;

    } catch (Throwable e) {
      throw new ArcadeDBException("Error in execution in lock", e);

    } finally {
      readUnlock();
    }
  }

  /**
   * Executes a callback in an exclusive lock.
   */
  public <RET extends Object> RET executeInWriteLock(final Callable<RET> callable) {
    writeLock();
    try {

      return callable.call();

    } catch (RuntimeException e) {
      throw e;

    } catch (Throwable e) {
      throw new ArcadeDBException("Error in execution in lock", e);

    } finally {
      writeUnlock();
    }
  }
}
