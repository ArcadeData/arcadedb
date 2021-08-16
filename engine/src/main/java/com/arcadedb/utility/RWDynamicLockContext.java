/*
 * Copyright 2021 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.arcadedb.utility;

import java.util.concurrent.Callable;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class RWDynamicLockContext {
  private final ReentrantReadWriteLock lock;

  public RWDynamicLockContext(final boolean multiThread) {
    lock = multiThread ? new ReentrantReadWriteLock() : null;
  }

  protected void readLock() {
    if (lock != null)
      lock.readLock().lock();
  }

  protected void readUnlock() {
    if (lock != null)
      lock.readLock().unlock();
  }

  protected void writeLock() {
    if (lock != null)
      lock.writeLock().lock();
  }

  protected void writeUnlock() {
    if (lock != null)
      lock.writeLock().unlock();
  }

  public Object executeInReadLock(Callable<Object> callable) {
    readLock();
    try {

      return callable.call();

    } catch (RuntimeException e) {
      throw e;

    } catch (Throwable e) {
      throw new RuntimeException("Error in execution in lock", e);

    } finally {
      readUnlock();
    }
  }

  public Object executeInWriteLock(Callable<Object> callable) {
    writeLock();
    try {

      return callable.call();

    } catch (RuntimeException e) {
      throw e;

    } catch (Throwable e) {
      throw new RuntimeException("Error in execution in lock", e);

    } finally {
      writeUnlock();
    }
  }
}
