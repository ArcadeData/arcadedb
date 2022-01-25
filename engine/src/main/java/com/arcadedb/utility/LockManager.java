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

import com.arcadedb.log.LogManager;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

/**
 * Lock manager implementation.
 */
public class LockManager<RESOURCE, REQUESTER> {
  public enum LOCK_STATUS {NO, YES, ALREADY_ACQUIRED}

  private final ConcurrentHashMap<RESOURCE, ODistributedLock> lockManager = new ConcurrentHashMap<>(256);

  private class ODistributedLock {
    final REQUESTER      owner;
    final CountDownLatch lock;
    final long           acquiredOn;

    private ODistributedLock(final REQUESTER owner) {
      this.owner = owner;
      this.lock = new CountDownLatch(1);
      this.acquiredOn = System.currentTimeMillis();
    }
  }

  public LOCK_STATUS tryLock(final RESOURCE resource, final REQUESTER requester, final long timeout) {
    if (resource == null)
      throw new IllegalArgumentException("Resource to lock is null");

    final ODistributedLock lock = new ODistributedLock(requester);

    ODistributedLock currentLock = lockManager.putIfAbsent(resource, lock);
    if (currentLock != null) {
      if (currentLock.owner.equals(requester)) {
        // SAME RESOURCE/SERVER, ALREADY LOCKED
        LogManager.instance().log(this, Level.FINE, "Resource '%s' already locked by requester '%s'", resource, currentLock.owner);
        return LOCK_STATUS.ALREADY_ACQUIRED;
      } else {
        // TRY TO RE-LOCK IT UNTIL TIMEOUT IS EXPIRED
        final long startTime = System.currentTimeMillis();
        do {
          try {
            if (timeout > 0) {
              if (!currentLock.lock.await(timeout, TimeUnit.MILLISECONDS))
                continue;
            } else
              currentLock.lock.await();

            currentLock = lockManager.putIfAbsent(resource, lock);

          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            break;
          }
        } while (currentLock != null && (timeout == 0 || System.currentTimeMillis() - startTime < timeout));
      }
    }

    return currentLock == null ? LOCK_STATUS.YES : LOCK_STATUS.NO;
  }

  public void unlock(final RESOURCE resource, final REQUESTER requester) {
    if (resource == null)
      throw new IllegalArgumentException("Resource to unlock is null");

    final ODistributedLock owner = lockManager.remove(resource);
    if (owner != null) {
      if (!owner.owner.equals(requester))
        throw new LockException("Cannot unlock resource '" + resource + "' because owner '" + owner.owner + "' <> requester '" + requester + "'");

      // NOTIFY ANY WAITERS
      owner.lock.countDown();
    }
  }

  public void close() {
    for (Iterator<Map.Entry<RESOURCE, ODistributedLock>> it = lockManager.entrySet().iterator(); it.hasNext(); ) {
      final Map.Entry<RESOURCE, ODistributedLock> entry = it.next();
      final ODistributedLock lock = entry.getValue();

      it.remove();

      // NOTIFY ANY WAITERS
      lock.lock.countDown();
    }
  }
}
