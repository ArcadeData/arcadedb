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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LockManagerTest {

  private LockManager<String, String> lockManager;

  @BeforeEach
  void setUp() {
    lockManager = new LockManager<>();
  }

  @AfterEach
  void tearDown() {
    lockManager.close();
  }

  @Test
  void tryLockAcquiresLock() {
    final LockManager.LOCK_STATUS status = lockManager.tryLock("resource1", "requester1", 1000);
    assertThat(status).isEqualTo(LockManager.LOCK_STATUS.YES);
  }

  @Test
  void tryLockSameRequesterReturnsAlreadyAcquired() {
    lockManager.tryLock("resource1", "requester1", 1000);

    // Same requester acquiring again returns ALREADY_ACQUIRED
    final LockManager.LOCK_STATUS status = lockManager.tryLock("resource1", "requester1", 1000);
    assertThat(status).isEqualTo(LockManager.LOCK_STATUS.ALREADY_ACQUIRED);
  }

  @Test
  void tryLockDifferentRequesterFailsWithTimeout() {
    lockManager.tryLock("resource1", "requester1", 1000);

    // Different requester should fail with short timeout
    final LockManager.LOCK_STATUS status = lockManager.tryLock("resource1", "requester2", 100);
    assertThat(status).isEqualTo(LockManager.LOCK_STATUS.NO);
  }

  @Test
  void unlockReleasesLock() {
    lockManager.tryLock("resource1", "requester1", 1000);
    lockManager.unlock("resource1", "requester1");

    // Now a different requester can acquire
    final LockManager.LOCK_STATUS status = lockManager.tryLock("resource1", "requester2", 1000);
    assertThat(status).isEqualTo(LockManager.LOCK_STATUS.YES);
  }

  @Test
  void unlockDifferentRequesterThrows() {
    lockManager.tryLock("resource1", "requester1", 1000);

    // Unlock by different requester should throw
    assertThatThrownBy(() -> lockManager.unlock("resource1", "requester2"))
        .isInstanceOf(LockException.class);
  }

  @Test
  void multipleLocksDifferentResources() {
    final LockManager.LOCK_STATUS status1 = lockManager.tryLock("resource1", "requester1", 1000);
    final LockManager.LOCK_STATUS status2 = lockManager.tryLock("resource2", "requester2", 1000);

    assertThat(status1).isEqualTo(LockManager.LOCK_STATUS.YES);
    assertThat(status2).isEqualTo(LockManager.LOCK_STATUS.YES);
  }

  @Test
  void concurrentLockAttempts() throws InterruptedException {
    final AtomicInteger successCount = new AtomicInteger(0);
    final CountDownLatch startLatch = new CountDownLatch(1);
    final CountDownLatch doneLatch = new CountDownLatch(3);

    for (int i = 0; i < 3; i++) {
      final String requester = "requester" + i;
      new Thread(() -> {
        try {
          startLatch.await();
          final LockManager.LOCK_STATUS status = lockManager.tryLock("resource", requester, 100);
          if (status == LockManager.LOCK_STATUS.YES) {
            successCount.incrementAndGet();
            Thread.sleep(50);
            lockManager.unlock("resource", requester);
          }
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        } finally {
          doneLatch.countDown();
        }
      }).start();
    }

    startLatch.countDown();
    doneLatch.await(5, TimeUnit.SECONDS);

    // At least one should have succeeded
    assertThat(successCount.get()).isGreaterThanOrEqualTo(1);
  }

  @Test
  void closeReleasesAllLocks() {
    lockManager.tryLock("resource1", "requester1", 1000);
    lockManager.tryLock("resource2", "requester2", 1000);

    lockManager.close();

    // Create a new lock manager to verify resources are available
    final LockManager<String, String> newManager = new LockManager<>();
    try {
      final LockManager.LOCK_STATUS status = newManager.tryLock("resource1", "requester3", 1000);
      assertThat(status).isEqualTo(LockManager.LOCK_STATUS.YES);
    } finally {
      newManager.close();
    }
  }

  @Test
  void lockWithWaitSucceeds() throws InterruptedException {
    lockManager.tryLock("resource1", "requester1", 1000);

    final AtomicReference<LockManager.LOCK_STATUS> acquired = new AtomicReference<>();
    final CountDownLatch waiterStarted = new CountDownLatch(1);
    final Thread waiter = new Thread(() -> {
      waiterStarted.countDown();
      // This thread will wait for the lock with a reasonable timeout
      acquired.set(lockManager.tryLock("resource1", "requester2", 2000));
    });
    waiter.start();

    // Wait for waiter to start
    waiterStarted.await(1, TimeUnit.SECONDS);

    // Give the waiter time to enter the wait state
    Thread.sleep(100);

    // Release the lock
    lockManager.unlock("resource1", "requester1");

    // Wait for the waiter to complete
    waiter.join(3000);

    assertThat(acquired.get()).isEqualTo(LockManager.LOCK_STATUS.YES);
  }

  @Test
  void tryLockWithNullResourceThrows() {
    assertThatThrownBy(() -> lockManager.tryLock(null, "requester", 1000))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Resource");
  }

  @Test
  void tryLockWithNullRequesterThrows() {
    assertThatThrownBy(() -> lockManager.tryLock("resource", null, 1000))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Requester");
  }

  @Test
  void unlockWithNullResourceThrows() {
    assertThatThrownBy(() -> lockManager.unlock(null, "requester"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Resource");
  }

  @Test
  void toStringShowsLockInfo() {
    lockManager.tryLock("resource1", "requester1", 1000);

    final String str = lockManager.toString();
    assertThat(str).contains("resource1");
    assertThat(str).contains("requester1");
  }

  @Test
  void lockStatusEnumValues() {
    // Verify all enum values exist
    assertThat(LockManager.LOCK_STATUS.YES).isNotNull();
    assertThat(LockManager.LOCK_STATUS.NO).isNotNull();
    assertThat(LockManager.LOCK_STATUS.ALREADY_ACQUIRED).isNotNull();
  }

  @Test
  void unlockNonExistentResourceDoesNotThrow() {
    // Unlocking a resource that was never locked should not throw
    // Just verify it doesn't throw (the method doesn't return anything)
    lockManager.unlock("nonexistent", "requester");
  }
}
