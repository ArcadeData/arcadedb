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

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Diagnostics for a held lock.
 * <p>
 * A lock is only ever released by its owner: there is no lease and no expiry, and the abandoned-lock
 * sweep reclaims a lock only once the owning thread has DIED. So a lock leaked by a thread that is still
 * alive is held until the process restarts, and the identity of the holder — the one fact needed to find
 * the culprit — is observable only while the lock is still held. These accessors are what makes it
 * observable, from a timeout message and from an operator diagnostics page.
 */
class LockManagerDiagnosticsTest {

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
  void describeOwnerNamesTheHolder() {
    assertThat(lockManager.tryLock("file-147", "tx-A", 1_000)).isEqualTo(LockManager.LOCK_STATUS.YES);

    final String description = lockManager.describeOwner("file-147");
    assertThat(description).isNotNull();
    assertThat(description).contains("tx-A");
    assertThat(description).contains("waiters");
  }

  @Test
  void describeOwnerReturnsNullWhenFree() {
    assertThat(lockManager.describeOwner("never-locked")).isNull();

    lockManager.tryLock("file-147", "tx-A", 1_000);
    lockManager.unlock("file-147", "tx-A");
    assertThat(lockManager.describeOwner("file-147")).isNull();
  }

  @Test
  void describeOwnerCountsWaiters() throws Exception {
    assertThat(lockManager.tryLock("file-147", "tx-A", 1_000)).isEqualTo(LockManager.LOCK_STATUS.YES);

    final CountDownLatch queued = new CountDownLatch(1);
    final Thread waiter = new Thread(() -> {
      queued.countDown();
      lockManager.tryLock("file-147", "tx-B", 5_000);
    });
    waiter.setDaemon(true);
    waiter.start();

    assertThat(queued.await(5, TimeUnit.SECONDS)).isTrue();
    // The waiter enqueues just after the latch; poll rather than sleep on a fixed guess.
    String description = "";
    for (int i = 0; i < 100 && !description.contains("1 waiters"); ++i) {
      description = String.valueOf(lockManager.describeOwner("file-147"));
      Thread.sleep(10);
    }
    assertThat(description).contains("1 waiters");

    lockManager.unlock("file-147", "tx-A");
    waiter.join(5_000);
  }

  @Test
  void statsSnapshotListsEveryHeldResource() {
    lockManager.tryLock("file-147", "tx-A", 1_000);
    lockManager.tryLock("file-343", "tx-A", 1_000);

    final List<LockManager.LockStats> stats = lockManager.statsSnapshot();
    assertThat(stats).hasSize(2);
    assertThat(stats).extracting(LockManager.LockStats::resource)
        .containsExactlyInAnyOrder("file-147", "file-343");
    assertThat(stats).allSatisfy(s -> {
      assertThat(s.owner()).isEqualTo("tx-A");
      assertThat(s.sinceMillis()).isPositive();
      assertThat(s.heldForMs()).isNotNegative();
      assertThat(s.waiters()).isZero();
    });
  }

  @Test
  void statsSnapshotOmitsReleasedResources() {
    lockManager.tryLock("file-147", "tx-A", 1_000);
    lockManager.tryLock("file-343", "tx-A", 1_000);
    lockManager.unlock("file-147", "tx-A");

    assertThat(lockManager.statsSnapshot()).extracting(LockManager.LockStats::resource)
        .containsExactly("file-343");
  }

  @Test
  void statsSnapshotIsEmptyWhenNothingIsHeld() {
    assertThat(lockManager.statsSnapshot()).isEmpty();
  }

  @Test
  void snapshotDoesNotRetainTheRequesterObject() {
    // The snapshot is handed to diagnostics code and may outlive the transaction; it must carry a
    // rendered owner, not a live reference to a Thread or session.
    lockManager.tryLock("file-147", "tx-A", 1_000);
    final LockManager.LockStats stats = lockManager.statsSnapshot().getFirst();
    assertThat(stats.owner()).isInstanceOf(String.class);
  }
}
