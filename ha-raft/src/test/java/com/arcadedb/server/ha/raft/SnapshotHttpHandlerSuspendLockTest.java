/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.server.ha.raft;

import org.junit.jupiter.api.Test;

import java.util.concurrent.locks.ReentrantLock;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * #5063 (review round 5): {@code PageManagerFlushThread.setSuspended} is ownership-based, so only ONE
 * thread at a time may be inside {@code suspendFlushAndExecute} for a given database or the non-owner
 * can observe a resumed flush mid-read. {@link SnapshotHttpHandler#suspendLockFor} provides the
 * per-database lock that serializes the handler's snapshot and checksums paths; these tests pin its
 * identity contract: same database name, same lock instance; different names, independent locks.
 */
class SnapshotHttpHandlerSuspendLockTest {

  @Test
  void sameDatabaseNameAlwaysMapsToTheSameLockInstance() {
    final SnapshotHttpHandler handler = new SnapshotHttpHandler(null);
    final ReentrantLock first = handler.suspendLockFor("mydb");
    assertThat(handler.suspendLockFor("mydb")).isSameAs(first);
  }

  @Test
  void differentDatabaseNamesDoNotShareALock() {
    final SnapshotHttpHandler handler = new SnapshotHttpHandler(null);
    assertThat(handler.suspendLockFor("db1")).isNotSameAs(handler.suspendLockFor("db2"));
  }

  @Test
  void lockIsHeldExclusivelyAcrossThreads() throws Exception {
    final SnapshotHttpHandler handler = new SnapshotHttpHandler(null);
    final ReentrantLock lock = handler.suspendLockFor("mydb");
    lock.lock();
    try {
      final boolean[] acquiredByOtherThread = { true };
      final Thread other = new Thread(() -> acquiredByOtherThread[0] = handler.suspendLockFor("mydb").tryLock());
      other.start();
      other.join(10_000);
      assertThat(acquiredByOtherThread[0])
          .as("a second thread must not enter the snapshot suspend section for the same database")
          .isFalse();
    } finally {
      lock.unlock();
    }
  }
}
