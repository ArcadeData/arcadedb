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
package com.arcadedb.database;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.DatabaseNotAvailableException;
import com.arcadedb.exception.DatabaseOperationException;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10881: {@link LocalDatabase#awaitCommitsInFlight()} waited only for the holders of the database lock, and an HA leader
 * commit goes on after it leaves the lock - quorum, publication, acknowledgement - so a restore that refused writes and awaited
 * the commits in flight swapped its target away under a commit the client was then told had succeeded. A commit that goes on
 * outside the lock now registers itself under it, and the await waits for it to end.
 * <p>
 * The HA commit path itself is driven by {@code Issue10881RestoreWaitsForReplicatingCommitIT} in ha-raft. This drives the engine
 * half: the counter, the await, and the schema change that waited for the write lock while the refusal went up.
 */
class Issue10881CommitsInFlightOutsideLockTest extends TestHelper {

  private static final String REASON = "issue 10881";

  @Override
  protected void endTest() {
    local().acceptWrites(REASON);
  }

  private LocalDatabase local() {
    return (LocalDatabase) ((DatabaseInternal) database).getEmbedded();
  }

  @Test
  void theAwaitWaitsForACommitRegisteredAsInFlightOutsideTheLock() throws Exception {
    final LocalDatabase local = local();
    // What an HA leader's phase 1 does: registered under the shared lock, after the refusal check, then the lock is released
    local.executeInReadLock(() -> {
      local.beginCommitInFlight();
      return null;
    });
    assertThat(local.getCommitsInFlight()).isEqualTo(1);

    final ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      local.refuseWrites(REASON, true);
      final Future<?> await = executor.submit(local::awaitCommitsInFlight);
      // Expected to time out: the commit is still on its way to quorum
      assertThatThrownBy(() -> await.get(500, TimeUnit.MILLISECONDS)).isInstanceOf(TimeoutException.class);

      local.endCommitInFlight();
      await.get(30, TimeUnit.SECONDS);
      assertThat(local.getCommitsInFlight()).isZero();
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  void theAwaitReturnsAtOnceWithNothingInFlight() {
    local().refuseWrites(REASON, true);
    local().awaitCommitsInFlight();
    assertThat(local().getCommitsInFlight()).isZero();
  }

  @Test
  void anInterruptedAwaitGivesUpAndKeepsTheInterruptFlag() throws Exception {
    final LocalDatabase local = local();
    local.executeInReadLock(() -> {
      local.beginCommitInFlight();
      return null;
    });
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final AtomicBoolean flagKept = new AtomicBoolean();
    final Thread waiter = new Thread(() -> {
      try {
        local.awaitCommitsInFlight();
      } catch (final Throwable t) {
        failure.set(t);
        flagKept.set(Thread.currentThread().isInterrupted());
      }
    });
    try {
      waiter.start();
      Awaitility.await().atMost(30, TimeUnit.SECONDS)
          .until(() -> waiter.getState() == Thread.State.TIMED_WAITING || waiter.getState() == Thread.State.WAITING);
      waiter.interrupt();
      waiter.join(30_000);
      assertThat(failure.get()).isInstanceOf(DatabaseOperationException.class).hasMessageContaining("in flight");
      assertThat(flagKept.get()).isTrue();
    } finally {
      local.endCommitInFlight();
    }
  }

  /**
   * A schema change checked the refusal before it waited for the write lock, and nowhere after. A refusal raised meanwhile was
   * followed by an await that took the lock first and returned, and the schema change then ran unrefused under a restore that
   * had already stopped waiting for writers.
   */
  @Test
  void aSchemaChangeThatWaitedForTheLockWhileTheRefusalWentUpIsRefused() throws Exception {
    final LocalDatabase local = local();
    final CountDownLatch holding = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    final AtomicBoolean callbackRan = new AtomicBoolean();
    final ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      final Future<?> holder = executor.submit(() -> local.executeInWriteLock(() -> {
        holding.countDown();
        release.await();
        return null;
      }));
      assertThat(holding.await(30, TimeUnit.SECONDS)).isTrue();

      final AtomicReference<Thread> schemaThread = new AtomicReference<>();
      final Future<?> schemaChange = executor.submit(() -> {
        schemaThread.set(Thread.currentThread());
        return local.recordFileChanges(() -> {
          callbackRan.set(true);
          return null;
        });
      });
      // Parked on the write lock: past the check made before it
      Awaitility.await().atMost(30, TimeUnit.SECONDS).until(() -> {
        final Thread t = schemaThread.get();
        return t != null && (t.getState() == Thread.State.WAITING || t.getState() == Thread.State.TIMED_WAITING);
      });

      local.refuseWrites(REASON, true);
      release.countDown();
      holder.get(30, TimeUnit.SECONDS);

      assertThatThrownBy(() -> schemaChange.get(30, TimeUnit.SECONDS)).hasCauseInstanceOf(DatabaseNotAvailableException.class);
      assertThat(callbackRan.get()).as("the schema change must not run under the refusal").isFalse();
    } finally {
      release.countDown();
      executor.shutdownNow();
    }
  }
}
