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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12098: a schema change on the leader must not deadlock with the commits already past the recording-session gate.
 * <p>
 * The cycle, built here one step at a time:
 * <ol>
 *   <li>commit A has replicated and holds its file locks, outside the database lock, and is about to take the shared lock
 *       again to conclude (parked in {@link RaftReplicatedDatabase#TEST_POST_REPLICATION_HOOK});</li>
 *   <li>commit B, on the same bucket, is inside phase 1: it holds the shared lock and waits for A's file locks;</li>
 *   <li>a schema change starts. Before the fix it queued for the exclusive lock straight away: A could no longer re-enter
 *       the shared lock behind the queued writer, the writer waited for B, B waited for A, and only B's
 *       {@code commitLockTimeout} broke it - B failed with {@code LockTimeoutException}.</li>
 * </ol>
 * With the fix the schema change waits for A and B to end before it asks for the exclusive lock, so all three complete.
 */
class Issue12098SchemaChangeDrainsGatedCommitsIT extends BaseRaftHATest {

  private static final String TYPE         = "Issue12098Item";
  private static final long   WAIT_SECONDS = 60;

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected void populateDatabase() {
  }

  @AfterEach
  void clearHook() {
    RaftReplicatedDatabase.TEST_POST_REPLICATION_HOOK = null;
  }

  @Test
  void schemaChangeWaitsForTheCommitsPastTheGateInsteadOfDeadlockingWithThem() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final DatabaseInternal leaderDb = (DatabaseInternal) getServerDatabase(leaderIndex, getDatabaseName());

    leaderDb.getSchema().buildDocumentType().withName(TYPE).withTotalBuckets(1).create();
    leaderDb.transaction(() -> leaderDb.newDocument(TYPE).set("k", "seed").save());

    final CountDownLatch aParked = new CountDownLatch(1);
    final CountDownLatch releaseA = new CountDownLatch(1);
    RaftReplicatedDatabase.TEST_POST_REPLICATION_HOOK = dbName -> {
      if (!"issue12098-commit-A".equals(Thread.currentThread().getName()))
        return;
      aParked.countDown();
      try {
        releaseA.await(WAIT_SECONDS, TimeUnit.SECONDS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    };

    final AtomicReference<Throwable> failureA = new AtomicReference<>();
    final AtomicReference<Throwable> failureB = new AtomicReference<>();
    final AtomicReference<Throwable> failureDdl = new AtomicReference<>();

    final Thread commitA = insertThread(leaderDb, "issue12098-commit-A", "a", failureA);
    commitA.start();
    try {
      assertThat(aParked.await(WAIT_SECONDS, TimeUnit.SECONDS)).as("commit A must reach the post-replication window").isTrue();

      final Thread commitB = insertThread(leaderDb, "issue12098-commit-B", "b", failureB);
      commitB.start();
      awaitInStack(commitB, frame -> frame.getMethodName().equals("tryLockFile"),
          "commit B must be inside phase 1, waiting for the file lock commit A holds");

      final Thread ddl = new Thread(() -> {
        try {
          leaderDb.getSchema().createBucket("issue12098_bucket");
        } catch (final Throwable t) {
          failureDdl.set(t);
        }
      }, "issue12098-ddl");
      ddl.start();
      // Before the fix the schema change queues for the exclusive lock here; with it, it waits for A and B to end
      awaitInStack(ddl, frame -> frame.getMethodName().equals("writeLock") || frame.getMethodName().startsWith("awaitCommitsPast"),
          "the schema change must have reached the point where it waits for the database");

      releaseA.countDown();

      for (final Thread t : new Thread[] { commitA, commitB, ddl }) {
        t.join(TimeUnit.SECONDS.toMillis(WAIT_SECONDS));
        assertThat(t.isAlive()).as("%s must end", t.getName()).isFalse();
      }
    } finally {
      releaseA.countDown();
    }

    assertThat(failureA.get()).as("commit A").isNull();
    assertThat(failureB.get())
        .as("commit B must not wait out commitLockTimeout behind a schema change queued between A's two lock sections (#12098)")
        .isNull();
    assertThat(failureDdl.get()).as("the schema change").isNull();

    waitForAllServers();
    testEachServer(serverIndex -> {
      final Database db = getServerDatabase(serverIndex, getDatabaseName());
      assertThat(db.countType(TYPE, false)).as("records on server %d", serverIndex).isEqualTo(3);
      assertThat(db.getSchema().existsBucket("issue12098_bucket")).as("new bucket on server %d", serverIndex).isTrue();
    });
  }

  private static Thread insertThread(final DatabaseInternal db, final String name, final String key,
      final AtomicReference<Throwable> failure) {
    return new Thread(() -> {
      try {
        db.transaction(() -> db.newDocument(TYPE).set("k", key).save(), false, 1);
      } catch (final Throwable t) {
        failure.set(t);
      }
    }, name);
  }

  private static void awaitInStack(final Thread thread, final Predicate<StackTraceElement> where, final String what)
      throws InterruptedException {
    final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WAIT_SECONDS);
    while (System.nanoTime() < deadline) {
      if (Arrays.stream(thread.getStackTrace()).anyMatch(where))
        return;
      assertThat(thread.isAlive()).as(what + " (the thread ended first)").isTrue();
      Thread.sleep(5);
    }
    throw new AssertionError(what + ": " + Arrays.toString(thread.getStackTrace()));
  }
}
