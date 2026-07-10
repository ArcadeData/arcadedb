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
package com.arcadedb.server.http;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.security.ServerSecurityUser;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.lang.reflect.Field;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for GitHub issue #4857: the {@link HttpSessionManager} idle-timeout sweep ran on a
 * background timer thread and rolled back a session's {@link TransactionContext} directly, without
 * coordinating with {@link HttpSession#execute}'s lock. A command that ran longer than the session
 * timeout (e.g. a large batch of Gremlin mutations under GC/memory pressure) could therefore have its
 * transaction rolled back - nulling {@code TransactionContext.newPages} - while the worker thread was
 * still actively mutating that same transaction, surfacing as an intermittent NPE
 * ("Cannot invoke java.util.Map.containsKey(Object) because this.newPages is null").
 *
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/4857">GitHub Issue #4857</a>
 */
class HttpSessionTimeoutRaceTest {
  // Large enough that the manager's own background sweep timer never fires during a test (tests run in
  // milliseconds), so `checkSessionsValidity()` is only ever driven explicitly and deterministically.
  // Sessions are made to look expired by backdating their `lastUpdate` via reflection instead of sleeping
  // past this value.
  private static final long TIMEOUT_MS = 600_000L;

  private DatabaseInternal   database;
  private File               databaseDirectory;
  private HttpSessionManager sessionManager;

  private DatabaseInternal createDatabase() {
    databaseDirectory = new File("./target/databases/HttpSessionTimeoutRaceTest-" + UUID.randomUUID());
    try (final Database db = new DatabaseFactory(databaseDirectory.getPath()).create()) {
      db.getSchema().createDocumentType("Doc");
    }
    return (DatabaseInternal) new DatabaseFactory(databaseDirectory.getPath()).open();
  }

  // ServerSecurityUser.equals()/getName() (the only members execute()/createSession() touch) never
  // dereference the ArcadeDBServer field, so a real instance can be built without a running server.
  private ServerSecurityUser createUser(final String name) {
    return new ServerSecurityUser(null, new JSONObject().put("name", name));
  }

  private static void makeSessionLookExpired(final HttpSession session) throws ReflectiveOperationException {
    final Field lastUpdate = HttpSession.class.getDeclaredField("lastUpdate");
    lastUpdate.setAccessible(true);
    lastUpdate.setLong(session, System.currentTimeMillis() - (TIMEOUT_MS * 2));
  }

  @AfterEach
  void tearDown() {
    if (sessionManager != null)
      sessionManager.close();
    if (database != null && database.isOpen())
      database.close();
    if (databaseDirectory != null)
      FileUtils.deleteRecursively(databaseDirectory);
  }

  @Test
  void timeoutSweepDoesNotCancelSessionWithInFlightCommand() throws Exception {
    database = createDatabase();
    sessionManager = new HttpSessionManager(TIMEOUT_MS);

    final TransactionContext tx = new TransactionContext(database);
    tx.begin(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);

    final ServerSecurityUser user = createUser("testuser");
    final HttpSession session = sessionManager.createSession(user, tx);

    final CountDownLatch commandStarted = new CountDownLatch(1);
    final CountDownLatch releaseCommand = new CountDownLatch(1);
    final AtomicReference<Exception> workerException = new AtomicReference<>();

    final Thread worker = new Thread(() -> {
      try {
        session.execute(user, () -> {
          commandStarted.countDown();
          if (!releaseCommand.await(5, TimeUnit.SECONDS))
            throw new IllegalStateException("Test did not release the in-flight command in time");
          return null;
        });
      } catch (final Exception e) {
        workerException.set(e);
      }
    });
    worker.start();

    assertThat(commandStarted.await(2, TimeUnit.SECONDS)).as("command started").isTrue();

    // Make the session look expired to the idle-timeout sweep while the command is still executing
    // (blocked on releaseCommand, holding session.execute()'s lock).
    makeSessionLookExpired(session);

    final int expiredByExplicitSweep = sessionManager.checkSessionsValidity();

    // The in-flight command must not have been rolled back or evicted out from under it.
    assertThat(expiredByExplicitSweep).as("sessions expired while command is in-flight").isEqualTo(0);
    assertThat(tx.isActive()).as("transaction still active while command is in-flight").isTrue();
    assertThat(sessionManager.getActiveSessions()).as("session still tracked while command is in-flight").isEqualTo(1);

    releaseCommand.countDown();
    worker.join(5_000);

    assertThat(worker.isAlive()).as("worker thread finished").isFalse();
    assertThat(workerException.get()).as("no exception surfaced from the in-flight command").isNull();
  }

  @Test
  void executeRollsBackOnFailureEvenWhenInterruptFlagIsSet() throws Exception {
    database = createDatabase();
    sessionManager = new HttpSessionManager(TIMEOUT_MS);

    final TransactionContext tx = new TransactionContext(database);
    tx.begin(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);

    final ServerSecurityUser user = createUser("testuser");
    final HttpSession session = sessionManager.createSession(user, tx);

    try {
      // A command whose thread already has its interrupt flag set when it fails (e.g. interrupted by
      // something unrelated) must still roll back the transaction via execute()'s catch block.
      // ReentrantLock.lockInterruptibly() throws immediately if the calling thread is interrupted, even on
      // its own reentrant fast path, so routing this rollback through cancel() (which re-acquires the lock)
      // would silently swallow it.
      assertThatThrownBy(() -> session.execute(user, () -> {
        Thread.currentThread().interrupt();
        throw new RuntimeException("simulated command failure");
      })).isInstanceOf(RuntimeException.class).hasMessage("simulated command failure");
    } finally {
      Thread.interrupted(); // clear the flag so it doesn't leak into later tests
    }

    assertThat(tx.isActive()).as("transaction rolled back despite interrupt flag being set").isFalse();
  }

  @Test
  @Tag("slow")
  void closeWaitsForInFlightCommandThenRollsBackInsteadOfLeaking() throws Exception {
    database = createDatabase();
    sessionManager = new HttpSessionManager(TIMEOUT_MS);

    final TransactionContext tx = new TransactionContext(database);
    tx.begin(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);

    final ServerSecurityUser user = createUser("testuser");
    final HttpSession session = sessionManager.createSession(user, tx);

    final CountDownLatch commandStarted = new CountDownLatch(1);
    final CountDownLatch releaseCommand = new CountDownLatch(1);

    // Hold the command past HttpSession.DEFAULT_TIMEOUT (5s): cancel()'s lock acquisition must not give up
    // after a bounded wait, or close() below would leave the transaction rolled-back-never, since the
    // session is removed from tracking before cancel() is even attempted.
    final Thread worker = new Thread(() -> {
      try {
        session.execute(user, () -> {
          commandStarted.countDown();
          releaseCommand.await(10, TimeUnit.SECONDS);
          return null;
        });
      } catch (final Exception ignored) {
        // Interrupted by close()'s rollback path on error; irrelevant to this test.
      }
    });
    worker.start();
    assertThat(commandStarted.await(2, TimeUnit.SECONDS)).as("command started").isTrue();

    // Close the session while the command is still in-flight, simulating a client abandoning mid-command.
    final Thread closer = new Thread(session::close);
    closer.start();

    Thread.sleep(200);
    assertThat(sessionManager.getActiveSessions()).as("session untracked by close() immediately").isEqualTo(0);
    assertThat(tx.isActive()).as("transaction not yet rolled back while command still in-flight").isTrue();

    // Release well past DEFAULT_TIMEOUT (5s) so the pre-fix bounded cancel() would already have given up.
    Thread.sleep(5_300);
    releaseCommand.countDown();
    worker.join(5_000);
    closer.join(5_000);

    assertThat(tx.isActive()).as("close() rolled back the transaction once the in-flight command finished").isFalse();
  }

  @Test
  void timeoutSweepStillCancelsAGenuinelyIdleSession() throws Exception {
    database = createDatabase();
    sessionManager = new HttpSessionManager(TIMEOUT_MS);

    final TransactionContext tx = new TransactionContext(database);
    tx.begin(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);

    final ServerSecurityUser user = createUser("testuser");
    final HttpSession session = sessionManager.createSession(user, tx);

    makeSessionLookExpired(session);

    final int expired = sessionManager.checkSessionsValidity();

    assertThat(expired).as("idle session expired").isEqualTo(1);
    assertThat(tx.isActive()).as("idle transaction rolled back").isFalse();
    assertThat(sessionManager.getActiveSessions()).as("idle session removed").isEqualTo(0);
  }

  @Test
  void timeoutSweepRemovesASessionWithAnAlreadyInactiveTransaction() throws Exception {
    database = createDatabase();
    sessionManager = new HttpSessionManager(TIMEOUT_MS);

    final TransactionContext tx = new TransactionContext(database);
    tx.begin(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
    tx.commit();

    final ServerSecurityUser user = createUser("testuser");
    final HttpSession session = sessionManager.createSession(user, tx);

    makeSessionLookExpired(session);

    final int expired = sessionManager.checkSessionsValidity();

    assertThat(expired).as("stale session with an already-committed transaction is reaped").isEqualTo(1);
    assertThat(sessionManager.getActiveSessions()).as("stale session removed").isEqualTo(0);
  }
}
