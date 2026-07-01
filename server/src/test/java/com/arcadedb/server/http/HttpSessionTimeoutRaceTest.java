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
import com.arcadedb.server.security.ServerSecurityUser;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.lang.reflect.Field;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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

  private ServerSecurityUser mockUser(final String name) {
    final ServerSecurityUser user = mock(ServerSecurityUser.class);
    when(user.getName()).thenReturn(name);
    return user;
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

    final ServerSecurityUser user = mockUser("testuser");
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
  void timeoutSweepStillCancelsAGenuinelyIdleSession() throws Exception {
    database = createDatabase();
    sessionManager = new HttpSessionManager(TIMEOUT_MS);

    final TransactionContext tx = new TransactionContext(database);
    tx.begin(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);

    final ServerSecurityUser user = mockUser("testuser");
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

    final ServerSecurityUser user = mockUser("testuser");
    final HttpSession session = sessionManager.createSession(user, tx);

    makeSessionLookExpired(session);

    final int expired = sessionManager.checkSessionsValidity();

    assertThat(expired).as("stale session with an already-committed transaction is reaped").isEqualTo(1);
    assertThat(sessionManager.getActiveSessions()).as("stale session removed").isEqualTo(0);
  }
}
