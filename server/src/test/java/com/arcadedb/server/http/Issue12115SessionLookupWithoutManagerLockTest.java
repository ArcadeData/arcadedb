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
import org.junit.jupiter.api.Test;

import java.io.File;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12115: every request of an HTTP transaction session resolved its session under the manager's single fair read-write
 * lock, and the idle sweep held the write side of it while it rolled expired sessions back. The lookups no longer take a
 * manager-wide lock; these tests pin that, and the invariants the lock used to give that are now kept another way:
 * <ul>
 * <li>a request that resolved a session before the sweep ended it runs nothing in it afterwards;</li>
 * <li>the sweep leaves a session that another request already ended to that request;</li>
 * <li>a database counts as in use by its expired session until the sweep has finished rolling that session back;</li>
 * <li>{@link HttpSessionManager#removeSessionsForUser} leaves none of the user's sessions resolvable.</li>
 * </ul>
 */
class Issue12115SessionLookupWithoutManagerLockTest {
  // Large enough that the manager's own sweep timer never fires during a test: the sweep runs only when a test calls it
  private static final long TIMEOUT_MS      = 600_000L;
  // Hang detector, not a latency bound: an operation waiting on another thread's rollback never completes before it is released
  private static final long HANG_SECONDS    = 30;
  private DatabaseInternal   database;
  private File               databaseDirectory;
  private HttpSessionManager sessionManager;
  private ExecutorService    executor;

  /** A transaction whose rollback parks until released: the sweep's rollback of it stays in progress for as long as needed. */
  private static final class ParkingRollbackTransaction extends TransactionContext {
    private final CountDownLatch rollbackEntered = new CountDownLatch(1);
    private final CountDownLatch releaseRollback = new CountDownLatch(1);

    private ParkingRollbackTransaction(final DatabaseInternal database) {
      super(database);
    }

    @Override
    public void rollback() {
      rollbackEntered.countDown();
      try {
        // Longer than the hang detector, so a lookup that waits for this rollback fails the test instead of being let through
        // by the rollback's own timeout. Every test releases it in a finally block
        releaseRollback.await(HANG_SECONDS * 4, TimeUnit.SECONDS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      super.rollback();
    }
  }

  @AfterEach
  void tearDown() {
    if (executor != null)
      executor.shutdownNow();
    if (sessionManager != null)
      sessionManager.close();
    if (database != null && database.isOpen())
      database.close();
    if (databaseDirectory != null)
      FileUtils.deleteRecursively(databaseDirectory);
  }

  @Test
  void lookupsDoNotWaitForTheSweepRollingBackAnExpiredSession() throws Exception {
    openDatabase();
    final ServerSecurityUser user = user("alice");
    final ParkingRollbackTransaction expiringTx = beginParking();
    final HttpSession expiring = sessionManager.createSession(user, expiringTx);
    final HttpSession live = sessionManager.createSession(user, begin());
    makeLookExpired(expiring);

    final Future<Integer> sweep = executor.submit(sessionManager::checkSessionsValidity);
    assertThat(expiringTx.rollbackEntered.await(HANG_SECONDS, TimeUnit.SECONDS)).as("the sweep reached the rollback").isTrue();

    try {
      // The sweep is in the middle of rolling the expired session back: the requests of every other session go on
      assertThat(withoutWaiting(() -> sessionManager.getSessionById(user, live.id))).isSameAs(live);
      assertThat(withoutWaiting(() -> sessionManager.isSessionRegistered(live.id))).isTrue();
      final HttpSession begun = withoutWaiting(() -> sessionManager.createSession(user, null));
      assertThat(withoutWaiting(() -> sessionManager.removeSession(begun.id))).isSameAs(begun);

      // and the expired one is still registered, read without waiting: its transaction is on no thread, so until the rollback
      // is over the registration is what keeps the idle-database closer away from the database
      assertThat(withoutWaiting(() -> sessionManager.isSessionRegistered(expiring.id))).isTrue();
    } finally {
      expiringTx.releaseRollback.countDown();
    }

    assertThat(sweep.get(HANG_SECONDS, TimeUnit.SECONDS)).as("sessions the sweep ended").isEqualTo(1);
    assertThat(expiringTx.isActive()).isFalse();
    assertThat(sessionManager.getSessionById(user, expiring.id)).isNull();
    assertThat(sessionManager.isSessionRegistered(expiring.id)).isFalse();
    assertThat(sessionManager.getSessions()).containsExactly(live);
  }

  @Test
  void databaseCountsAsInUseUntilTheSweepHasRolledTheExpiredSessionBack() throws Exception {
    openDatabase();
    final ParkingRollbackTransaction tx = beginParking();
    final HttpSession session = sessionManager.createSession(user("alice"), tx);
    makeLookExpired(session);

    final Future<Integer> sweep = executor.submit(sessionManager::checkSessionsValidity);
    assertThat(tx.rollbackEntered.await(HANG_SECONDS, TimeUnit.SECONDS)).as("the sweep reached the rollback").isTrue();

    try {
      // What the server closing idle databases asks before it closes one: the rollback is still running on this database
      assertThat(withoutWaiting(() -> sessionManager.hasSessionOnDatabase(database.getName()))).isTrue();
      assertThat(withoutWaiting(sessionManager::getSessionDatabaseNames)).containsExactly(database.getName());
    } finally {
      tx.releaseRollback.countDown();
    }

    assertThat(sweep.get(HANG_SECONDS, TimeUnit.SECONDS)).isEqualTo(1);
    assertThat(tx.isActive()).isFalse();
    assertThat(sessionManager.hasSessionOnDatabase(database.getName())).isFalse();
    assertThat(sessionManager.getSessionDatabaseNames()).isEmpty();
  }

  @Test
  void requestThatResolvedTheSessionBeforeTheSweepRunsNothingInItAfterwards() throws Exception {
    openDatabase();
    final ServerSecurityUser user = user("alice");
    final ParkingRollbackTransaction tx = beginParking();
    final HttpSession session = sessionManager.createSession(user, tx);

    // The request resolves its session, then the sweep ends it before the request takes the session lock
    assertThat(sessionManager.getSessionById(user, session.id)).isSameAs(session);
    makeLookExpired(session);
    final Future<Integer> sweep = executor.submit(sessionManager::checkSessionsValidity);
    assertThat(tx.rollbackEntered.await(HANG_SECONDS, TimeUnit.SECONDS)).as("the sweep reached the rollback").isTrue();

    final AtomicBoolean ran = new AtomicBoolean();
    final Future<Exception> request = executor.submit(() -> {
      try {
        session.execute(user, () -> {
          ran.set(true);
          return null;
        });
        return null;
      } catch (final Exception e) {
        return e;
      }
    });
    try {
      // The request waits for the session lock the sweep holds for the rollback
      final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(HANG_SECONDS);
      while (!session.hasWaitingRequests() && System.nanoTime() < deadline)
        Thread.sleep(1);
      assertThat(session.hasWaitingRequests()).as("the request queued on the session lock").isTrue();
    } finally {
      tx.releaseRollback.countDown();
    }
    assertThat(sweep.get(HANG_SECONDS, TimeUnit.SECONDS)).isEqualTo(1);
    assertThat(request.get(HANG_SECONDS, TimeUnit.SECONDS)).isInstanceOf(HttpSessionEndedException.class);
    assertThat(ran.get()).as("the request ran something in the session the sweep ended").isFalse();
  }

  @Test
  void sweepLeavesASessionAnotherRequestEndedToThatRequest() throws Exception {
    openDatabase();
    final TransactionContext tx = begin();
    final HttpSession session = sessionManager.createSession(user("alice"), tx);

    // A /commit or /rollback unregistered it and ends the transaction itself: the sweep must not roll it back under it
    assertThat(sessionManager.removeSession(session.id)).isSameAs(session);
    assertThat(session.cancelIfIdle()).isEqualTo(HttpSession.IdleCancelOutcome.ENDED);
    assertThat(tx.isActive()).isTrue();
    tx.rollback();
  }

  @Test
  void removingAUsersSessionsLeavesNoneOfThemResolvable() throws Exception {
    openDatabase();
    final ServerSecurityUser alice = user("alice");
    final ServerSecurityUser bob = user("bob");
    final TransactionContext aliceTx1 = begin();
    final TransactionContext aliceTx2 = begin();
    final TransactionContext bobTx = begin();
    final HttpSession alice1 = sessionManager.createSession(alice, aliceTx1);
    final HttpSession alice2 = sessionManager.createSession(alice, aliceTx2);
    final HttpSession bobSession = sessionManager.createSession(bob, bobTx);
    sessionManager.recordCommitted("AS-committed-by-alice", alice);

    assertThat(sessionManager.removeSessionsForUser("alice")).isEqualTo(2);

    assertThat(sessionManager.getSessionById(alice, alice1.id)).isNull();
    assertThat(sessionManager.getSessionById(alice, alice2.id)).isNull();
    assertThat(aliceTx1.isActive()).isFalse();
    assertThat(aliceTx2.isActive()).isFalse();
    assertThat(sessionManager.wasCommittedBy(alice, "AS-committed-by-alice")).isFalse();
    assertThat(sessionManager.getSessionById(bob, bobSession.id)).isSameAs(bobSession);
    assertThat(bobTx.isActive()).isTrue();
    assertThat(sessionManager.removeSessionsForUser("alice")).isZero();
  }

  @Test
  void concurrentBeginLookupAndEndKeepTheRegistryConsistent() throws Exception {
    sessionManager = new HttpSessionManager(TIMEOUT_MS);
    executor = Executors.newFixedThreadPool(9);
    final ServerSecurityUser user = user("alice");
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final AtomicBoolean stop = new AtomicBoolean();

    final List<Future<?>> clients = new ArrayList<>();
    for (int t = 0; t < 8; t++)
      clients.add(executor.submit(() -> {
        try {
          for (int i = 0; i < 2_000; i++) {
            final HttpSession session = sessionManager.createSession(user, null);
            if (sessionManager.getSessionById(user, session.id) != session || !sessionManager.isSessionRegistered(session.id))
              throw new AssertionError("a session just begun does not resolve to itself");
            sessionManager.recordCommitted(session.id, user);
            if (sessionManager.removeSession(session.id) != session || sessionManager.isSessionRegistered(session.id))
              throw new AssertionError("a session just ended is still registered");
          }
        } catch (final Throwable e) {
          failure.compareAndSet(null, e);
        }
      }));
    final Future<?> sweeper = executor.submit(() -> {
      while (!stop.get()) {
        sessionManager.checkSessionsValidity();
        sessionManager.getSessions();
        sessionManager.getSessionDatabaseNames();
      }
    });

    for (final Future<?> client : clients)
      client.get(HANG_SECONDS, TimeUnit.SECONDS);
    stop.set(true);
    sweeper.get(HANG_SECONDS, TimeUnit.SECONDS);

    assertThat(failure.get()).isNull();
    assertThat(sessionManager.getActiveSessions()).isZero();
    // 16,000 commits remembered past the cap: the oldest were dropped, from many threads at once
    assertThat(sessionManager.getRememberedCommits()).isEqualTo(HttpSessionManager.MAX_REMEMBERED_COMMITS);
  }

  private <T> T withoutWaiting(final Callable<T> call) throws Exception {
    // Run on another thread so a call that parks behind the sweep fails the test instead of hanging it
    return executor.submit(call).get(HANG_SECONDS, TimeUnit.SECONDS);
  }

  private void openDatabase() {
    databaseDirectory = new File("./target/databases/Issue12115SessionLookupWithoutManagerLockTest-" + UUID.randomUUID());
    new DatabaseFactory(databaseDirectory.getPath()).create().close();
    database = (DatabaseInternal) new DatabaseFactory(databaseDirectory.getPath()).open();
    sessionManager = new HttpSessionManager(TIMEOUT_MS);
    executor = Executors.newCachedThreadPool();
  }

  private TransactionContext begin() {
    final TransactionContext tx = new TransactionContext(database);
    tx.begin(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
    return tx;
  }

  private ParkingRollbackTransaction beginParking() {
    final ParkingRollbackTransaction tx = new ParkingRollbackTransaction(database);
    tx.begin(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
    return tx;
  }

  // ServerSecurityUser.equals()/getName() never dereference the server, so a real instance can be built without one
  private static ServerSecurityUser user(final String name) {
    return new ServerSecurityUser(null, new JSONObject().put("name", name));
  }

  private static void makeLookExpired(final HttpSession session) throws ReflectiveOperationException {
    final Field lastUpdate = HttpSession.class.getDeclaredField("lastUpdate");
    lastUpdate.setAccessible(true);
    lastUpdate.setLong(session, System.currentTimeMillis() - TIMEOUT_MS * 2);
  }
}
