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
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for GitHub issue #5026 (server audit, HTTP transaction-session lifecycle). Covers the
 * two lower-level defects that can be exercised directly against {@link HttpSessionManager}/{@link HttpSession}
 * without a running HTTP server:
 * <ul>
 *   <li>Defect 2: the idle-timeout sweep can remove a session between {@code getSessionById()} and
 *   {@link HttpSession#execute} acquiring the session lock; {@code execute} must re-validate the session is
 *   still registered and refuse to run its callback on a swept (rolled-back) transaction.</li>
 *   <li>Defect 3: {@link HttpSessionManager#getSessionById} must enforce ownership (a different user cannot
 *   resolve another user's session), and a user's live sessions must be invalidatable
 *   ({@link HttpSessionManager#removeSessionsForUser}) so a dropped-and-recreated same-name principal cannot
 *   adopt the prior principal's still-open session.</li>
 * </ul>
 *
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/5026">GitHub Issue #5026</a>
 */
class Issue5026HttpSessionLifecycleTest {
  private static final long TIMEOUT_MS = 600_000L;

  private DatabaseInternal   database;
  private File               databaseDirectory;
  private HttpSessionManager sessionManager;

  private DatabaseInternal createDatabase() {
    databaseDirectory = new File("./target/databases/Issue5026HttpSessionLifecycleTest-" + UUID.randomUUID());
    try (final Database db = new DatabaseFactory(databaseDirectory.getPath()).create()) {
      db.getSchema().createDocumentType("Doc");
    }
    return (DatabaseInternal) new DatabaseFactory(databaseDirectory.getPath()).open();
  }

  // ServerSecurityUser.equals()/getName() (the only members the session manager touches) never dereference
  // the ArcadeDBServer field, so a real instance can be built without a running server.
  private ServerSecurityUser createUser(final String name) {
    return new ServerSecurityUser(null, new JSONObject().put("name", name));
  }

  private TransactionContext beginTx() {
    final TransactionContext tx = new TransactionContext(database);
    tx.begin(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
    return tx;
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

  // Defect 2: a session removed from the manager (e.g. by the idle sweep) between lookup and lock acquisition
  // must not have its callback run; execute() re-validates registration under the lock.
  @Test
  void executeRefusesToRunOnAnUnregisteredSession() throws Exception {
    database = createDatabase();
    sessionManager = new HttpSessionManager(TIMEOUT_MS);

    final TransactionContext tx = beginTx();
    final ServerSecurityUser user = createUser("alice");
    final HttpSession session = sessionManager.createSession(user, tx);

    // Simulate the idle sweep having rolled back and removed the session after this request looked it up.
    tx.rollback();
    sessionManager.removeSession(session.id);

    final AtomicBoolean callbackRan = new AtomicBoolean(false);
    assertThatThrownBy(() -> session.execute(user, () -> {
      callbackRan.set(true);
      return null;
    })).isInstanceOf(HttpSessionException.class);

    assertThat(callbackRan).as("callback must not run on a session that was removed before the lock was taken").isFalse();
  }

  // A session that is still registered runs its callback normally (guards against the re-validation being too
  // aggressive and breaking the happy path).
  @Test
  void executeRunsCallbackWhenSessionStillRegistered() throws Exception {
    database = createDatabase();
    sessionManager = new HttpSessionManager(TIMEOUT_MS);

    final TransactionContext tx = beginTx();
    final ServerSecurityUser user = createUser("alice");
    final HttpSession session = sessionManager.createSession(user, tx);

    final AtomicBoolean callbackRan = new AtomicBoolean(false);
    session.execute(user, () -> {
      callbackRan.set(true);
      return null;
    });

    assertThat(callbackRan).isTrue();
  }

  // Defect 3: getSessionById must not hand a session to a different user.
  @Test
  void getSessionByIdEnforcesOwnership() {
    database = createDatabase();
    sessionManager = new HttpSessionManager(TIMEOUT_MS);

    final ServerSecurityUser alice = createUser("alice");
    final ServerSecurityUser bob = createUser("bob");
    final HttpSession session = sessionManager.createSession(alice, beginTx());

    assertThat(sessionManager.getSessionById(alice, session.id)).as("owner resolves its own session").isSameAs(session);
    assertThat(sessionManager.getSessionById(bob, session.id)).as("a different user cannot resolve the session").isNull();
  }

  // Defect 3: removeSessionsForUser rolls back and removes only the target user's sessions.
  @Test
  void removeSessionsForUserInvalidatesOnlyThatUsersSessions() {
    database = createDatabase();
    sessionManager = new HttpSessionManager(TIMEOUT_MS);

    final ServerSecurityUser alice = createUser("alice");
    final ServerSecurityUser bob = createUser("bob");
    final TransactionContext aliceTx = beginTx();
    final TransactionContext bobTx = beginTx();
    final HttpSession aliceSession = sessionManager.createSession(alice, aliceTx);
    final HttpSession bobSession = sessionManager.createSession(bob, bobTx);

    final int removed = sessionManager.removeSessionsForUser("alice");

    assertThat(removed).as("only alice's session removed").isEqualTo(1);
    assertThat(aliceTx.isActive()).as("alice's transaction rolled back").isFalse();
    assertThat(bobTx.isActive()).as("bob's transaction untouched").isTrue();
    assertThat(sessionManager.getActiveSessions()).isEqualTo(1);
    assertThat(sessionManager.getSessionById(alice, aliceSession.id)).isNull();
    assertThat(sessionManager.getSessionById(bob, bobSession.id)).isSameAs(bobSession);
  }

  // Defect 3: after invalidation a recreated same-name principal (a fresh ServerSecurityUser instance) cannot
  // adopt the prior principal's session id.
  @Test
  void recreatedSameNameUserCannotAdoptPriorSession() {
    database = createDatabase();
    sessionManager = new HttpSessionManager(TIMEOUT_MS);

    final ServerSecurityUser alice = createUser("alice");
    final HttpSession aliceSession = sessionManager.createSession(alice, beginTx());

    // User is dropped: invalidate its live sessions.
    sessionManager.removeSessionsForUser("alice");

    // A brand new principal happens to reuse the same name.
    final ServerSecurityUser recreatedAlice = createUser("alice");
    assertThat(sessionManager.getSessionById(recreatedAlice, aliceSession.id))
        .as("recreated same-name user must not adopt the prior principal's session").isNull();
  }
}
