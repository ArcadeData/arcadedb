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
import com.arcadedb.database.DatabaseContext;
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
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9312: once a failed command rolled the session's transaction back, the refusal of every request that would run in it
 * was keyed on {@code rollbackOnFailure}, so the /ws insert session (which passes false, to not destroy a transaction on its
 * own refusals, yet writes into that transaction) autocommitted its rows into the gap. The refusal now keys on whether the
 * request joins the transaction.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9312RolledBackSessionRefusesJoiningWorkTest {
  private DatabaseInternal   database;
  private File               databaseDirectory;
  private HttpSessionManager sessionManager;

  private HttpSession rolledBackSession(final ServerSecurityUser user) throws Exception {
    databaseDirectory = new File("./target/databases/Issue9312-" + UUID.randomUUID());
    try (final Database db = new DatabaseFactory(databaseDirectory.getPath()).create()) {
      db.getSchema().createDocumentType("Doc");
    }
    database = (DatabaseInternal) new DatabaseFactory(databaseDirectory.getPath()).open();
    sessionManager = new HttpSessionManager(600_000L);

    final TransactionContext tx = new TransactionContext(database);
    tx.begin(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
    final HttpSession session = sessionManager.createSession(user, tx);
    DatabaseContext.INSTANCE.init(database, tx);

    // a command that writes and then fails: the session rolls the transaction back
    assertThatThrownBy(() -> session.execute(user, () -> {
      database.newDocument("Doc").set("n", 1).save();
      throw new IllegalStateException("boom");
    })).isInstanceOf(IllegalStateException.class);
    assertThat(session.isRolledBackByFailure()).isTrue();
    return session;
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
  void aRequestThatJoinsTheTransactionIsRefusedEvenWhenItDoesNotRollBackOnFailure() throws Exception {
    final ServerSecurityUser user = new ServerSecurityUser(null, new JSONObject().put("name", "u"));
    final HttpSession session = rolledBackSession(user);
    final AtomicInteger ran = new AtomicInteger();

    assertThatThrownBy(() -> session.execute(user, () -> {
      ran.incrementAndGet();
      return null;
    }, false, false, true)).isInstanceOf(HttpSessionException.class).hasMessageContaining("rolled back");

    assertThat(ran.get()).as("the work never ran, so nothing autocommitted").isZero();
  }

  @Test
  void aRequestThatDoesNotJoinTheTransactionStillRuns() throws Exception {
    final ServerSecurityUser user = new ServerSecurityUser(null, new JSONObject().put("name", "u"));
    final HttpSession session = rolledBackSession(user);
    final AtomicInteger ran = new AtomicInteger();

    session.execute(user, () -> {
      ran.incrementAndGet();
      return null;
    }, false, false, false);

    assertThat(ran.get()).isEqualTo(1);
  }

  @Test
  void theEndingRoutesStillRun() throws Exception {
    final ServerSecurityUser user = new ServerSecurityUser(null, new JSONObject().put("name", "u"));
    final HttpSession session = rolledBackSession(user);
    final AtomicInteger ran = new AtomicInteger();

    session.execute(user, () -> {
      ran.incrementAndGet();
      return null;
    }, true, true);

    assertThat(ran.get()).isEqualTo(1);
  }
}
