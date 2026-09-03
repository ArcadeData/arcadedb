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
package com.arcadedb.remote;

import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issue #7030: {@link RemoteDatabase#transaction} was missing the two guards
 * {@code LocalDatabase.transaction()} has - it retried a transaction owned by the caller (the guard issue #661
 * established), and it never rolled back a failed attempt before starting the next one.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7030RemoteTransactionRetryIT extends BaseGraphServerTest {
  private static final String DATABASE_NAME = "remote-tx-7030";

  @Override
  protected boolean isCreateDatabases() {
    return false;
  }

  @BeforeEach
  public void beginTest() {
    super.beginTest();
    final RemoteServer server = new RemoteServer("127.0.0.1", 2480, "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
    if (!server.exists(DATABASE_NAME))
      server.create(DATABASE_NAME);
  }

  @AfterEach
  public void endTest() {
    final RemoteServer server = new RemoteServer("127.0.0.1", 2480, "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
    if (server.exists(DATABASE_NAME))
      server.drop(DATABASE_NAME);
    super.endTest();
  }

  /**
   * The bug: joining a transaction the caller owns and then hitting a {@link NeedRetryException} re-ran the block
   * inside that still-open transaction, which silently accumulated the partial effects of every failed attempt.
   * The block must run once and the exception must reach the owner of the transaction.
   */
  @Test
  void aJoinedTransactionIsNotRetried() {
    try (final RollbackCountingDatabase database = newDatabase()) {
      database.command("sql", "CREATE DOCUMENT TYPE Person");

      final AtomicInteger executions = new AtomicInteger();

      database.begin();
      try {
        assertThatThrownBy(() -> database.transaction(() -> {
          executions.incrementAndGet();
          throw new NeedRetryException("simulated conflict");
        }, true, 5)).isInstanceOf(NeedRetryException.class).hasMessageContaining("simulated conflict");

        assertThat(executions.get()).as("a transaction owned by the caller must not be retried").isEqualTo(1);
        assertThat(database.rollbacks.get()).as("the caller's transaction must not be rolled back from here").isZero();
        assertThat(database.isTransactionActive()).as("the caller's transaction must still be open").isTrue();
      } finally {
        if (database.isTransactionActive())
          database.rollback();
      }
    }
  }

  /**
   * The bug: a failed attempt left its server-side transaction open, so the next attempt contended with the locks
   * of the previous one until the server timed it out. Each failed attempt must be rolled back before the retry.
   */
  @Test
  void everyFailedAttemptIsRolledBackBeforeTheRetry() {
    try (final RollbackCountingDatabase database = newDatabase()) {
      database.command("sql", "CREATE DOCUMENT TYPE Person");

      final AtomicInteger executions = new AtomicInteger();

      final boolean createdNewTx = database.transaction(() -> {
        database.newDocument("Person").set("name", "Alice").save();
        if (executions.incrementAndGet() < 3)
          throw new NeedRetryException("simulated conflict");
      }, false, 5);

      assertThat(createdNewTx).isTrue();
      assertThat(executions.get()).isEqualTo(3);
      assertThat(database.rollbacks.get()).as("each of the two failed attempts must be rolled back").isEqualTo(2);
      assertThat(database.countType("Person", false)).as("only the attempt that committed must leave a record")
          .isEqualTo(1);
    }
  }

  /**
   * The same on the arm that gives up: an exception the retry loop does not handle still has to release the
   * server-side transaction on the way out instead of leaving it open until its timeout.
   */
  @Test
  void aFatalFailureRollsBackBeforeRethrowing() {
    try (final RollbackCountingDatabase database = newDatabase()) {
      database.command("sql", "CREATE DOCUMENT TYPE Person");

      assertThatThrownBy(() -> database.transaction(() -> {
        database.newDocument("Person").set("name", "Alice").save();
        throw new IllegalArgumentException("fatal");
      }, false, 5)).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("fatal");

      assertThat(database.rollbacks.get()).isEqualTo(1);
      assertThat(database.isTransactionActive()).isFalse();
      assertThat(database.countType("Person", false)).isZero();
    }
  }

  private RollbackCountingDatabase newDatabase() {
    return new RollbackCountingDatabase("127.0.0.1", 2480, DATABASE_NAME, "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
  }

  /**
   * Counts the {@code POST /rollback} the retry loop issues, which is what tells a failed attempt that released
   * its server-side transaction from one that abandoned it.
   */
  private static class RollbackCountingDatabase extends RemoteDatabase {
    final AtomicInteger rollbacks = new AtomicInteger();

    RollbackCountingDatabase(final String server, final int port, final String databaseName, final String userName,
        final String userPassword) {
      super(server, port, databaseName, userName, userPassword);
    }

    @Override
    public void rollback() {
      rollbacks.incrementAndGet();
      super.rollback();
    }
  }
}
