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

import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10135, remote side: a {@link RemoteDatabase#transaction} call that JOINED the caller's transaction owns
 * nothing, so whatever its block throws must leave the caller's server-side transaction - and the session id that
 * names it - alive. Before the fix the non-retryable arm rolled it back and cleared the session id.
 */
class Issue10135RemoteJoinedTransactionFailureIT extends BaseGraphServerTest {
  private static final String DATABASE_NAME = "remote-tx-10135";

  @Override
  protected boolean isCreateDatabases() {
    return false;
  }

  @BeforeEach
  public void beginTest() {
    super.beginTest();
    final RemoteServer server = new RemoteServer("127.0.0.1", getServerHttpPort(), "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
    if (!server.exists(DATABASE_NAME))
      server.create(DATABASE_NAME);
  }

  @AfterEach
  public void endTest() {
    final RemoteServer server = new RemoteServer("127.0.0.1", getServerHttpPort(), "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
    if (server.exists(DATABASE_NAME))
      server.drop(DATABASE_NAME);
    super.endTest();
  }

  static Stream<Arguments> failures() {
    return Stream.of(//
        Arguments.of("retryable", (Supplier<Throwable>) () -> new NeedRetryException("simulated conflict")),
        Arguments.of("duplicated key", (Supplier<Throwable>) () -> new DuplicatedKeyException("idx", "k", null)),
        Arguments.of("plain RuntimeException", (Supplier<Throwable>) () -> new IllegalStateException("boom")),
        Arguments.of("Error", (Supplier<Throwable>) () -> new AssertionError("fatal")));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("failures")
  void aJoinedBlockThatThrowsLeavesTheCallersTransactionIntact(final String label, final Supplier<Throwable> failure) {
    try (final RollbackCountingDatabase database = newDatabase()) {
      database.command("sql", "CREATE DOCUMENT TYPE Person");

      database.begin();
      try {
        database.newDocument("Person").set("name", "outer").save();
        final String sessionBefore = database.getSessionId();

        final Throwable expected = failure.get();
        assertThatThrownBy(() -> database.transaction(() -> {
          database.newDocument("Person").set("name", "inner").save();
          throwUnchecked(expected);
        }, true, 3)).isSameAs(expected);

        assertThat(database.rollbacks.get()).as("the caller's transaction must not be rolled back on a %s", label).isZero();
        assertThat(database.isTransactionActive()).isTrue();
        assertThat(database.getSessionId()).as("the caller's session must survive").isEqualTo(sessionBefore);

        database.newDocument("Person").set("name", "outer2").save();
        database.commit();
      } finally {
        if (database.isTransactionActive())
          database.rollback();
      }

      assertThat(database.query("sql", "select from Person where name in ['outer', 'outer2']").stream().count())
          .as("the caller committed its own work").isEqualTo(2);
    }
  }

  @Test
  void theDefaultOverloadJoinsAndLeavesTheCallersTransactionIntact() {
    try (final RollbackCountingDatabase database = newDatabase()) {
      database.command("sql", "CREATE DOCUMENT TYPE Person");

      database.transaction(() -> {
        database.newDocument("Person").set("name", "outer").save();
        try {
          database.transaction(() -> {
            throw new IllegalStateException("boom");
          });
        } catch (final IllegalStateException ignore) {
          // EXPECTED
        }
        assertThat(database.isTransactionActive()).isTrue();
        database.newDocument("Person").set("name", "outer2").save();
      });

      assertThat(database.countType("Person", false)).isEqualTo(2);
    }
  }

  /**
   * Not joining while the caller has a transaction open makes {@code begin()} refuse ("Transaction already begun").
   * The session it refused to replace is still the caller's, so it must not be rolled back either.
   */
  @Test
  void aRefusedBeginDoesNotRollBackTheCallersTransaction() {
    try (final RollbackCountingDatabase database = newDatabase()) {
      database.command("sql", "CREATE DOCUMENT TYPE Person");

      final AtomicInteger executions = new AtomicInteger();

      database.begin();
      try {
        database.newDocument("Person").set("name", "outer").save();
        final String sessionBefore = database.getSessionId();

        assertThatThrownBy(() -> database.transaction(executions::incrementAndGet, false, 3)).isInstanceOf(
            TransactionException.class);

        assertThat(executions.get()).isZero();
        assertThat(database.rollbacks.get()).isZero();
        assertThat(database.getSessionId()).isEqualTo(sessionBefore);

        database.commit();
      } finally {
        if (database.isTransactionActive())
          database.rollback();
      }

      assertThat(database.countType("Person", false)).isEqualTo(1);
    }
  }

  /**
   * The transaction this call opened is still released on an {@link Error}, not only on an {@link Exception}.
   */
  @Test
  void anOwnedTransactionIsReleasedOnAnError() {
    try (final RollbackCountingDatabase database = newDatabase()) {
      database.command("sql", "CREATE DOCUMENT TYPE Person");

      assertThatThrownBy(() -> database.transaction(() -> {
        database.newDocument("Person").set("name", "owned").save();
        throw new AssertionError("fatal");
      }, false, 3)).isInstanceOf(AssertionError.class);

      assertThat(database.rollbacks.get()).isEqualTo(1);
      assertThat(database.isTransactionActive()).isFalse();
      assertThat(database.countType("Person", false)).isZero();
    }
  }

  private RollbackCountingDatabase newDatabase() {
    return new RollbackCountingDatabase("127.0.0.1", getServerHttpPort(), DATABASE_NAME, "root",
        BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
  }

  @SuppressWarnings("unchecked")
  private static <T extends Throwable> void throwUnchecked(final Throwable t) throws T {
    throw (T) t;
  }

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
