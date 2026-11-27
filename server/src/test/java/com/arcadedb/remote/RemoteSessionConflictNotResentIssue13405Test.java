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

import com.arcadedb.database.Database;
import com.arcadedb.event.BeforeRecordUpdateListener;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #13405: inside a transaction opened with {@link RemoteDatabase#begin()}, a command that fails on the server with a
 * retryable conflict (503) makes the server roll the session's transaction back. The client used to wait and send the same
 * command again in the same session, where it could only be refused ("was rolled back after a failed command"): the caller got
 * a {@link TransactionException} instead of the conflict, so {@link RemoteDatabase#transaction} did not retry the block. The
 * client now raises the server's answer at once, and the retry of the whole block is the transaction owner's.
 *
 * @author robfrank (ro.franchini@gmail.com)
 */
class RemoteSessionConflictNotResentIssue13405Test extends BaseGraphServerTest {
  private static final String ROWS    = "Issue13405Row";
  private static final String TRIGGER = "Issue13405Trigger";
  private static final int    SIZE    = 5;
  private static final String SCRIPT  = "UPDATE " + ROWS + " SET seq = seq + 1; UPDATE " + TRIGGER + " SET seq = seq + 1;";

  private final AtomicInteger              conflicts         = new AtomicInteger();
  private final AtomicInteger              conflictsToRaise  = new AtomicInteger(Integer.MAX_VALUE);
  private       BeforeRecordUpdateListener conflict;

  @BeforeEach
  void createSchemaAndConflict() {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.command("sql", "CREATE VERTEX TYPE " + ROWS);
    database.command("sql", "CREATE PROPERTY " + ROWS + ".seq INTEGER");
    database.command("sql", "CREATE VERTEX TYPE " + TRIGGER);
    database.transaction(() -> {
      for (int i = 0; i < SIZE; i++)
        database.newVertex(ROWS).set("seq", 0).save();
      database.newVertex(TRIGGER).set("seq", 0).save();
    });

    // The second statement of the script fails the way a genuine MVCC conflict on it would, after the first one wrote
    conflict = record -> {
      if (record instanceof Vertex vertex && vertex.getTypeName().equals(TRIGGER) && conflictsToRaise.getAndDecrement() > 0) {
        conflicts.incrementAndGet();
        throw new ConcurrentModificationException("simulated conflict on the second statement (issue #13405)");
      }
      return true;
    };
    database.getEvents().registerListener(conflict);
  }

  @AfterEach
  void unregisterConflict() {
    getServerDatabase(0, getDatabaseName()).getEvents().unregisterListener(conflict);
  }

  @Test
  void aConflictInsideASessionReachesTheCallerAndIsNotResent() {
    try (final CommandCountingDatabase db = remoteDatabase()) {
      db.begin();
      try {
        assertThatThrownBy(() -> db.command("sqlscript", SCRIPT))
            .as("the conflict the server answered, not the refusal of a resend in the rolled back session")
            .isInstanceOf(ConcurrentModificationException.class)
            .isNotInstanceOf(TransactionException.class);
      } finally {
        if (db.isTransactionActive())
          db.rollback();
      }

      assertThat(db.commands.get()).as("the command was sent once").isEqualTo(1);
    }

    assertThat(conflicts.get()).as("the command ran once").isEqualTo(1);
    assertThat(countRowsWithSeq(ROWS, 1)).as("the rolled back transaction left nothing").isZero();
  }

  /**
   * The writes of an earlier command of the session are lost with the transaction the failed command rolled back: the session
   * must not go on, or the commit would report success for a transaction that lost part of its work.
   */
  @Test
  void aSessionThatLostEarlierWritesToAFailedCommandRefusesTheCommit() {
    try (final CommandCountingDatabase db = remoteDatabase()) {
      db.begin();
      try {
        db.command("sql", "UPDATE " + ROWS + " SET seq = seq + 1");
        assertThatThrownBy(() -> db.command("sql", "UPDATE " + TRIGGER + " SET seq = seq + 1"))
            .isInstanceOf(ConcurrentModificationException.class);

        assertThatThrownBy(db::commit).as("the commit of a transaction that lost the first update")
            .isInstanceOf(TransactionException.class);
      } finally {
        if (db.isTransactionActive())
          db.rollback();
      }
    }

    assertThat(countRowsWithSeq(ROWS, 1)).as("nothing of the rolled back transaction was committed").isZero();
  }

  @Test
  void aTransactionBlockIsRetriedOnAConflictOfOneOfItsCommands() {
    conflictsToRaise.set(1);

    try (final CommandCountingDatabase db = remoteDatabase()) {
      final AtomicInteger executions = new AtomicInteger();
      db.transaction(() -> {
        executions.incrementAndGet();
        db.command("sqlscript", SCRIPT);
      }, false, 3);

      assertThat(executions.get()).as("the block failed on the conflict once, then committed").isEqualTo(2);
      assertThat(db.commands.get()).as("one command per attempt of the block, none resent").isEqualTo(2);
    }

    assertThat(conflicts.get()).isEqualTo(1);
    assertThat(countRowsWithSeq(ROWS, 1)).as("the committed attempt updated every row once").isEqualTo(SIZE);
    assertThat(countRowsWithSeq(TRIGGER, 1)).isEqualTo(1);
  }

  private long countRowsWithSeq(final String type, final int seq) {
    try (final ResultSet rs = getServerDatabase(0, getDatabaseName()).query("sql",
        "SELECT count(*) AS c FROM " + type + " WHERE seq = ?", seq)) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  private CommandCountingDatabase remoteDatabase() {
    return new CommandCountingDatabase(getServerHttpPort(0), getDatabaseName());
  }

  /** Counts the requests sent to the command route, which is what tells a command resent by the client from one sent once. */
  private static class CommandCountingDatabase extends RemoteDatabase {
    final AtomicInteger commands = new AtomicInteger();

    CommandCountingDatabase(final int port, final String databaseName) {
      super("127.0.0.1", port, databaseName, "root", DEFAULT_PASSWORD_FOR_TESTS);
    }

    @Override
    HttpResponse<String> sendWithWatchdog(final HttpRequest request) throws IOException, InterruptedException {
      if (request.uri().getPath().contains("/api/v1/command/"))
        commands.incrementAndGet();
      return super.sendWithWatchdog(request);
    }
  }
}
