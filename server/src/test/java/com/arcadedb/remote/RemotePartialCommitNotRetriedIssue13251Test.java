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
import com.arcadedb.exception.ArcadeDBException;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URI;
import java.util.Base64;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #13251: a request whose block committed part of its work (a statement with a {@code BATCH n} boundary, the batches of a
 * Cypher {@code CALL { ... } IN TRANSACTIONS}) and then failed on a conflict is not replayed by the server's retry loop, but the
 * conflict used to leave the server as the retryable one it is: HTTP 503 with {@code Retry-After: 0}. The Java client resends a
 * request answered so, and every resend ran the statement again and applied the committed batches once more. The server now
 * answers a failure no client retries, so the committed part stands exactly once.
 *
 * @author robfrank (ro.franchini@gmail.com)
 */
class RemotePartialCommitNotRetriedIssue13251Test extends BaseGraphServerTest {
  private static final String ROWS    = "Issue13251Row";
  private static final String TRIGGER = "Issue13251Trigger";
  private static final String BATCHES = "Issue13251Batch";
  private static final int    SIZE    = 25;

  private final AtomicInteger              conflicts = new AtomicInteger();
  private       BeforeRecordUpdateListener conflict;

  @BeforeEach
  void createSchemaAndConflict() {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.command("sql", "CREATE VERTEX TYPE " + ROWS);
    database.command("sql", "CREATE PROPERTY " + ROWS + ".seq INTEGER");
    database.command("sql", "CREATE VERTEX TYPE " + TRIGGER);
    database.command("sql", "CREATE VERTEX TYPE " + BATCHES);
    database.transaction(() -> {
      for (int i = 0; i < SIZE; i++)
        database.newVertex(ROWS).set("seq", 0).save();
      database.newVertex(TRIGGER).set("seq", 0).save();
    });

    // The statement after the batches fails the way a genuine MVCC conflict on it would
    conflict = record -> {
      if (record instanceof Vertex vertex && vertex.getTypeName().equals(TRIGGER)) {
        conflicts.incrementAndGet();
        throw new ConcurrentModificationException("simulated conflict after the batches committed (issue #13251)");
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
  void aBatchedUpdateFollowedByAConflictIsNotResentByTheClient() {
    try (final RemoteDatabase db = remoteDatabase()) {
      assertThatThrownBy(() -> db.command("sqlscript", """
          UPDATE %s SET seq = seq + 1 BATCH 10;
          UPDATE %s SET seq = seq + 1;
          """.formatted(ROWS, TRIGGER))).satisfies(RemotePartialCommitNotRetriedIssue13251Test::assertNotRetryable);
    }

    assertThat(conflicts.get()).as("the request ran once: neither the server nor the client replayed it").isEqualTo(1);
    assertThat(countRowsWithSeq(1)).as("the 20 rows the batches committed are incremented exactly once").isEqualTo(20);
    assertThat(countRowsWithSeq(0)).as("the last 5 were rolled back").isEqualTo(5);
    assertThat(countRowsWithSeq(2)).as("no row may be incremented twice").isZero();
  }

  @Test
  void callInTransactionsBatchesFollowedByAConflictAreNotResentByTheClient() {
    try (final RemoteDatabase db = remoteDatabase()) {
      assertThatThrownBy(() -> db.command("opencypher", ("UNWIND range(1, 3) AS i CALL (i) { CREATE (:%s {i: i}) } IN TRANSACTIONS OF 1 ROW "
          + "WITH count(*) AS n MATCH (t:%s) SET t.seq = n").formatted(BATCHES, TRIGGER)))
          .satisfies(RemotePartialCommitNotRetriedIssue13251Test::assertNotRetryable);
    }

    assertThat(conflicts.get()).as("the request ran once: neither the server nor the client replayed it").isEqualTo(1);
    try (final ResultSet rs = getServerDatabase(0, getDatabaseName()).query("sql", "SELECT count(*) AS c FROM " + BATCHES)) {
      assertThat(rs.next().<Number>getProperty("c").longValue()).as("each batch is applied exactly once").isEqualTo(3L);
    }
  }

  /**
   * In a transaction the client opened, the server runs the request once and rolls the transaction back on the failure, so a
   * resend of the request is refused rather than run: the committed batches still stand exactly once.
   */
  @Test
  void insideASessionTheBatchesAreAppliedOnce() {
    try (final RemoteDatabase db = remoteDatabase()) {
      db.begin();
      try {
        assertThatThrownBy(() -> db.command("sqlscript", """
            UPDATE %s SET seq = seq + 1 BATCH 10;
            UPDATE %s SET seq = seq + 1;
            """.formatted(ROWS, TRIGGER))).isInstanceOf(ArcadeDBException.class);
      } finally {
        if (db.isTransactionActive())
          db.rollback();
      }
    }

    assertThat(conflicts.get()).as("the request ran once").isEqualTo(1);
    assertThat(countRowsWithSeq(1)).as("the 20 rows the batches committed are incremented exactly once").isEqualTo(20);
    assertThat(countRowsWithSeq(2)).as("no row may be incremented twice").isZero();
  }

  /** On the wire: not the 503 with {@code Retry-After} that tells any HTTP client, proxy or load balancer to send it again. */
  @Test
  void theAnswerIsNotARetryableStatus() throws Exception {
    final JSONObject payload = new JSONObject();
    payload.put("language", "sqlscript");
    payload.put("command", "UPDATE %s SET seq = seq + 1 BATCH 10; UPDATE %s SET seq = seq + 1;".formatted(ROWS, TRIGGER));
    payload.put("retries", 3);

    final HttpURLConnection connection = (HttpURLConnection) URI.create(
        getServerHttpUrl(0, "/api/v1/command/" + getDatabaseName())).toURL().openConnection();
    try {
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      formatPayload(connection, payload);
      connection.connect();

      assertThat(connection.getResponseCode()).isEqualTo(500);
      assertThat(connection.getHeaderField("Retry-After")).isNull();
      final String error = readError(connection);
      assertThat(error).contains(TransactionException.class.getName()).contains("committed part of its work");
    } finally {
      connection.disconnect();
    }

    assertThat(conflicts.get()).isEqualTo(1);
    assertThat(countRowsWithSeq(1)).isEqualTo(20);
  }

  private static void assertNotRetryable(final Throwable failure) {
    assertThat(failure).isInstanceOf(TransactionException.class).isNotInstanceOf(NeedRetryException.class);
    assertThat(failure.getMessage()).contains("committed part of its work").contains(ConcurrentModificationException.class.getSimpleName());
  }

  private long countRowsWithSeq(final int seq) {
    try (final ResultSet rs = getServerDatabase(0, getDatabaseName()).query("sql",
        "SELECT count(*) AS c FROM " + ROWS + " WHERE seq = ?", seq)) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  private RemoteDatabase remoteDatabase() {
    return new RemoteDatabase("127.0.0.1", getServerHttpPort(0), getDatabaseName(), "root", DEFAULT_PASSWORD_FOR_TESTS);
  }
}
