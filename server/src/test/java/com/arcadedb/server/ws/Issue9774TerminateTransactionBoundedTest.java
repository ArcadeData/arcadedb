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
package com.arcadedb.server.ws;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.query.RunningQuery;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.ServerControlPlane;
import com.arcadedb.server.http.HttpSession;
import com.arcadedb.server.security.ServerSecurityUser;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Issue #9774: {@code terminate transaction} bounded its wait only over the registry entries of the session, and fell
 * through to {@code HttpSession.cancel()}, whose wait for the session lock is unbounded, whenever there was none - which is
 * exactly the case of work the registry did not list, such as a {@code /ws} insert chunk applied in the session. A request
 * thread was parked for as long as the chunk took, past the documented cap, and the call then answered {@code terminated}
 * for work that was never asked to stop. The wait now shares the call's deadline, with the rollback left to the holder of
 * the lock past it, and a {@code /ws} chunk in a session is listed under that session and stopped by the terminate.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
// A hang detector, not a latency bound
@Timeout(value = 5, unit = TimeUnit.MINUTES)
class Issue9774TerminateTransactionBoundedTest extends BaseGraphServerTest {

  @Test
  void terminatingASessionHeldByUnlistedWorkIsBoundedAndTheHolderRollsItBack() throws Exception {
    final ServerSecurityUser root = getServer(0).getSecurity().getUser("root");
    final String transactionId = httpBegin();
    assertThat(httpCommand(transactionId, "INSERT INTO Person SET name = 'pending'")).isEqualTo(200);
    final HttpSession session = getServer(0).getHttpServer().getSessionManager().getSessionById(root, transactionId);

    final CountDownLatch holding = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    final Thread holder = new Thread(() -> {
      try {
        // Holds the session lock as a /ws chunk or any work the registry does not list would, without an entry
        session.execute(root, () -> {
          holding.countDown();
          release.await();
          return null;
        });
      } catch (final Exception ignored) {
      }
    }, "issue9774-session-holder");
    holder.setDaemon(true);
    holder.start();
    try {
      assertThat(holding.await(10, TimeUnit.SECONDS)).isTrue();

      final StallAwareStopwatch watch = StallAwareStopwatch.start();
      final JSONObject answer = new ServerControlPlane(getServer(0)).terminateTransaction(root, transactionId, 500);
      watch.assertGaveUpWithin(5_000, "a terminate bounded by its wait, against one parked until the holder lets go");
      assertThat(answer.getString("status")).as("the session is still held: it is not terminated yet").isEqualTo("terminating");
      assertThat(getServer(0).getHttpServer().getSessionManager().isSessionRegistered(transactionId))
          .as("no new request can resolve the session meanwhile").isFalse();
    } finally {
      release.countDown();
    }
    holder.join(10_000);

    // The holder let go and rolled the transaction back on its way out: nothing of it is committed, nor can be
    assertThat(session.transaction.isActive()).isFalse();
    assertThat(getServerDatabase(0, getDatabaseName()).countType("Person", false)).isZero();
  }

  /** An interrupted caller may never have tried the lock: with nobody holding it, the bounded cancel rolls back itself. */
  @Test
  void anInterruptedBoundedCancelOfAnIdleSessionStillRollsItBack() throws Exception {
    final ServerSecurityUser root = getServer(0).getSecurity().getUser("root");
    final String transactionId = httpBegin();
    assertThat(httpCommand(transactionId, "INSERT INTO Person SET name = 'pending'")).isEqualTo(200);
    final HttpSession session = getServer(0).getHttpServer().getSessionManager().getSessionById(root, transactionId);
    assertThat(session.transaction.isActive()).isTrue();

    Thread.currentThread().interrupt();
    final boolean rolledBack;
    try {
      rolledBack = session.cancel(500);
    } finally {
      assertThat(Thread.interrupted()).as("the interrupt is kept for the caller").isTrue();
    }
    assertThat(rolledBack).isTrue();
    assertThat(session.transaction.isActive()).isFalse();
  }

  @Test
  void aChunkInTheCallersSessionIsListedUnderItAndStoppedByTerminateTransaction() throws Throwable {
    final ServerSecurityUser root = getServer(0).getSecurity().getUser("root");
    final Database database = getServerDatabase(0, getDatabaseName());
    final String transactionId = httpBegin();
    // one chunk large enough to still be running when it is found and stopped
    getServer(0).getConfiguration().setValue(GlobalConfiguration.SERVER_WS_MAX_INSERT_CHUNK_ROWS, 0);

    try (final var client = new WebSocketClientHelper("ws://localhost:" + getServer(0).getHttpServer().getPort() + "/ws", "root",
        DEFAULT_PASSWORD_FOR_TESTS)) {
      final JSONObject started = new JSONObject(client.send(new JSONObject().put("action", "start").put("database", getDatabaseName())
          .put("transactionId", transactionId)
          .put("options", new JSONObject().put("targetType", "Person").put("transactionMode", "none")).toString()));
      final String sessionId = started.getString("sessionId");

      // Large enough to still be running when it is found and stopped
      final JSONArray records = new JSONArray();
      for (int i = 0; i < 400_000; i++)
        records.put(new JSONObject().put("name", "n" + i));
      client.sendWithoutWaiting(new JSONObject().put("action", "chunk").put("sessionId", sessionId).put("chunkSeq", 1)
          .put("records", records).toString());

      final RunningQuery[] found = new RunningQuery[1];
      final String[] answeredFirst = new String[1];
      await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(5)).until(() -> {
        for (final RunningQuery q : getServer(0).getRunningQueries().getRunning())
          if ("ws".equals(q.getProtocol()) && transactionId.equals(q.getSessionId())) {
            found[0] = q;
            return true;
          }
        answeredFirst[0] = client.popMessage(0);
        return answeredFirst[0] != null;
      });
      assertThat(answeredFirst[0]).as("the chunk was answered before it was seen running").isNull();
      assertThat(found[0].getUser()).isEqualTo("root");
      assertThat(found[0].getDatabase()).isEqualTo(getDatabaseName());

      final JSONObject answer = new ServerControlPlane(getServer(0)).terminateTransaction(root, transactionId, 10_000);
      assertThat(answer.getString("status")).isEqualTo("terminated");
      assertThat(found[0].getOutcome()).isEqualTo(RunningQuery.Outcome.TERMINATED);

      final JSONObject refused = new JSONObject(client.popMessage(60_000));
      assertThat(refused.getString("result", "")).isEqualTo("error");
      assertThat(refused.getString("detail", "")).contains("terminated");
    } finally {
      getServer(0).getConfiguration().setValue(GlobalConfiguration.SERVER_WS_MAX_INSERT_CHUNK_ROWS, GlobalConfiguration.SERVER_WS_MAX_INSERT_CHUNK_ROWS.getDefValue());
    }

    assertThat(getServer(0).getHttpServer().getSessionManager().isSessionRegistered(transactionId)).isFalse();
    assertThat(database.countType("Person", false)).as("what the chunk wrote was rolled back").isZero();
  }

  private String httpBegin() throws Exception {
    final HttpURLConnection connection = open("begin/" + getDatabaseName());
    try {
      write(connection, "{}");
      assertThat(connection.getResponseCode()).isEqualTo(204);
      return connection.getHeaderField("arcadedb-session-id");
    } finally {
      connection.disconnect();
    }
  }

  private int httpCommand(final String transactionId, final String sql) throws Exception {
    final HttpURLConnection connection = open("command/" + getDatabaseName());
    connection.setRequestProperty("arcadedb-session-id", transactionId);
    connection.setRequestProperty("Content-Type", "application/json");
    try {
      write(connection, new JSONObject().put("language", "sql").put("command", sql).toString());
      return connection.getResponseCode();
    } finally {
      connection.disconnect();
    }
  }

  private HttpURLConnection open(final String path) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(getServerHttpUrl(0, "/api/v1/" + path)).toURL()
        .openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)));
    connection.setDoOutput(true);
    return connection;
  }

  private static void write(final HttpURLConnection connection, final String body) throws Exception {
    try (final OutputStream out = connection.getOutputStream()) {
      out.write(body.getBytes(StandardCharsets.UTF_8));
    }
  }
}
