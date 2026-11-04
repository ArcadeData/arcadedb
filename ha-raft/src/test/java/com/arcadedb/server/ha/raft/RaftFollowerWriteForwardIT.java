/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.server.ha.raft;

import com.arcadedb.database.Database;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test verifying that write commands sent to a Raft follower's HTTP endpoint reach the leader
 * and succeed end-to-end.
 *
 * <p>Specifically:
 * <ul>
 *   <li>A DDL command (CREATE VERTEX TYPE) sent to a follower is forwarded to the leader,
 *       executed there, and the resulting schema change replicated to all nodes.</li>
 *   <li>A DML command (INSERT) sent to a follower is similarly forwarded, persisted on the
 *       leader, and replicated to all nodes.</li>
 * </ul>
 *
 * <p>It also covers concurrent transactions issued through one shared follower handle (issue #5503).
 *
 * <p>The component that does the forwarding is {@code RaftReplicatedDatabase.forwardCommandToLeaderViaRaft},
 * reached from {@code PostCommandHandler} through the engine. This class was called {@code RaftLeaderProxyIT}
 * and its javadoc credited {@code LeaderProxy} - a transparent follower-to-leader HTTP proxy that nothing ever
 * constructed, so no request had travelled that path and the test was green for a reason unrelated to the name
 * it carried (issue #7528). {@code LeaderProxy} is gone; the name now says which path is under test.
 */
class RaftFollowerWriteForwardIT extends BaseRaftHATest {

  private static final String     PROXIED_TYPE = "ProxiedType";
  private static final String     CONCURRENT_TYPE_NAME = "Concurrent";
  private static final int        WRITER_THREADS       = 3;
  private static final int        RECORDS_PER_THREAD   = 150;
  private static final int        TOTAL_RECORDS        = WRITER_THREADS * RECORDS_PER_THREAD;
  private static final int        MAX_RETRIES          = 100;
  private static final HttpClient HTTP_CLIENT  = HttpClient.newBuilder()
      .version(HttpClient.Version.HTTP_1_1)
      .connectTimeout(Duration.ofSeconds(10))
      .build();

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  void ddlViaFollowerIsProxiedToLeader() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected before the test").isGreaterThanOrEqualTo(0);

    final int followerIndex = firstFollowerIndex(leaderIndex);
    LogManager.instance().log(this, Level.INFO,
        "TEST: leader=%d, follower=%d - sending DDL to follower HTTP endpoint", leaderIndex, followerIndex);

    final int followerPort = getServerHttpPort(followerIndex);
    final String dbName = getDatabaseName();
    final HttpResponse<String> response = postCommand(followerPort, dbName,
        "CREATE VERTEX TYPE " + PROXIED_TYPE);

    assertThat(response.statusCode())
        .as("Follower should proxy DDL to leader and return 200")
        .isEqualTo(200);
    assertThat(response.body())
        .as("Response body should acknowledge type creation")
        .contains(PROXIED_TYPE);

    // Wait for replication on all nodes
    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    // Verify the type was created and replicated to every node
    for (int i = 0; i < getServerCount(); i++) {
      final Database db = getServerDatabase(i, dbName);
      assertThat(db.getSchema().existsType(PROXIED_TYPE))
          .as("Server %d should have type %s after leader-proxy DDL replication", i, PROXIED_TYPE)
          .isTrue();
    }
  }

  @Test
  void dmlViaFollowerIsProxiedToLeader() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected before the test").isGreaterThanOrEqualTo(0);

    final int followerIndex = firstFollowerIndex(leaderIndex);
    LogManager.instance().log(this, Level.INFO,
        "TEST: leader=%d, follower=%d - sending DML to follower HTTP endpoint", leaderIndex, followerIndex);

    final int followerPort = getServerHttpPort(followerIndex);
    final String dbName = getDatabaseName();

    // Use the built-in V1 type that is pre-created by the base graph schema setup
    final int insertCount = 5;
    for (int i = 0; i < insertCount; i++) {
      final HttpResponse<String> response = postCommand(followerPort, dbName,
          "INSERT INTO V1 SET name = 'proxy-test-" + i + "'");
      assertThat(response.statusCode())
          .as("INSERT %d proxied via follower should return 200", i)
          .isEqualTo(200);
    }

    // Wait for replication on all nodes
    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    // Verify record count is consistent across all nodes
    for (int i = 0; i < getServerCount(); i++) {
      final Database db = getServerDatabase(i, dbName);
      final long[] count = { 0 };
      db.transaction(() -> count[0] = db.countType("V1", false));
      assertThat(count[0])
          .as("Server %d should have at least %d V1 records after follower DML proxy replication", i, insertCount)
          .isGreaterThanOrEqualTo(insertCount);
    }
  }

  /**
   * Regression test for the bug where positional params serialized as a JSON array [110]
   * were converted to a primitive numeric array by toMap(true) on the leader (float[]{110.0}
   * before the issue #4148 fix, long[]{110L} after it), causing ClassCastException when
   * PostCommandHandler tried to cast it to Map.
   */
  @Test
  void positionalParamsViaFollowerIsProxiedToLeader() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected before the test").isGreaterThanOrEqualTo(0);

    final int followerIndex = firstFollowerIndex(leaderIndex);
    final int followerPort = getServerHttpPort(followerIndex);
    final String dbName = getDatabaseName();

    final JSONObject body = new JSONObject()
        .put("language", "sql")
        .put("command", "INSERT INTO V1 SET id = ?, name = ?")
        .put("params", new JSONArray().put(42).put("positional-test"));

    final HttpResponse<String> response = postCommandWithBody(followerPort, dbName, body.toString());
    assertThat(response.statusCode())
        .as("INSERT with positional params proxied via follower should return 200, body: " + response.body())
        .isEqualTo(200);

    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    for (int i = 0; i < getServerCount(); i++) {
      final Database db = getServerDatabase(i, dbName);
      final long[] found = { 0 };
      db.transaction(() -> {
        try (final ResultSet rs = db.query("sql",
            "SELECT FROM V1 WHERE name = 'positional-test'")) {
          while (rs.hasNext()) {
            rs.next();
            found[0]++;
          }
        }
      });
      assertThat(found[0])
          .as("Server %d should have the positional-param-inserted record after replication", i)
          .isEqualTo(1L);
    }

    // Also test the primitive-array code path: an all-numeric JSON array [42] is converted by
    // toMap(true) on the leader to a primitive array (long[] post-#4148, float[] before it).
    // This previously caused ClassCastException; the regression covers both shapes.
    final JSONObject numericBody = new JSONObject()
        .put("language", "sql")
        .put("command", "SELECT FROM V1 WHERE id = ?")
        .put("params", new JSONArray().put(42));

    final HttpResponse<String> numericResponse = postCommandWithBody(followerPort, dbName, numericBody.toString());
    assertThat(numericResponse.statusCode())
        .as("SELECT with all-numeric positional params (primitive-array path) should not throw ClassCastException, body: " + numericResponse.body())
        .isEqualTo(200);
  }

  /**
   * Issue #5503, from {@code Issue5503ConcurrentFollowerWritesIT}: concurrent transactions through a shared follower handle must not
   * corrupt or lose records; each either commits or fails retryably, and every node ends with every record exactly once.
   */
  @Test
  @Tag("slow")
  void concurrentWritesOnAFollowerHandleKeepEveryRecord() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("a leader must be elected before the test starts").isGreaterThanOrEqualTo(0);

    getServer(leaderIndex).getDatabaseForOperator(getDatabaseName()).command("sql", "CREATE DOCUMENT TYPE " + CONCURRENT_TYPE_NAME);
    waitForAllServers();

    // ALL WRITES GO THROUGH A FOLLOWER, FROM SEVERAL THREADS SHARING THE SAME EMBEDDED HANDLE
    final Database follower = getServer((leaderIndex + 1) % getServerCount()).getDatabaseForOperator(getDatabaseName());

    final ConcurrentLinkedQueue<Throwable> failures = new ConcurrentLinkedQueue<>();
    final AtomicInteger retries = new AtomicInteger();
    final CountDownLatch start = new CountDownLatch(1);
    final Thread[] writers = new Thread[WRITER_THREADS];

    for (int t = 0; t < WRITER_THREADS; t++) {
      final int threadId = t;
      writers[t] = new Thread(() -> {
        try {
          start.await();
          for (int i = 0; i < RECORDS_PER_THREAD; i++) {
            final int value = threadId * RECORDS_PER_THREAD + i;
            commitWithRetry(follower, value, retries);
          }
        } catch (final Throwable e) {
          failures.add(e);
        }
      }, "issue5503-writer-" + t);
      writers[t].start();
    }

    start.countDown();
    for (final Thread writer : writers)
      writer.join(TimeUnit.MINUTES.toMillis(5));

    assertThat(failures).as("no writer may fail with a non-retryable error").isEmpty();

    waitForAllServers();

    // EVERY NODE MUST HOLD EVERY RECORD EXACTLY ONCE: A LOST OR CORRUPTED RECORD SHOWS UP AS A GAP
    for (int i = 0; i < getServerCount(); i++) {
      final Database db = getServer(i).getDatabaseForOperator(getDatabaseName());
      final String role = i == leaderIndex ? "leader" : "follower";

      assertThat(scanCount(db)).as("record count on the %s (server %d), %d retries observed", role, i, retries.get())
          .isEqualTo(TOTAL_RECORDS);
      assertThat(distinctValues(db)).as("distinct values on the %s (server %d)", role, i).hasSize(TOTAL_RECORDS);
    }
  }

  /**
   * Commits a single-record transaction, retrying the conflicts the engine is allowed to raise. A
   * concurrent writer losing the race for a page must surface a retryable {@link NeedRetryException}
   * rather than silently dropping or corrupting the record.
   */
  private void commitWithRetry(final Database db, final int value, final AtomicInteger retries) {
    for (int attempt = 0; ; attempt++) {
      try {
        db.transaction(() -> db.newDocument(CONCURRENT_TYPE_NAME).set("value", value).save());
        return;
      } catch (final NeedRetryException e) {
        retries.incrementAndGet();
        if (attempt >= MAX_RETRIES)
          throw e;
      }
    }
  }

  private long scanCount(final Database db) {
    // count(@rid) forces a scan: count(*) reads the cached per-bucket counter, which would hide a
    // record the engine deleted as malformed.
    try (final ResultSet rs = db.query("sql", "SELECT count(@rid) AS c FROM " + CONCURRENT_TYPE_NAME)) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  private Set<Integer> distinctValues(final Database db) {
    final Set<Integer> values = new HashSet<>();
    try (final ResultSet rs = db.query("sql", "SELECT value FROM " + CONCURRENT_TYPE_NAME)) {
      while (rs.hasNext())
        values.add(rs.next().<Number>getProperty("value").intValue());
    }
    return values;
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  private int firstFollowerIndex(final int leaderIndex) {
    for (int i = 0; i < getServerCount(); i++) {
      if (i != leaderIndex) {
        final RaftHAPlugin plugin = getRaftPlugin(i);
        if (plugin != null)
          return i;
      }
    }
    throw new IllegalStateException("No follower found in " + getServerCount() + "-node cluster");
  }

  /**
   * Sends a POST /api/v1/command/{dbName} request with a SQL command to the server
   * at {@code port}, using Basic authentication.
   */
  private HttpResponse<String> postCommand(final int port, final String dbName, final String sql) throws Exception {
    return postCommandWithBody(port, dbName, new JSONObject().put("language", "sql").put("command", sql).toString());
  }

  private HttpResponse<String> postCommandWithBody(final int port, final String dbName, final String jsonBody)
      throws Exception {
    final String authHeader = "Basic " + Base64.getEncoder()
        .encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8));

    final HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create("http://127.0.0.1:" + port + "/api/v1/command/" + dbName))
        .timeout(Duration.ofSeconds(30))
        .header("Authorization", authHeader)
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(jsonBody, StandardCharsets.UTF_8))
        .build();

    return HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
  }
}
