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
package com.arcadedb.server.ha.raft;

import com.arcadedb.database.Database;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.http.IdempotencyCache;
import com.arcadedb.server.http.RetryLaterException;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Issue #8355, end to end on a two-node cluster. A SQL write served on the follower is forwarded to the leader, which
 * answers {@code 503} + {@code Retry-After: 5} because it is installing a snapshot - refused before any handler ran, so
 * the write did not run. The body of that refusal names no exception class, and the follower used to rebuild it as a
 * plain {@code TransactionException} and answer its client 500 "Error on transaction commit" with no {@code Retry-After}.
 * <p>
 * The window is opened with {@link ArcadeDBServer#setSnapshotInstallInProgress} on the leader alone: that flag is what
 * the HTTP gate reads, and it touches nothing else, so the cluster itself stays healthy.
 */
@Tag("slow")
class Issue8355SqlForwardSnapshotInstallRefusalIT extends BaseRaftHATest {

  private static final String TYPE = "Issue8355Doc";

  private static final HttpClient HTTP = HttpClient.newBuilder()
      .version(HttpClient.Version.HTTP_1_1)
      .connectTimeout(Duration.ofSeconds(10))
      .build();

  /** The reported path: the follower answers the leader's refusal as a refusal, with its back-off. */
  @Test
  @Timeout(120)
  void aWriteTheLeaderRefusedWhileInstallingASnapshotIsAServiceUnavailableWithARetryAfter() throws Exception {
    final int leader = findLeaderIndex();
    final int follower = follower(leader);
    createType(leader);

    final String payload = new JSONObject().put("language", "sql")
        .put("command", "INSERT INTO " + TYPE + " SET tag = 'refused'")
        // One attempt, so the follower's own retry of the refusal (the test below) does not stretch this one out.
        .put("retries", 1).toString();

    final ArcadeDBServer leaderServer = getServer(leader);
    leaderServer.setSnapshotInstallInProgress(true);
    final HttpResponse<String> refused;
    try {
      refused = postCommand(follower, payload, "issue8355-refused");
    } finally {
      leaderServer.setSnapshotInstallInProgress(false);
    }

    assertThat(refused.statusCode()).as("body: %s", refused.body()).isEqualTo(503);
    assertThat(refused.headers().firstValue("Retry-After"))
        .as("the machine-readable back-off the leader gave, relayed by the follower")
        .contains("5");
    final JSONObject body = new JSONObject(refused.body());
    assertThat(body.getString("exception")).isEqualTo(RetryLaterException.class.getName());
    assertThat(body.getString("exceptionArgs")).isEqualTo("5");
    assertThat(countTagged(leader, "refused")).as("the leader refused the write before running it").isZero();

    // The client does what the answer told it: retries the same request, which runs once.
    final HttpResponse<String> retried = postCommand(follower, payload, "issue8355-refused");
    assertThat(retried.statusCode()).as("retry, body: %s", retried.body()).isEqualTo(200);
    await().atMost(30, TimeUnit.SECONDS).until(() -> countTagged(leader, "refused") >= 1L);
    assertThat(countTagged(leader, "refused")).isEqualTo(1L);
  }

  /**
   * The care point the issue names: the refusal is a {@code NeedRetryException}, so the follower's own auto-commit
   * wrapper ({@code DatabaseAbstractHandler}, {@code LocalDatabase.transaction}) retries it, and each retry re-forwards
   * the write under a new forward ordinal (issue #8323). That is safe here because the refusing node answered before
   * its idempotency gate: it neither ran the write nor reserved its id. The write must land exactly once.
   */
  @Test
  @Timeout(120)
  void theFollowersOwnRetryOfTheRefusalRunsTheWriteExactlyOnce() throws Exception {
    final int leader = findLeaderIndex();
    final int follower = follower(leader);
    createType(leader);

    // Enough attempts, with the engine's jittered back-off between them, to outlast the window opened below.
    final String payload = new JSONObject().put("language", "sql")
        .put("command", "INSERT INTO " + TYPE + " SET tag = 'retried'")
        .put("retries", 500).toString();

    final ArcadeDBServer leaderServer = getServer(leader);
    leaderServer.setSnapshotInstallInProgress(true);
    final CompletableFuture<HttpResponse<String>> response;
    try {
      response = CompletableFuture.supplyAsync(() -> postCommand(follower, payload, "issue8355-retried"));
      // Long enough for several forwards to be refused, short enough to stay well inside the attempts above.
      Thread.sleep(1_000);
      assertThat(response.isDone()).as("the request must still be retrying while the leader refuses it: %s",
          response.isDone() ? response.get().body() : "").isFalse();
      assertThat(countTagged(leader, "retried")).isZero();
    } finally {
      leaderServer.setSnapshotInstallInProgress(false);
    }

    final HttpResponse<String> answered = response.get(60, TimeUnit.SECONDS);
    assertThat(answered.statusCode()).as("body: %s", answered.body()).isEqualTo(200);
    await().atMost(30, TimeUnit.SECONDS).until(() -> countTagged(leader, "retried") >= 1L);
    assertThat(countTagged(leader, "retried"))
        .as("every refused forward ran nothing, so the one that got through is the only execution")
        .isEqualTo(1L);
  }

  // ---------------------------------------------------------------------------------------------

  private void createType(final int leader) {
    final Database db = getServerDatabase(leader, getDatabaseName());
    if (!db.getSchema().existsType(TYPE))
      db.command("sql", "CREATE DOCUMENT TYPE " + TYPE + " IF NOT EXISTS");
    await().atMost(30, TimeUnit.SECONDS).until(() -> {
      for (int i = 0; i < getServerCount(); i++)
        if (!getServerDatabase(i, getDatabaseName()).getSchema().existsType(TYPE))
          return false;
      return true;
    });
  }

  private long countTagged(final int serverIndex, final String tag) {
    final Database db = getServerDatabase(serverIndex, getDatabaseName());
    return ((Number) db.query("sql", "SELECT count(*) AS cnt FROM " + TYPE + " WHERE tag = ?", tag).next()
        .getProperty("cnt")).longValue();
  }

  private HttpResponse<String> postCommand(final int serverIndex, final String body, final String requestId) {
    try {
      final HttpRequest request = HttpRequest.newBuilder()
          .uri(URI.create("http://127.0.0.1:" + getServer(serverIndex).getHttpServer().getPort() + "/api/v1/command/"
              + getDatabaseName()))
          .timeout(Duration.ofSeconds(90))
          .header("Content-Type", "application/json")
          .header("Authorization", basic("root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS))
          .header(IdempotencyCache.HEADER_REQUEST_ID, requestId)
          .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
          .build();
      return HTTP.send(request, HttpResponse.BodyHandlers.ofString());
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static String basic(final String user, final String password) {
    return "Basic " + Base64.getEncoder().encodeToString((user + ":" + password).getBytes(StandardCharsets.UTF_8));
  }

  private int follower(final int leader) {
    assertThat(leader).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    for (int i = 0; i < getServerCount(); i++)
      if (i != leader)
        return i;
    throw new IllegalStateException("no follower in a " + getServerCount() + "-node cluster");
  }
}
