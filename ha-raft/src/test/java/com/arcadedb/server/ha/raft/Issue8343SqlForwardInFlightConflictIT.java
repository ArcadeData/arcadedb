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
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.http.IdempotencyCache;
import com.arcadedb.server.http.RequestStillInFlightException;
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
 * Issue #8343, end to end on a three-node cluster. A non-idempotent SQL write served on follower A is forwarded to the
 * leader with the client's {@code X-Request-Id} (issue #8323). While it is still executing there, the client retries
 * the same request, with the same id, through follower B. The leader refuses B's forward with its in-flight
 * {@code 409} + {@code Retry-After} (issue #8324).
 * <p>
 * Follower B used to rebuild that answer - whose body named no exception class - as a plain
 * {@code TransactionException} and answer the client 500 "Error on transaction commit" with no {@code Retry-After}.
 * It must answer the leader's 409 and back-off instead, and the write must still run exactly once.
 */
@Tag("slow")
class Issue8343SqlForwardInFlightConflictIT extends BaseRaftHATest {

  private static final String TYPE       = "Issue8343Doc";
  private static final String REQUEST_ID = "issue8343-request-id";

  private static final HttpClient HTTP = HttpClient.newBuilder()
      .version(HttpClient.Version.HTTP_1_1)
      .connectTimeout(Duration.ofSeconds(10))
      .build();

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  @Timeout(180)
  void aRetryThroughAnotherFollowerWhileTheLeaderStillRunsTheFirstIsAConflictWithARetryAfter() throws Exception {
    final int leader = findLeaderIndex();
    final int followerA = follower(leader, -1);
    final int followerB = follower(leader, followerA);
    createType(leader);

    // Sleeps well past the leader's 5 s in-flight wait, so the retry is refused rather than replayed.
    final String payload = new JSONObject().put("language", "sqlscript")
        .put("command", "SLEEP 15000; INSERT INTO " + TYPE + " SET tag = 'inflight';").toString();

    final IdempotencyCache leaderCache = getServer(leader).getHttpServer().getIdempotencyCache();
    final int leaderEntriesBefore = leaderCache.size();

    final CompletableFuture<HttpResponse<String>> first = CompletableFuture.supplyAsync(() -> postCommand(followerA, payload));

    // The forward reached the leader once its pending marker for the id is in the leader's cache.
    await().atMost(30, TimeUnit.SECONDS).pollInterval(20, TimeUnit.MILLISECONDS)
        .until(() -> leaderCache.size() > leaderEntriesBefore || first.isDone());
    assertThat(first.isDone()).as("the first attempt settled before the retry could be sent: %s",
        first.isDone() ? first.get().body() : "").isFalse();

    final HttpResponse<String> retry = postCommand(followerB, payload);

    assertThat(first.isDone()).as("the retry must be answered while the first execution is still running").isFalse();
    assertThat(retry.statusCode()).as("retry through the other follower, body: %s", retry.body()).isEqualTo(409);
    assertThat(retry.headers().firstValue("Retry-After"))
        .as("the machine-readable back-off the leader gave, relayed by the follower")
        .contains("5");
    final JSONObject body = new JSONObject(retry.body());
    assertThat(body.getString("error")).contains("still executing");
    assertThat(body.getString("exception")).isEqualTo(RequestStillInFlightException.class.getName());

    final HttpResponse<String> original = first.get(90, TimeUnit.SECONDS);
    assertThat(original.statusCode()).as("first attempt, body: %s", original.body()).isEqualTo(200);

    await().atMost(30, TimeUnit.SECONDS).until(() -> countTagged(leader, "inflight") >= 1L);
    assertThat(countTagged(leader, "inflight"))
        .as("the refused retry must not have executed the write a second time")
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

  private HttpResponse<String> postCommand(final int serverIndex, final String body) {
    try {
      final HttpRequest request = HttpRequest.newBuilder()
          .uri(URI.create("http://127.0.0.1:" + getServer(serverIndex).getHttpServer().getPort() + "/api/v1/command/"
              + getDatabaseName()))
          .timeout(Duration.ofSeconds(90))
          .header("Content-Type", "application/json")
          .header("Authorization", basic("root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS))
          .header(IdempotencyCache.HEADER_REQUEST_ID, REQUEST_ID)
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

  private int follower(final int leader, final int other) {
    assertThat(leader).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    for (int i = 0; i < getServerCount(); i++)
      if (i != leader && i != other)
        return i;
    throw new IllegalStateException("no follower in a " + getServerCount() + "-node cluster");
  }
}
