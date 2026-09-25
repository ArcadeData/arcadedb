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
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.http.IdempotencyCache;
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
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Issue #8359, end to end on a three-node cluster: a retry deduplicated across a follower's forward and a direct leader
 * request (issue #8347) was replayed in the forward's rendering. The forward was rebuilt from the statement, so the
 * leader answered it with its defaults ({@code serializer: "record"}) and settled the client's key with that answer: a
 * direct retry asking for {@code serializer: "studio"} got a row array back. The other way round, the follower parsed the
 * direct attempt's studio answer as a row array, found no rows in it and rendered an empty result.
 * <p>
 * Both orders now answer the client's own rendering, on both attempts, and still execute the write once.
 */
@Tag("slow")
class Issue8359ReplayRenderingIT extends BaseRaftHATest {

  private static final String TYPE = "Issue8359Doc";

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
  void aDirectRetryOfAForwardedWriteIsReplayedInTheRetrysSerializer() throws Exception {
    final int leader = findLeaderIndex();
    final int follower = follower(leader);
    createType(leader);

    final String payload = studioBody("follower-then-leader");

    final HttpResponse<String> first = command(follower, payload, "issue8359-follower-then-leader");
    assertThat(first.statusCode()).as("first attempt, body: %s", first.body()).isEqualTo(200);
    assertStudioRecord(first.body(), "follower-then-leader");

    final HttpResponse<String> retry = command(leader, payload, "issue8359-follower-then-leader");
    assertThat(retry.statusCode()).as("retry, body: %s", retry.body()).isEqualTo(200);
    assertStudioRecord(retry.body(), "follower-then-leader");
    assertThat(retry.body()).as("the retry is a replay of the same answer").isEqualTo(first.body());

    assertThat(countTagged(leader, "follower-then-leader")).as("executed once").isEqualTo(1L);
  }

  @Test
  @Timeout(180)
  void aForwardedRetryOfADirectWriteIsAnsweredInTheClientsSerializer() throws Exception {
    final int leader = findLeaderIndex();
    final int follower = follower(leader);
    createType(leader);

    final String payload = studioBody("leader-then-follower");

    final HttpResponse<String> first = command(leader, payload, "issue8359-leader-then-follower");
    assertThat(first.statusCode()).as("first attempt, body: %s", first.body()).isEqualTo(200);
    assertStudioRecord(first.body(), "leader-then-follower");

    final HttpResponse<String> retry = command(follower, payload, "issue8359-leader-then-follower");
    assertThat(retry.statusCode()).as("retry, body: %s", retry.body()).isEqualTo(200);
    assertStudioRecord(retry.body(), "leader-then-follower");
    assertThat(retry.body()).as("the retry is a replay of the same answer").isEqualTo(first.body());

    assertThat(countTagged(leader, "leader-then-follower")).as("executed once").isEqualTo(1L);
  }

  /** Without an X-Request-Id nothing is cached, and a write sent to a follower is still answered in the rows parsed back. */
  @Test
  @Timeout(180)
  void aForwardWithoutARequestIdStillAnswersItsRows() throws Exception {
    final int leader = findLeaderIndex();
    final int follower = follower(leader);
    createType(leader);

    final HttpResponse<String> response = command(follower,
        "{\"language\":\"sql\",\"command\":\"INSERT INTO " + TYPE + " SET tag = 'no-id'\"}", null);
    assertThat(response.statusCode()).as("body: %s", response.body()).isEqualTo(200);
    final JSONArray rows = new JSONObject(response.body()).getJSONArray("result");
    assertThat(rows.length()).isEqualTo(1);
    assertThat(rows.getJSONObject(0).getString("tag")).isEqualTo("no-id");
  }

  // ---------------------------------------------------------------------------------------------

  /** A client asking for the studio rendering, which a forward rebuilt from the statement never did. */
  private static String studioBody(final String tag) {
    return "{ \"serializer\": \"studio\", \"limit\": 20, \"language\": \"sql\", \"command\": \"INSERT INTO " + TYPE
        + " SET tag = '" + tag + "'\" }";
  }

  /** The studio rendering: an object with vertices, edges and records - never a row array - carrying the one record. */
  private static void assertStudioRecord(final String body, final String tag) {
    final JSONObject json = new JSONObject(body);
    assertThat(json.get("result")).as("studio rendering, body: %s", body).isInstanceOf(JSONObject.class);
    final JSONArray records = json.getJSONObject("result").getJSONArray("records");
    assertThat(records.length()).as("body: %s", body).isEqualTo(1);
    assertThat(records.getJSONObject(0).getString("tag")).isEqualTo(tag);
  }

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

  private HttpResponse<String> command(final int serverIndex, final String body, final String requestId) throws Exception {
    final HttpRequest.Builder builder = HttpRequest.newBuilder()
        .uri(URI.create("http://127.0.0.1:" + getServer(serverIndex).getHttpServer().getPort() + "/api/v1/command/"
            + getDatabaseName()))
        .timeout(Duration.ofSeconds(30))
        .header("Content-Type", "application/json")
        .header("Authorization", "Basic " + Base64.getEncoder().encodeToString(
            ("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)))
        .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8));
    if (requestId != null)
      builder.header(IdempotencyCache.HEADER_REQUEST_ID, requestId);
    return HTTP.send(builder.build(), HttpResponse.BodyHandlers.ofString());
  }

  private int follower(final int leader) {
    assertThat(leader).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    for (int i = 0; i < getServerCount(); i++)
      if (i != leader)
        return i;
    throw new IllegalStateException("no follower in a " + getServerCount() + "-node cluster");
  }
}
