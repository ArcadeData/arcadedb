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
 * Issue #8323, end to end on a three-node cluster: a non-idempotent SQL write served on a follower is forwarded to the
 * leader by {@code RaftReplicatedDatabase}, and that forward now carries the client's {@code X-Request-Id}. A retry
 * with the same id that lands on the OTHER follower - whose own cache has never seen the id - is then answered from
 * the leader's cache instead of inserting the record a second time.
 * <p>
 * The type has no unique index on purpose: with one, a second execution fails on the duplicate key and looks like
 * deduplication from the outside. Here a second execution is a second record, and the count says which happened.
 */
@Tag("slow")
class Issue8323SqlForwardRequestIdIT extends BaseRaftHATest {

  private static final String TYPE = "Issue8323Doc";

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
  void aSqlWriteRetriedOnAnotherFollowerWithTheSameRequestIdExecutesOnceOnTheLeader() throws Exception {
    final int leader = findLeaderIndex();
    final int followerA = follower(leader, -1);
    final int followerB = follower(leader, followerA);
    createType(leader);

    final String payload = new JSONObject().put("language", "sql")
        .put("command", "INSERT INTO " + TYPE + " SET tag = 'dedup'").toString();

    final HttpResponse<String> first = command(followerA, payload, "issue8323-request-id");
    assertThat(first.statusCode()).as("first attempt, body: %s", first.body()).isEqualTo(200);

    final HttpResponse<String> retry = command(followerB, payload, "issue8323-request-id");
    assertThat(retry.statusCode()).as("retry, body: %s", retry.body()).isEqualTo(200);

    assertThat(countTagged(leader, "dedup"))
        .as("the retry must be answered from the leader's idempotency cache, not executed a second time")
        .isEqualTo(1L);
  }

  /** The control: the same two requests without an id are two writes, so the test above can tell the outcomes apart. */
  @Test
  @Timeout(180)
  void theSameWriteSentTwiceWithoutARequestIdExecutesTwice() throws Exception {
    final int leader = findLeaderIndex();
    final int followerA = follower(leader, -1);
    final int followerB = follower(leader, followerA);
    createType(leader);

    final String payload = new JSONObject().put("language", "sql")
        .put("command", "INSERT INTO " + TYPE + " SET tag = 'control'").toString();

    assertThat(command(followerA, payload, null).statusCode()).isEqualTo(200);
    assertThat(command(followerB, payload, null).statusCode()).isEqualTo(200);

    assertThat(countTagged(leader, "control")).isEqualTo(2L);
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

  private HttpResponse<String> command(final int serverIndex, final String body, final String requestId) throws Exception {
    final HttpRequest.Builder builder = HttpRequest.newBuilder()
        .uri(URI.create("http://127.0.0.1:" + getServer(serverIndex).getHttpServer().getPort() + "/api/v1/command/"
            + getDatabaseName()))
        .timeout(Duration.ofSeconds(30))
        .header("Content-Type", "application/json")
        .header("Authorization", basic("root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS));
    if (requestId != null)
      builder.header(IdempotencyCache.HEADER_REQUEST_ID, requestId);
    return HTTP.send(builder.POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8)).build(),
        HttpResponse.BodyHandlers.ofString());
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
