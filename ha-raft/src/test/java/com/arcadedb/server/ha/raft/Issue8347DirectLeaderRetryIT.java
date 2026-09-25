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
 * Issue #8347, end to end on a three-node cluster: a client's retry sent STRAIGHT to the leader, with the same
 * {@code X-Request-Id} as an attempt a follower forwarded there, executed a second time. The follower forwards a body
 * rebuilt from the statement ({@code language}, {@code command}, {@code params}), while the client's own body carries
 * other fields, in another order and spacing; the leader keyed the two differently. The forward now names the key the
 * client's request has on the follower, and the leader claims it too.
 * <p>
 * The type has no unique index on purpose: a second execution is a second record, and the count says which happened.
 */
@Tag("slow")
class Issue8347DirectLeaderRetryIT extends BaseRaftHATest {

  private static final String TYPE = "Issue8347Doc";

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
  void aRetrySentStraightToTheLeaderAfterAFollowerForwardedTheWriteExecutesOnce() throws Exception {
    final int leader = findLeaderIndex();
    final int follower = follower(leader);
    createType(leader);

    final String payload = clientBody("follower-then-leader");

    final HttpResponse<String> first = command(follower, payload, "issue8347-follower-then-leader");
    assertThat(first.statusCode()).as("first attempt, body: %s", first.body()).isEqualTo(200);

    final HttpResponse<String> retry = command(leader, payload, "issue8347-follower-then-leader");
    assertThat(retry.statusCode()).as("retry, body: %s", retry.body()).isEqualTo(200);

    assertThat(countTagged(leader, "follower-then-leader"))
        .as("the retry must be answered from the entry the forward settled on the leader, not executed again")
        .isEqualTo(1L);
  }

  @Test
  @Timeout(180)
  void aRetryAFollowerForwardsAfterTheClientWentStraightToTheLeaderExecutesOnce() throws Exception {
    final int leader = findLeaderIndex();
    final int follower = follower(leader);
    createType(leader);

    final String payload = clientBody("leader-then-follower");

    final HttpResponse<String> first = command(leader, payload, "issue8347-leader-then-follower");
    assertThat(first.statusCode()).as("first attempt, body: %s", first.body()).isEqualTo(200);

    final HttpResponse<String> retry = command(follower, payload, "issue8347-leader-then-follower");
    assertThat(retry.statusCode()).as("retry, body: %s", retry.body()).isEqualTo(200);

    assertThat(countTagged(leader, "leader-then-follower")).isEqualTo(1L);
  }

  // ---------------------------------------------------------------------------------------------

  /** A client's own body: fields the follower's rebuilt forward does not carry, in its own order and spacing. */
  private static String clientBody(final String tag) {
    return "{ \"serializer\": \"record\", \"limit\": 20, \"language\": \"sql\", \"command\": \"INSERT INTO " + TYPE
        + " SET tag = '" + tag + "'\" }";
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
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create("http://127.0.0.1:" + getServer(serverIndex).getHttpServer().getPort() + "/api/v1/command/"
            + getDatabaseName()))
        .timeout(Duration.ofSeconds(30))
        .header("Content-Type", "application/json")
        .header("Authorization", "Basic " + Base64.getEncoder().encodeToString(
            ("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)))
        .header(IdempotencyCache.HEADER_REQUEST_ID, requestId)
        .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
        .build();
    return HTTP.send(request, HttpResponse.BodyHandlers.ofString());
  }

  private int follower(final int leader) {
    assertThat(leader).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    for (int i = 0; i < getServerCount(); i++)
      if (i != leader)
        return i;
    throw new IllegalStateException("no follower in a " + getServerCount() + "-node cluster");
  }
}
