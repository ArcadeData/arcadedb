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

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.LeaderForwardContext;
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
import java.util.function.UnaryOperator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Issue #7603, end to end on a three-node cluster: what a follower-to-leader forward carries across the hop.
 * <ul>
 *   <li>{@code X-Request-Id} reaches the leader, so its idempotency cache deduplicates a retry wherever it lands -
 *   including on a different follower, whose own cache has never seen the id;</li>
 *   <li>a node that receives an already-forwarded request and is not the leader tells a leadership change in flight
 *   (503, retryable) from an address that named the wrong node (400, the configuration error), on each of the three
 *   forwards that refuse a second hop: {@code POST /api/v1/server}, a SQL write, and {@code POST /api/v1/batch}.</li>
 * </ul>
 * The leadership change is reproduced by sending the follower exactly what its peer would have sent it had it been
 * the leader a moment earlier - the cluster token, the one-hop marker and the follower's own peer id as the intended
 * leader - rather than by forcing an election mid-request, which no test can time reliably.
 */
@Tag("slow")
class Issue7603LeaderForwardHopIT extends BaseRaftHATest {

  private static final HttpClient HTTP = HttpClient.newBuilder()
      .version(HttpClient.Version.HTTP_1_1)
      .connectTimeout(Duration.ofSeconds(10))
      .build();

  @Override
  protected int getServerCount() {
    return 3;
  }

  /**
   * The reported repro, with the retry sent to the other follower. Before the fix the id never reached the leader,
   * so the second request created the user a second time there - answered with an error, because the user already
   * exists - instead of replaying the first answer.
   */
  @Test
  @Timeout(180)
  void aRetryWithTheSameRequestIdIsDeduplicatedOnTheLeaderWhicheverFollowerItLandsOn() throws Exception {
    final int leader = findLeaderIndex();
    assertThat(leader).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int followerA = follower(leader, -1);
    final int followerB = follower(leader, followerA);

    final String name = "issue7603dedup";
    final String payload = new JSONObject().put("command",
        "create user { \"name\": \"" + name + "\", \"password\": \"issue7603password\", \"databases\": {} }").toString();
    final UnaryOperator<HttpRequest.Builder> withId = b -> b
        .header("Authorization", basic("root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS))
        .header(IdempotencyCache.HEADER_REQUEST_ID, "issue7603-request-id");

    final HttpResponse<String> first = send(followerA, "/api/v1/server", payload, withId);
    assertThat(first.statusCode()).as("first attempt, body: %s", first.body()).isEqualTo(200);
    awaitUserOnEveryServer(name);

    final HttpResponse<String> retry = send(followerB, "/api/v1/server", payload, withId);
    assertThat(retry.statusCode())
        .as("the retry must be answered from the leader's cache, not executed again, body: %s", retry.body())
        .isEqualTo(200);
    assertThat(retry.body()).isEqualTo(first.body());
  }

  @Test
  @Timeout(180)
  void aServerCommandRefusedBecauseLeadershipMovedIsRetryable() throws Exception {
    final int follower = follower(findLeaderIndex(), -1);
    final String payload = new JSONObject().put("command",
        "create user { \"name\": \"issue7603moved\", \"password\": \"issue7603password\", \"databases\": {} }").toString();

    final HttpResponse<String> moved = send(follower, "/api/v1/server", payload, forwardedFrom(follower, ownPeerId(follower)));
    assertThat(moved.statusCode()).as("body: %s", moved.body()).isEqualTo(503);
    assertThat(moved.body()).contains("leadership moved");

    final HttpResponse<String> misidentified = send(follower, "/api/v1/server", payload,
        forwardedFrom(follower, ownPeerId(findLeaderIndex())));
    assertThat(misidentified.statusCode()).as("body: %s", misidentified.body()).isEqualTo(400);
    assertThat(misidentified.body()).contains("already forwarded");

    for (int i = 0; i < getServerCount(); i++)
      assertThat(getServer(i).getSecurity().existsUser("issue7603moved")).isFalse();
  }

  @Test
  @Timeout(180)
  void aSqlWriteRefusedBecauseLeadershipMovedIsRetryable() throws Exception {
    final int follower = follower(findLeaderIndex(), -1);
    final String payload = new JSONObject().put("language", "sql")
        .put("command", "INSERT INTO " + VERTEX1_TYPE_NAME + " SET id = 7603001").toString();

    final HttpResponse<String> moved = send(follower, "/api/v1/command/" + getDatabaseName(), payload,
        forwardedFrom(follower, ownPeerId(follower)));
    assertThat(moved.statusCode()).as("body: %s", moved.body()).isEqualTo(503);

    final HttpResponse<String> misidentified = send(follower, "/api/v1/command/" + getDatabaseName(), payload,
        forwardedFrom(follower, ownPeerId(findLeaderIndex())));
    assertThat(misidentified.statusCode()).as("body: %s", misidentified.body()).isEqualTo(400);
  }

  @Test
  @Timeout(180)
  void aBatchRefusedBecauseLeadershipMovedIsRetryable() throws Exception {
    final int follower = follower(findLeaderIndex(), -1);
    final String payload = "{\"@type\":\"vertex\",\"@class\":\"" + VERTEX1_TYPE_NAME + "\",\"id\":7603002}\n";

    final HttpResponse<String> moved = sendBatch(follower, payload, forwardedFrom(follower, ownPeerId(follower)));
    assertThat(moved.statusCode()).as("body: %s", moved.body()).isEqualTo(503);
    assertThat(moved.body()).contains("leadership moved");

    final HttpResponse<String> misidentified = sendBatch(follower, payload,
        forwardedFrom(follower, ownPeerId(findLeaderIndex())));
    assertThat(misidentified.statusCode()).as("body: %s", misidentified.body()).isEqualTo(400);
  }

  /**
   * Issue #8393: a peer that names no intended leader - one on an older build during a rolling upgrade, or one that
   * saw leadership change while it resolved the leader's address - proves nothing about the address. All three
   * refusal sites answer it as the election it usually is: 503, retryable, and the once-per-database configuration
   * warning is left for the refusal that does prove the address wrong.
   */
  @Test
  @Timeout(180)
  void aRefusalFromAPeerThatNamedNoLeaderIsRetryableOnEveryForward() throws Exception {
    final int follower = follower(findLeaderIndex(), -1);
    final RaftReplicatedDatabase sqlForwarder = (RaftReplicatedDatabase) ((DatabaseInternal) getServerDatabase(follower,
        getDatabaseName())).getWrappedDatabaseInstance();

    final HttpResponse<String> command = send(follower, "/api/v1/server", new JSONObject().put("command",
            "create user { \"name\": \"issue8393undetermined\", \"password\": \"issue8393password\", \"databases\": {} }")
        .toString(), forwardedFrom(follower, null));
    assertThat(command.statusCode()).as("body: %s", command.body()).isEqualTo(503);
    assertThat(command.body()).contains("retry");

    final HttpResponse<String> sql = send(follower, "/api/v1/command/" + getDatabaseName(), new JSONObject()
        .put("language", "sql").put("command", "INSERT INTO " + VERTEX1_TYPE_NAME + " SET id = 8393001").toString(),
        forwardedFrom(follower, null));
    assertThat(sql.statusCode()).as("body: %s", sql.body()).isEqualTo(503);
    assertThat(sqlForwarder.misconfigurationWarned())
        .as("an unclassifiable refusal must not spend the misconfiguration's once-per-database notice").isFalse();

    final HttpResponse<String> batch = sendBatch(follower,
        "{\"@type\":\"vertex\",\"@class\":\"" + VERTEX1_TYPE_NAME + "\",\"id\":8393002}\n", forwardedFrom(follower, null));
    assertThat(batch.statusCode()).as("body: %s", batch.body()).isEqualTo(503);
    assertThat(batch.body()).contains("retry");

    // The refusal that does prove the address wrong still gets the configuration error and the notice.
    final HttpResponse<String> misidentified = send(follower, "/api/v1/command/" + getDatabaseName(), new JSONObject()
            .put("language", "sql").put("command", "INSERT INTO " + VERTEX1_TYPE_NAME + " SET id = 8393003").toString(),
        forwardedFrom(follower, ownPeerId(findLeaderIndex())));
    assertThat(misidentified.statusCode()).as("body: %s", misidentified.body()).isEqualTo(400);
    assertThat(sqlForwarder.misconfigurationWarned()).isTrue();

    for (int i = 0; i < getServerCount(); i++)
      assertThat(getServer(i).getSecurity().existsUser("issue8393undetermined")).isFalse();
  }

  // ---------------------------------------------------------------------------------------------

  /**
   * What a peer relaying a Basic-authenticated request to {@code intendedLeader} sends; with a null
   * {@code intendedLeader}, what a peer that cannot name the leader - or predates the header - sends.
   */
  private UnaryOperator<HttpRequest.Builder> forwardedFrom(final int receiver, final String intendedLeader) {
    final String token = getRaftPlugin(receiver).getRaftHAServer().getClusterToken();
    return b -> {
      b.header("Authorization", basic("root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS))
          .header("X-ArcadeDB-Cluster-Token", token)
          .header(LeaderForwardContext.FORWARDED_TO_LEADER_HEADER, "true");
      if (intendedLeader != null)
        b.header(LeaderForwardContext.FORWARDED_LEADER_ID_HEADER, intendedLeader);
      return b;
    };
  }

  private String ownPeerId(final int serverIndex) {
    final String peerId = getRaftPlugin(serverIndex).getLocalPeerId();
    assertThat(peerId).isNotBlank();
    return peerId;
  }

  private void awaitUserOnEveryServer(final String name) {
    await().atMost(30, TimeUnit.SECONDS).until(() -> {
      for (int i = 0; i < getServerCount(); i++)
        if (!getServer(i).getSecurity().existsUser(name))
          return false;
      return true;
    });
  }

  private HttpResponse<String> send(final int serverIndex, final String path, final String body,
      final UnaryOperator<HttpRequest.Builder> decorate) throws Exception {
    final HttpRequest.Builder builder = decorate.apply(HttpRequest.newBuilder()
        .uri(URI.create("http://127.0.0.1:" + getServer(serverIndex).getHttpServer().getPort() + path))
        .timeout(Duration.ofSeconds(30))
        .header("Content-Type", "application/json"));
    return HTTP.send(builder.POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8)).build(),
        HttpResponse.BodyHandlers.ofString());
  }

  private HttpResponse<String> sendBatch(final int serverIndex, final String body,
      final UnaryOperator<HttpRequest.Builder> decorate) throws Exception {
    final HttpRequest.Builder builder = decorate.apply(HttpRequest.newBuilder()
        .uri(URI.create("http://127.0.0.1:" + getServer(serverIndex).getHttpServer().getPort() + "/api/v1/batch/"
            + getDatabaseName()))
        .timeout(Duration.ofSeconds(30))
        .header("Content-Type", "application/x-ndjson"));
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
