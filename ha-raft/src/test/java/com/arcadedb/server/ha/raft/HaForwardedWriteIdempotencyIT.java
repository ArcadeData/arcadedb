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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.LeaderForwardContext;
import com.arcadedb.server.http.IdempotencyCache;
import com.arcadedb.server.http.RequestStillInFlightException;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.Timeout;

import java.io.DataOutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * A write sent to a follower and forwarded to the leader, end to end on ONE three-node cluster shared by every test
 * ({@link SharedClusterRaftHATest}). Merged from {@code Issue8323SqlForwardRequestIdIT},
 * {@code Issue8343SqlForwardInFlightConflictIT}, {@code Issue8347DirectLeaderRetryIT}, {@code Issue8359ReplayRenderingIT},
 * {@code Issue7603LeaderForwardHopIT}, {@code Issue7516BasicAuthForwardLoopIT} (two of three tests),
 * {@code Issue6191FollowerForwardLoopIT} (three of five) and {@code Issue9689ForwardedQueryTerminateIT}, which ran on the
 * same fixture: three servers, default quorum, no configuration override.
 * <ul>
 * <li><b>#8323</b>: a non-idempotent SQL write forwarded by {@code RaftReplicatedDatabase} carries the client's
 * {@code X-Request-Id}, so a retry with the same id on the OTHER follower is answered from the leader's cache instead of
 * inserting the record again. The types have no unique index on purpose: a second execution is a second record, and the
 * count says which happened.</li>
 * <li><b>#8343</b>: a retry through follower B while the leader still runs follower A's forward is refused by the leader
 * with its in-flight {@code 409} + {@code Retry-After} (#8324). B used to rebuild that answer as a 500 "Error on
 * transaction commit" with no back-off; it must relay the 409, and the write must still run once.</li>
 * <li><b>#8347</b>: a retry sent straight to the leader, with the id of an attempt a follower forwarded, executed again,
 * because the forward's rebuilt body and the client's own body were keyed differently. The forward now names the key
 * the client's request has on the follower, and the leader claims it too, in both orders.</li>
 * <li><b>#8359</b>: such a cross-path retry was replayed in the forward's rendering ({@code serializer: "record"}), or
 * parsed as rows when the direct attempt had answered in the studio rendering. Both orders now answer the client's own
 * rendering and still execute the write once.</li>
 * <li><b>#7603</b>: what a follower-to-leader forward carries across the hop. {@code X-Request-Id} reaches the leader;
 * and a node that receives an already-forwarded request and is not the leader tells a leadership change in flight (503)
 * from an address that named the wrong node (400) on all three forwards ({@code POST /api/v1/server}, a SQL write,
 * {@code POST /api/v1/batch}), reproduced by sending exactly what a peer would have sent. #8393: a peer that names no
 * intended leader is answered 503 as well.</li>
 * <li><b>#7516</b>: the one-hop marker travels beside a relayed Basic-auth or API-token {@code Authorization} header,
 * proved by the cluster token. It is honoured only beside a valid cluster token, and a session-token forward
 * authenticates on the leader with the effective (derived) cluster token.</li>
 * <li><b>#6191</b>: the receiving side refuses an already-forwarded batch, a client's own marker does not suppress
 * forwarding, and a correctly addressed write still reaches the leader.</li>
 * <li><b>#9689</b>: terminating a forwarded write on the replica stops it on the leader.</li>
 * </ul>
 * Why these share a cluster: every test leaves it as it found it. Each type, request id and created user goes through
 * {@link #uniqueName(String)}, because the leader's idempotency cache outlives the test, and #9689 waits until the
 * terminated statement is gone from the leader's list. The tests of #7516 and #6191 that rewrite a follower's live
 * resolved-HTTP-address map are in {@link HaForwardedWriteMisaddressedLeaderIT}, on the per-test fixture: restoring the
 * map is not enough, because the capability monitor the test restarts runs its first round at once, and a round in flight
 * when the next such test pauses the monitor still relearns the real address over that test's injection (seen in 3 of 6
 * runs while they shared this cluster).
 * <p>
 * Two tests read state that an earlier test on the same cluster could change, so they run first
 * ({@link Order}), on the freshly booted cluster their source classes gave them:
 * <ul>
 * <li>#8343 waits for the leader's idempotency cache to GROW by its pending marker. An entry of an earlier test that
 * expires (60 s TTL) in the same moment would hide the growth;</li>
 * <li>#8393 asserts the once-per-database "address names the wrong node" warning has NOT been logged yet on the follower,
 * and #7603's SQL refusal sets that latch for good.</li>
 * </ul>
 * Siblings: {@link HaForwardedWriteMisaddressedLeaderIT} (#7516, #6191, address injection),
 * {@link HaForwardedWriteIdempotencyTwoNodeIT} (#8355, two nodes) and {@link HaForwardedWriteIdempotencyDisruptiveIT}
 * (#8481, ALL quorum, stops a follower).
 * <p>
 * The tests tagged {@code slow} were tagged so in their source class; #6191's were not.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
// new tests must not take @Order <= 2: the two pinned tests need to run first on a fresh cluster
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class HaForwardedWriteIdempotencyIT extends SharedClusterRaftHATest {

  private static final HttpClient HTTP = HttpClient.newBuilder()
      .version(HttpClient.Version.HTTP_1_1)
      .connectTimeout(Duration.ofSeconds(10))
      .build();

  /**
   * #7516's request timeout. Short on purpose: a build without the fix ping-pongs the request between the two followers
   * forever, so this timeout - not an assertion - is what ends that test, long before the {@link Timeout} that fails it.
   */
  private static final Duration REQUEST_TIMEOUT_7516 = Duration.ofSeconds(20);

  private static final String PASSWORD_7516 = "issue7516password";

  /** #9689's client tag. */
  private static final String TAG_9689 = "ha-9689";

  @Override
  protected int getServerCount() {
    return 3;
  }

  // ------------------------------------------------------------------ request id across the hop (#8323)

  /** Issue #8323, from {@code Issue8323SqlForwardRequestIdIT}. */
  @Tag("slow")
  @Test
  @Timeout(180)
  void aSqlWriteRetriedOnAnotherFollowerWithTheSameRequestIdExecutesOnceOnTheLeader() throws Exception {
    final int leader = findLeaderIndex();
    final int followerA = follower(leader, -1);
    final int followerB = follower(leader, followerA);
    final String type = uniqueName("Issue8323Doc");
    final String requestId = uniqueName("issue8323-request-id");
    createType(leader, type);

    final String payload = new JSONObject().put("language", "sql")
        .put("command", "INSERT INTO " + type + " SET tag = 'dedup'").toString();

    final HttpResponse<String> first = command(followerA, payload, requestId);
    assertThat(first.statusCode()).as("first attempt, body: %s", first.body()).isEqualTo(200);

    final HttpResponse<String> retry = command(followerB, payload, requestId);
    assertThat(retry.statusCode()).as("retry, body: %s", retry.body()).isEqualTo(200);

    assertThat(countTagged(leader, type, "dedup"))
        .as("the retry must be answered from the leader's idempotency cache, not executed a second time")
        .isEqualTo(1L);
  }

  /**
   * Issue #8323, from {@code Issue8323SqlForwardRequestIdIT}.
   * <p>
   * The control: the same two requests without an id are two writes, so the test above can tell the outcomes apart.
   */
  @Tag("slow")
  @Test
  @Timeout(180)
  void theSameWriteSentTwiceWithoutARequestIdExecutesTwice() throws Exception {
    final int leader = findLeaderIndex();
    final int followerA = follower(leader, -1);
    final int followerB = follower(leader, followerA);
    final String type = uniqueName("Issue8323Doc");
    createType(leader, type);

    final String payload = new JSONObject().put("language", "sql")
        .put("command", "INSERT INTO " + type + " SET tag = 'control'").toString();

    assertThat(command(followerA, payload, null).statusCode()).isEqualTo(200);
    assertThat(command(followerB, payload, null).statusCode()).isEqualTo(200);

    assertThat(countTagged(leader, type, "control")).isEqualTo(2L);
  }

  // ------------------------------------------------------------------ in-flight conflict (#8343)

  /**
   * Issue #8343, from {@code Issue8343SqlForwardInFlightConflictIT}.
   * <p>
   * Runs first: it waits for the leader's idempotency cache to grow, which an earlier test's entry expiring at the same
   * moment would hide.
   */
  @Tag("slow")
  @Test
  @Order(1)
  @Timeout(180)
  void aRetryThroughAnotherFollowerWhileTheLeaderStillRunsTheFirstIsAConflictWithARetryAfter() throws Exception {
    final int leader = findLeaderIndex();
    final int followerA = follower(leader, -1);
    final int followerB = follower(leader, followerA);
    final String type = uniqueName("Issue8343Doc");
    final String requestId = uniqueName("issue8343-request-id");
    createType(leader, type);

    // Sleeps well past the leader's 5 s in-flight wait, so the retry is refused rather than replayed.
    final String payload = new JSONObject().put("language", "sqlscript")
        .put("command", "SLEEP 15000; INSERT INTO " + type + " SET tag = 'inflight';").toString();

    final IdempotencyCache leaderCache = getServer(leader).getHttpServer().getIdempotencyCache();
    final int leaderEntriesBefore = leaderCache.size();

    final CompletableFuture<HttpResponse<String>> first = CompletableFuture.supplyAsync(
        () -> postCommand(followerA, payload, requestId));

    // The forward reached the leader once its pending marker for the id is in the leader's cache.
    await().atMost(30, TimeUnit.SECONDS).pollInterval(20, TimeUnit.MILLISECONDS)
        .until(() -> leaderCache.size() > leaderEntriesBefore || first.isDone());
    assertThat(first.isDone()).as("the first attempt settled before the retry could be sent: %s",
        first.isDone() ? first.get().body() : "").isFalse();

    final HttpResponse<String> retry = postCommand(followerB, payload, requestId);

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

    await().atMost(30, TimeUnit.SECONDS).until(() -> countTagged(leader, type, "inflight") >= 1L);
    assertThat(countTagged(leader, type, "inflight"))
        .as("the refused retry must not have executed the write a second time")
        .isEqualTo(1L);
  }

  // ------------------------------------------------------------------ direct leader retry (#8347)

  /** Issue #8347, from {@code Issue8347DirectLeaderRetryIT}. */
  @Tag("slow")
  @Test
  @Timeout(180)
  void aRetrySentStraightToTheLeaderAfterAFollowerForwardedTheWriteExecutesOnce() throws Exception {
    final int leader = findLeaderIndex();
    final int follower = follower(leader);
    final String type = uniqueName("Issue8347Doc");
    final String requestId = uniqueName("issue8347-follower-then-leader");
    createType(leader, type);

    final String payload = clientBody(type, "follower-then-leader");

    final HttpResponse<String> first = command(follower, payload, requestId);
    assertThat(first.statusCode()).as("first attempt, body: %s", first.body()).isEqualTo(200);

    final HttpResponse<String> retry = command(leader, payload, requestId);
    assertThat(retry.statusCode()).as("retry, body: %s", retry.body()).isEqualTo(200);

    assertThat(countTagged(leader, type, "follower-then-leader"))
        .as("the retry must be answered from the entry the forward settled on the leader, not executed again")
        .isEqualTo(1L);
  }

  /** Issue #8347, from {@code Issue8347DirectLeaderRetryIT}. */
  @Tag("slow")
  @Test
  @Timeout(180)
  void aRetryAFollowerForwardsAfterTheClientWentStraightToTheLeaderExecutesOnce() throws Exception {
    final int leader = findLeaderIndex();
    final int follower = follower(leader);
    final String type = uniqueName("Issue8347Doc");
    final String requestId = uniqueName("issue8347-leader-then-follower");
    createType(leader, type);

    final String payload = clientBody(type, "leader-then-follower");

    final HttpResponse<String> first = command(leader, payload, requestId);
    assertThat(first.statusCode()).as("first attempt, body: %s", first.body()).isEqualTo(200);

    final HttpResponse<String> retry = command(follower, payload, requestId);
    assertThat(retry.statusCode()).as("retry, body: %s", retry.body()).isEqualTo(200);

    assertThat(countTagged(leader, type, "leader-then-follower")).isEqualTo(1L);
  }

  // ------------------------------------------------------------------ replay rendering (#8359)

  /** Issue #8359, from {@code Issue8359ReplayRenderingIT}. */
  @Tag("slow")
  @Test
  @Timeout(180)
  void aDirectRetryOfAForwardedWriteIsReplayedInTheRetrysSerializer() throws Exception {
    final int leader = findLeaderIndex();
    final int follower = follower(leader);
    final String type = uniqueName("Issue8359Doc");
    final String requestId = uniqueName("issue8359-follower-then-leader");
    createType(leader, type);

    final String payload = studioBody(type, "follower-then-leader");

    final HttpResponse<String> first = command(follower, payload, requestId);
    assertThat(first.statusCode()).as("first attempt, body: %s", first.body()).isEqualTo(200);
    assertStudioRecord(first.body(), "follower-then-leader");

    final HttpResponse<String> retry = command(leader, payload, requestId);
    assertThat(retry.statusCode()).as("retry, body: %s", retry.body()).isEqualTo(200);
    assertStudioRecord(retry.body(), "follower-then-leader");
    assertThat(retry.body()).as("the retry is a replay of the same answer").isEqualTo(first.body());

    assertThat(countTagged(leader, type, "follower-then-leader")).as("executed once").isEqualTo(1L);
  }

  /** Issue #8359, from {@code Issue8359ReplayRenderingIT}. */
  @Tag("slow")
  @Test
  @Timeout(180)
  void aForwardedRetryOfADirectWriteIsAnsweredInTheClientsSerializer() throws Exception {
    final int leader = findLeaderIndex();
    final int follower = follower(leader);
    final String type = uniqueName("Issue8359Doc");
    final String requestId = uniqueName("issue8359-leader-then-follower");
    createType(leader, type);

    final String payload = studioBody(type, "leader-then-follower");

    final HttpResponse<String> first = command(leader, payload, requestId);
    assertThat(first.statusCode()).as("first attempt, body: %s", first.body()).isEqualTo(200);
    assertStudioRecord(first.body(), "leader-then-follower");

    final HttpResponse<String> retry = command(follower, payload, requestId);
    assertThat(retry.statusCode()).as("retry, body: %s", retry.body()).isEqualTo(200);
    assertStudioRecord(retry.body(), "leader-then-follower");
    assertThat(retry.body()).as("the retry is a replay of the same answer").isEqualTo(first.body());

    assertThat(countTagged(leader, type, "leader-then-follower")).as("executed once").isEqualTo(1L);
  }

  /**
   * Issue #8359, from {@code Issue8359ReplayRenderingIT}.
   * <p>
   * Without an X-Request-Id nothing is cached, and a write sent to a follower is still answered in the rows parsed back.
   */
  @Tag("slow")
  @Test
  @Timeout(180)
  void aForwardWithoutARequestIdStillAnswersItsRows() throws Exception {
    final int leader = findLeaderIndex();
    final int follower = follower(leader);
    final String type = uniqueName("Issue8359Doc");
    createType(leader, type);

    final HttpResponse<String> response = command(follower,
        "{\"language\":\"sql\",\"command\":\"INSERT INTO " + type + " SET tag = 'no-id'\"}", null);
    assertThat(response.statusCode()).as("body: %s", response.body()).isEqualTo(200);
    final JSONArray rows = new JSONObject(response.body()).getJSONArray("result");
    assertThat(rows.length()).isEqualTo(1);
    assertThat(rows.getJSONObject(0).getString("tag")).isEqualTo("no-id");
  }

  // ------------------------------------------------------------------ the forward hop (#7603, #8393)

  /**
   * Issue #7603, from {@code Issue7603LeaderForwardHopIT}.
   * <p>
   * The reported repro, with the retry sent to the other follower. Before the fix the id never reached the leader,
   * so the second request created the user a second time there - answered with an error, because the user already
   * exists - instead of replaying the first answer.
   */
  @Tag("slow")
  @Test
  @Timeout(180)
  void aRetryWithTheSameRequestIdIsDeduplicatedOnTheLeaderWhicheverFollowerItLandsOn() throws Exception {
    final int leader = findLeaderIndex();
    assertThat(leader).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int followerA = follower(leader, -1);
    final int followerB = follower(leader, followerA);

    final String name = uniqueName("issue7603dedup");
    final String requestId = uniqueName("issue7603-request-id");
    final String payload = new JSONObject().put("command",
        "create user { \"name\": \"" + name + "\", \"password\": \"issue7603password\", \"databases\": {} }").toString();
    final UnaryOperator<HttpRequest.Builder> withId = b -> b
        .header("Authorization", basic("root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS))
        .header(IdempotencyCache.HEADER_REQUEST_ID, requestId);

    final HttpResponse<String> first = send(followerA, "/api/v1/server", payload, withId);
    assertThat(first.statusCode()).as("first attempt, body: %s", first.body()).isEqualTo(200);
    awaitUserOnEveryServer(name);

    final HttpResponse<String> retry = send(followerB, "/api/v1/server", payload, withId);
    assertThat(retry.statusCode())
        .as("the retry must be answered from the leader's cache, not executed again, body: %s", retry.body())
        .isEqualTo(200);
    assertThat(retry.body()).isEqualTo(first.body());
  }

  /** Issue #7603, from {@code Issue7603LeaderForwardHopIT}. */
  @Tag("slow")
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

  /** Issue #7603, from {@code Issue7603LeaderForwardHopIT}. */
  @Tag("slow")
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

  /** Issue #7603, from {@code Issue7603LeaderForwardHopIT}. */
  @Tag("slow")
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
   * Issue #8393, from {@code Issue7603LeaderForwardHopIT}.
   * <p>
   * A peer that names no intended leader - one on an older build during a rolling upgrade, or one that saw leadership
   * change while it resolved the leader's address - proves nothing about the address. All three refusal sites answer
   * it as the election it usually is: 503, retryable, and the once-per-database configuration warning is left for the
   * refusal that does prove the address wrong.
   * <p>
   * Runs right after the first test: {@link #aSqlWriteRefusedBecauseLeadershipMovedIsRetryable()} sets that
   * once-per-database latch on the same follower, and it is never reset while the cluster runs.
   */
  @Tag("slow")
  @Test
  @Order(2)
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

  // ------------------------------------------------------------------ Basic-auth forward loop (#7516)

  /**
   * Issue #7516, from {@code Issue7516BasicAuthForwardLoopIT}.
   * <p>
   * The trust gate, which the fix must not widen: the marker is honoured only when a valid cluster token travels beside
   * it. A client that copies the header onto its own Basic-authenticated write - or a proxy that relays unknown
   * {@code X-ArcadeDB-*} headers through - must not be able to turn its own transparent forward-to-leader into a
   * refusal.
   */
  @Tag("slow")
  @Test
  @Timeout(180)
  void aMarkerWithoutTheClusterTokenDoesNotSuppressForwarding() throws Exception {
    final int leader = findLeaderIndex();
    assertThat(leader).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int follower = firstFollower(leader);

    final String name = uniqueName("issue7516forgedmarker");
    final HttpResponse<String> response = send(follower, "POST", "/api/v1/server/users",
        new JSONObject().put("name", name).put("password", PASSWORD_7516).toString(),
        b -> b.header("Authorization", basic("root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS))
            .header(LeaderForwardContext.FORWARDED_TO_LEADER_HEADER, "true"));

    assertThat(response.statusCode())
        .as("a client's own marker must not stop this follower forwarding, body: %s", response.body())
        .isEqualTo(201);
    awaitUserOnEveryServer(name);
  }

  /**
   * Issue #7516, from {@code Issue7516BasicAuthForwardLoopIT}.
   * <p>
   * The branch that always carried the marker, driven end to end for the first time. It authenticates to the leader with
   * the cluster token, which {@code LeaderCommandForwarder} read from the raw {@code arcadedb.ha.clusterToken} setting -
   * empty on every cluster that did not declare one explicitly, because {@code ClusterTokenProvider} derives the
   * effective token at startup and never writes it back into the configuration. The forward then went out with no
   * credentials at all and was answered 401.
   */
  @Tag("slow")
  @Test
  @Timeout(180)
  void aSessionTokenWriteOnAFollowerIsForwardedAndExecutedOnTheLeader() throws Exception {
    final int leader = findLeaderIndex();
    assertThat(leader).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int follower = firstFollower(leader);

    assertThat(getRaftPlugin(follower).getRaftHAServer().getClusterToken())
        .as("this cluster declares no explicit cluster token, so the effective one is derived at startup")
        .isNotBlank();
    assertThat(getServer(follower).getConfiguration()
        .getValueAsString(GlobalConfiguration.HA_CLUSTER_TOKEN))
        .as("and the derived token is NOT written back into the configuration - reading it there is the defect")
        .isNullOrEmpty();

    final HttpResponse<String> login = send(follower, "POST", "/api/v1/login", null,
        b -> b.header("Authorization", basic("root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)));
    assertThat(login.statusCode()).as("login on a follower, body: %s", login.body()).isEqualTo(200);
    final String sessionToken = new JSONObject(login.body()).getString("token");
    assertThat(sessionToken).startsWith("AU-");

    final String name = uniqueName("issue7516sessiontoken");
    final HttpResponse<String> created = send(follower, "POST", "/api/v1/server/users",
        new JSONObject().put("name", name).put("password", PASSWORD_7516).toString(),
        b -> b.header("Authorization", "Bearer " + sessionToken));

    assertThat(created.statusCode())
        .as("a session-token write forwarded to the leader must be authenticated there, body: %s", created.body())
        .isEqualTo(201);
    awaitUserOnEveryServer(name);
  }

  // ------------------------------------------------------------------ follower forward loop (#6191)

  /**
   * Issue #6191, from {@code Issue6191FollowerForwardLoopIT}.
   * <p>
   * Control: with the addresses the cluster actually has, a write issued on a follower is forwarded to the leader and
   * executed. Without it the two tests above would also pass against a build that refused every forwarded write.
   */
  @Test
  @Timeout(120)
  void aFollowerWriteWithCorrectAddressesStillReachesTheLeader() {
    final int leader = findLeaderIndex();
    assertThat(leader).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int follower = firstFollower(leader);
    final String type = uniqueName("Issue6191Forwarded");

    getServerDatabase(follower, getDatabaseName()).command("sql", "CREATE VERTEX TYPE " + type);
    waitForAllServers();

    assertThat(getServerDatabase(leader, getDatabaseName()).getSchema().existsType(type))
        .as("the forwarded DDL must have been executed by the leader")
        .isTrue();
  }

  /**
   * Issue #6191, from {@code Issue6191FollowerForwardLoopIT}.
   * <p>
   * The bulk-load endpoint relays to the leader too, with a whole upload buffered per hop, so it has to enforce the same
   * one-hop rule. Driven over real HTTP because the marker is a request header, and sent the way a peer sends it - with
   * the cluster token, the only form in which the marker is trusted.
   */
  @Test
  @Timeout(120)
  void aBatchForwardedOntoAFollowerIsRefusedInsteadOfForwardedAgain() throws Exception {
    final int leader = findLeaderIndex();
    assertThat(leader).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int follower = firstFollower(leader);
    final String clusterToken = getRaftPlugin(follower).getRaftHAServer().getClusterToken();
    assertThat(clusterToken).as("peers authenticate to each other with a cluster token").isNotBlank();

    final HttpURLConnection conn = (HttpURLConnection) new URI(
        "http://127.0.0.1:" + getServer(follower).getHttpServer().getPort() + "/api/v1/batch/" + getDatabaseName())
        .toURL().openConnection();
    try {
      conn.setRequestMethod("POST");
      conn.setRequestProperty("X-ArcadeDB-Cluster-Token", clusterToken);
      conn.setRequestProperty("X-ArcadeDB-Forwarded-User", "root");
      conn.setRequestProperty("Content-Type", "application/x-ndjson");
      conn.setRequestProperty(LeaderForwardContext.FORWARDED_TO_LEADER_HEADER, "true");
      conn.setDoOutput(true);
      final byte[] payload = "{\"@type\":\"Issue6191Batch\",\"id\":1}\n".getBytes(StandardCharsets.UTF_8);
      conn.setRequestProperty("Content-Length", Integer.toString(payload.length));
      try (final DataOutputStream out = new DataOutputStream(conn.getOutputStream())) {
        out.write(payload);
      }

      // No intended leader id is sent, as from a peer on an older build: retryable, not the configuration error
      // (issue #8393). The serverList advice is still offered as the secondary cause.
      assertThat(conn.getResponseCode())
          .as("a batch that arrives already forwarded must be refused by this follower, not relayed again")
          .isEqualTo(503);
      assertThat(new String(conn.getErrorStream().readAllBytes(), StandardCharsets.UTF_8))
          .contains("already forwarded")
          .contains(GlobalConfiguration.HA_SERVER_LIST.getKey());
    } finally {
      conn.disconnect();
    }
  }

  /**
   * Issue #6191, from {@code Issue6191FollowerForwardLoopIT}.
   * <p>
   * The gate on the marker: it is a statement one node makes to another, so it is honored only on a request that
   * authenticated with the cluster token. A client that copies the header onto its own write - or a proxy that relays
   * unknown {@code X-ArcadeDB-*} headers through - must not be able to turn a transparent forward-to-leader into a
   * refusal for itself.
   */
  @Test
  @Timeout(120)
  void theMarkerFromAnOrdinaryClientDoesNotSuppressForwarding() throws Exception {
    final int leader = findLeaderIndex();
    assertThat(leader).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int follower = firstFollower(leader);
    final String type = uniqueName("Issue6191ClientMarker");

    final HttpURLConnection conn = (HttpURLConnection) new URI(
        "http://127.0.0.1:" + getServer(follower).getHttpServer().getPort() + "/api/v1/command/" + getDatabaseName())
        .toURL().openConnection();
    try {
      conn.setRequestMethod("POST");
      conn.setRequestProperty("Authorization", "Basic " + Base64.getEncoder().encodeToString(
          ("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)));
      conn.setRequestProperty("Content-Type", "application/json");
      conn.setRequestProperty(LeaderForwardContext.FORWARDED_TO_LEADER_HEADER, "true");
      conn.setDoOutput(true);
      final byte[] payload = ("{\"language\":\"sql\",\"command\":\"CREATE VERTEX TYPE " + type + "\"}")
          .getBytes(StandardCharsets.UTF_8);
      try (final DataOutputStream out = new DataOutputStream(conn.getOutputStream())) {
        out.write(payload);
      }

      assertThat(conn.getResponseCode())
          .as("a client's own header must not stop this follower from forwarding its write to the leader")
          .isEqualTo(200);
    } finally {
      conn.disconnect();
    }

    waitForAllServers();
    assertThat(getServerDatabase(leader, getDatabaseName()).getSchema().existsType(type))
        .as("the write was forwarded and executed by the leader")
        .isTrue();
  }

  // ------------------------------------------------------------------ forwarded query terminate (#9689)

  /** Issue #9689, from {@code Issue9689ForwardedQueryTerminateIT}. */
  @Tag("slow")
  @Test
  @Timeout(180)
  void terminatingAForwardedWriteOnTheReplicaStopsItOnTheLeader() throws Exception {
    final int leader = findLeaderIndex();
    assertThat(leader).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int replica = leader == 0 ? 1 : 0;
    final String type = uniqueName("Written9689");

    final Database leaderDb = getServerDatabase(leader, getDatabaseName());
    if (!leaderDb.getSchema().existsType(type))
      leaderDb.command("sql", "CREATE VERTEX TYPE " + type + " IF NOT EXISTS");
    await().atMost(30, TimeUnit.SECONDS).until(() -> getServerDatabase(replica, getDatabaseName()).getSchema().existsType(type));

    final CompletableFuture<HttpResponse<String>> running = HTTP.sendAsync(HttpRequest.newBuilder()
        .uri(URI.create(baseUrl(replica) + "/api/v1/command/" + getDatabaseName())).timeout(Duration.ofSeconds(120))
        .header("Content-Type", "application/json").header("Authorization", basic()).header("X-ArcadeDB-Query-Tag", TAG_9689)
        .POST(HttpRequest.BodyPublishers.ofString(
            new JSONObject().put("language", "opencypher").put("command", longWrite(type)).toString(), StandardCharsets.UTF_8))
        .build(), HttpResponse.BodyHandlers.ofString());

    // The replica lists the statement as forwarded to the leader...
    final JSONObject onReplica = awaitSingle(replica, "forwardedTo");
    // ...and the leader lists its own entry for it, with the replica's id and the client's tag
    final JSONObject onLeader = awaitSingle(leader, "forwardedFrom");
    assertThat(onLeader.getString("forwardedFrom", null)).isEqualTo(onReplica.getString("id"));
    assertThat(onLeader.getString("tag")).isEqualTo(TAG_9689);

    final JSONObject terminated = serverCommand(replica, "terminate query " + onReplica.getString("id")).getJSONObject("result");
    assertThat(terminated.getString("status")).as("the terminate on the replica: %s", terminated).isEqualTo("terminated");

    final HttpResponse<String> response = running.get(60, TimeUnit.SECONDS);
    assertThat(response.statusCode()).as("body: %s", response.body()).isEqualTo(409);

    // The work stopped on the leader, and wrote nothing
    await().atMost(30, TimeUnit.SECONDS).until(() -> listQueries(leader).length() == 0);
    assertThat(listQueries(replica).length()).isZero();
    assertThat(leaderDb.countType(type, false)).isZero();
  }

  // ------------------------------------------------------------------ helpers: types and counts (#8323, #8343, #8347, #8359)

  private void createType(final int leader, final String type) {
    final Database db = getServerDatabase(leader, getDatabaseName());
    if (!db.getSchema().existsType(type))
      db.command("sql", "CREATE DOCUMENT TYPE " + type + " IF NOT EXISTS");
    await().atMost(30, TimeUnit.SECONDS).until(() -> {
      for (int i = 0; i < getServerCount(); i++)
        if (!getServerDatabase(i, getDatabaseName()).getSchema().existsType(type))
          return false;
      return true;
    });
  }

  private long countTagged(final int serverIndex, final String type, final String tag) {
    final Database db = getServerDatabase(serverIndex, getDatabaseName());
    return ((Number) db.query("sql", "SELECT count(*) AS cnt FROM " + type + " WHERE tag = ?", tag).next()
        .getProperty("cnt")).longValue();
  }

  /** #8347: a client's own body: fields the follower's rebuilt forward does not carry, in its own order and spacing. */
  private static String clientBody(final String type, final String tag) {
    return "{ \"serializer\": \"record\", \"limit\": 20, \"language\": \"sql\", \"command\": \"INSERT INTO " + type
        + " SET tag = '" + tag + "'\" }";
  }

  /** #8359: a client asking for the studio rendering, which a forward rebuilt from the statement never did. */
  private static String studioBody(final String type, final String tag) {
    return "{ \"serializer\": \"studio\", \"limit\": 20, \"language\": \"sql\", \"command\": \"INSERT INTO " + type
        + " SET tag = '" + tag + "'\" }";
  }

  /** #8359: the studio rendering: an object with vertices, edges and records - never a row array - carrying the one record. */
  private static void assertStudioRecord(final String body, final String tag) {
    final JSONObject json = new JSONObject(body);
    assertThat(json.get("result")).as("studio rendering, body: %s", body).isInstanceOf(JSONObject.class);
    final JSONArray records = json.getJSONObject("result").getJSONArray("records");
    assertThat(records.length()).as("body: %s", body).isEqualTo(1);
    assertThat(records.getJSONObject(0).getString("tag")).isEqualTo(tag);
  }

  // ------------------------------------------------------------------ helpers: HTTP

  /** {@code POST /api/v1/command} with a 30 s timeout and an optional request id (#8323, #8347, #8359). */
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

  /** {@code POST /api/v1/command} with a 90 s timeout and a request id, unchecked for an async caller (#8343). */
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

  /**
   * #7603: what a peer relaying a Basic-authenticated request to {@code intendedLeader} sends; with a null
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

  /** #7603: a JSON {@code POST} with a 30 s timeout. */
  private HttpResponse<String> send(final int serverIndex, final String path, final String body,
      final UnaryOperator<HttpRequest.Builder> decorate) throws Exception {
    final HttpRequest.Builder builder = decorate.apply(HttpRequest.newBuilder()
        .uri(URI.create("http://127.0.0.1:" + getServer(serverIndex).getHttpServer().getPort() + path))
        .timeout(Duration.ofSeconds(30))
        .header("Content-Type", "application/json"));
    return HTTP.send(builder.POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8)).build(),
        HttpResponse.BodyHandlers.ofString());
  }

  /** #7603: an NDJSON {@code POST /api/v1/batch} with a 30 s timeout. */
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

  /** #7516: any method, with {@link #REQUEST_TIMEOUT_7516}; a null body is sent as no body. */
  private HttpResponse<String> send(final int serverIndex, final String method, final String path,
      final String body, final UnaryOperator<HttpRequest.Builder> decorate) throws Exception {
    final int port = getServer(serverIndex).getHttpServer().getPort();
    HttpRequest.Builder builder = HttpRequest.newBuilder()
        .uri(URI.create("http://127.0.0.1:" + port + path))
        .timeout(REQUEST_TIMEOUT_7516);
    builder = decorate.apply(builder);
    if (body != null) {
      builder.header("Content-Type", "application/json");
      builder.method(method, HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8));
    } else
      builder.method(method, HttpRequest.BodyPublishers.noBody());
    return HTTP.send(builder.build(), HttpResponse.BodyHandlers.ofString());
  }

  /** #9689: about 16 s on one core when left alone, then a write: forwarded to the leader, where it runs. */
  private static String longWrite(final String type) {
    return "UNWIND range(1, 10000) AS i UNWIND range(1, 10000) AS j WITH sum(sin(toFloat(i * j))) AS total "
        + "CREATE (:" + type + " {total: total})";
  }

  /** #9689: the one statement {@code server} lists under the tag, once it carries {@code property}. */
  private JSONObject awaitSingle(final int server, final String property) {
    final JSONObject[] found = new JSONObject[1];
    await().atMost(30, TimeUnit.SECONDS).pollInterval(Duration.ofMillis(50)).until(() -> {
      final JSONArray list = listQueries(server);
      if (list.length() == 1 && list.getJSONObject(0).has(property)) {
        found[0] = list.getJSONObject(0);
        return true;
      }
      return false;
    });
    return found[0];
  }

  private JSONArray listQueries(final int server) throws Exception {
    return serverCommand(server, "list queries tag " + TAG_9689).getJSONArray("result");
  }

  private JSONObject serverCommand(final int server, final String command) throws Exception {
    final HttpResponse<String> response = HTTP.send(HttpRequest.newBuilder()
        .uri(URI.create(baseUrl(server) + "/api/v1/server")).timeout(Duration.ofSeconds(30))
        .header("Content-Type", "application/json").header("Authorization", basic())
        .POST(HttpRequest.BodyPublishers.ofString(new JSONObject().put("command", command).toString(), StandardCharsets.UTF_8))
        .build(), HttpResponse.BodyHandlers.ofString());
    assertThat(response.statusCode()).as("%s: %s", command, response.body()).isEqualTo(200);
    return new JSONObject(response.body());
  }

  private String baseUrl(final int server) {
    return "http://127.0.0.1:" + getServer(server).getHttpServer().getPort();
  }

  // ------------------------------------------------------------------ helpers: credentials and roles

  private static String basic(final String user, final String password) {
    return "Basic " + Base64.getEncoder().encodeToString((user + ":" + password).getBytes(StandardCharsets.UTF_8));
  }

  /** #9689: root's Basic credentials. */
  private static String basic() {
    return basic("root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
  }

  private int follower(final int leader, final int other) {
    assertThat(leader).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    for (int i = 0; i < getServerCount(); i++)
      if (i != leader && i != other)
        return i;
    throw new IllegalStateException("no follower in a " + getServerCount() + "-node cluster");
  }

  private int follower(final int leader) {
    return follower(leader, -1);
  }

  private int firstFollower(final int leader) {
    for (int i = 0; i < getServerCount(); i++)
      if (i != leader)
        return i;
    throw new IllegalStateException("no follower in a " + getServerCount() + "-node cluster");
  }
}
