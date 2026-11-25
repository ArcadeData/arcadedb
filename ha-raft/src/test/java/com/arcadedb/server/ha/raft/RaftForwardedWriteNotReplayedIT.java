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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12635: a write a follower forwards to the leader is durable there as soon as the leader answers it, so nothing on the
 * follower may run it a second time. The follower's HTTP handler runs every request inside {@code Database.transaction(...)} with
 * {@code arcadedb.txRetries} attempts, and it answered a forwarded write by serializing the records it returned from its OWN copy,
 * which had not applied the write yet. A record missing there surfaced as a retryable {@code ConcurrentModificationException}, the
 * retry loop ran the request again, and the leader executed the client's non-idempotent script twice:
 * {@code RaftHTTPGraphConcurrentIT} counted 402 edges for 400 scripts on every node.
 */
class RaftForwardedWriteNotReplayedIT extends BaseRaftHATest {
  private static final String   DOCUMENT_TYPE = "ForwardedNotReplayed";
  private static final String   VERTEX_TYPE   = "ForwardedNotReplayedVertex";
  private static final Duration HTTP_TIMEOUT  = Duration.ofSeconds(60);

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    // The leader commits with the other follower while the one under test is held off its database
    config.setValue(GlobalConfiguration.HA_QUORUM, "majority");
  }

  @Override
  protected int getServerCount() {
    return 3;
  }

  /**
   * The retry loop of {@code Database.transaction(...)} must not run a block again once a write of the block was forwarded to the
   * leader and answered: rolling back takes nothing back on the leader, so the replay writes it a second time.
   */
  @Test
  void aRetryLoopDoesNotReplayAWriteTheLeaderAlreadyCommitted() throws Exception {
    final int followerIndex = followerIndex();
    final Database follower = getServerDatabase(followerIndex, getDatabaseName());
    final AtomicInteger attempts = new AtomicInteger();

    assertThatThrownBy(() -> follower.transaction(() -> {
      attempts.incrementAndGet();
      follower.command("sql", "INSERT INTO " + DOCUMENT_TYPE + " SET k = 1").close();
      throw new ConcurrentModificationException("a conflict raised after the write was forwarded (issue #12635 test)");
    }, false, 3))
        // Issue #13251: and the conflict is raised as a failure no client retries, or the client replays the request instead
        .isInstanceOf(TransactionException.class).isNotInstanceOf(NeedRetryException.class)
        .satisfies(failure -> assertThat(failure.getSuppressed()).hasAtLeastOneElementOfType(ConcurrentModificationException.class));

    assertThat(attempts.get()).as("the block forwarded a write the leader committed, so it must not run again").isEqualTo(1);
    assertThat(countOnTheLeader("SELECT count(*) AS n FROM " + DOCUMENT_TYPE + " WHERE k = 1"))
        .as("the leader must hold the forwarded write exactly once").isEqualTo(1L);
  }

  /**
   * The follower answers a forwarded write only once it has applied it, so the records the answer carries are read from a copy
   * that holds them. The follower's apply thread is held off the database until the request is seen waiting for that apply, past
   * the leader's answer, and the answer must still be pending then: without the wait it was serialized at once, and the vertex
   * the statement created was not found there.
   */
  @Test
  void aForwardedWriteIsAnsweredWithTheRecordsItCreated() throws Exception {
    final int followerIndex = followerIndex();
    final RaftHAServer followerRaft = getRaftPlugin(followerIndex).getRaftHAServer();
    final String countOfTheVertex = "SELECT count(*) AS n FROM " + VERTEX_TYPE + " WHERE k = 1";

    final CountDownLatch held = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    final Thread gateHolder = new Thread(() -> {
      final ArcadeStateMachine.InstallApplyGate gate = followerRaft.getStateMachine().installApplyGate(getDatabaseName());
      gate.lock();
      try {
        held.countDown();
        release.await(120, TimeUnit.SECONDS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      } finally {
        gate.unlock();
      }
    }, "issue12635-follower-gate");
    gateHolder.start();

    final HttpClient client = HttpClient.newBuilder().connectTimeout(HTTP_TIMEOUT).build();
    try {
      assertThat(held.await(30, TimeUnit.SECONDS)).isTrue();

      final CompletableFuture<HttpResponse<String>> write = client.sendAsync(
          commandRequest(followerIndex, "CREATE VERTEX " + VERTEX_TYPE + " SET k = 1"), HttpResponse.BodyHandlers.ofString());

      // The leader commits with the other follower and answers; the request then waits for this follower to apply the write,
      // which it cannot while the gate is held. Seen on the request's own stack: the leader holding the write would not prove
      // its answer reached the follower, and releasing the gate before then would let the test pass without the wait
      Awaitility.await().atMost(HTTP_TIMEOUT).pollInterval(Duration.ofMillis(50))
          .until(() -> write.isDone() || aForwardWaitsForTheLocalApply());
      assertThat(write.isDone()).as("the follower answered the forwarded write before applying it: %s",
          write.isDone() ? write.get().body() : "").isFalse();
      assertThat(countOnTheLeader(countOfTheVertex)).as("the leader answered, so it holds the write").isEqualTo(1L);

      release.countDown();
      final HttpResponse<String> response = write.get(HTTP_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
      assertThat(response.statusCode()).as("the forwarded CREATE VERTEX failed: %s", response.body()).isEqualTo(200);
      assertThat(new JSONObject(response.body()).getJSONObject("result").getJSONArray("vertices").length())
          .as("the answer must carry the vertex the forwarded statement created: %s", response.body()).isEqualTo(1);
      assertThat(countOnTheLeader(countOfTheVertex)).as("the leader must hold the forwarded write exactly once").isEqualTo(1L);
    } finally {
      release.countDown();
      gateHolder.join(TimeUnit.SECONDS.toMillis(30));
      client.close();
    }
  }

  private int followerIndex() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final Database leader = getServerDatabase(leaderIndex, getDatabaseName());
    leader.command("sql", "CREATE DOCUMENT TYPE " + DOCUMENT_TYPE + " IF NOT EXISTS").close();
    leader.command("sql", "CREATE VERTEX TYPE " + VERTEX_TYPE + " IF NOT EXISTS").close();
    waitForAllServers();
    return (leaderIndex + 1) % getServerCount();
  }

  /** Whether a thread of this JVM is in the forward's wait for the local apply of the write the leader answered. */
  private static boolean aForwardWaitsForTheLocalApply() {
    for (final StackTraceElement[] stack : Thread.getAllStackTraces().values()) {
      boolean waits = false;
      for (final StackTraceElement frame : stack) {
        if (frame.getClassName().equals(AppliedIndexWaits.class.getName()) && frame.getMethodName().equals("awaitAppliedIndex"))
          waits = true;
        else if (waits && frame.getClassName().equals(RaftReplicatedDatabase.class.getName())
            && frame.getMethodName().equals("forwardCommandToLeaderViaRaft"))
          return true;
      }
    }
    return false;
  }

  private long countOnTheLeader(final String sql) {
    try (final ResultSet rs = getServerDatabase(findLeaderIndex(), getDatabaseName()).query("sql", sql)) {
      return rs.next().<Number>getProperty("n").longValue();
    }
  }

  /** The request sending {@code sql} to the server's {@code /command} endpoint, asking for the graph serializer as Studio does. */
  private HttpRequest commandRequest(final int serverIndex, final String sql) {
    return HttpRequest.newBuilder()
        .uri(URI.create("http://127.0.0.1:" + getServer(serverIndex).getHttpServer().getPort() + "/api/v1/command/"
            + getDatabaseName()))
        .timeout(HTTP_TIMEOUT)
        .header("Authorization", "Basic " + Base64.getEncoder()
            .encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(
            new JSONObject().put("language", "sql").put("command", sql).put("serializer", "graph").toString()))
        .build();
  }
}
