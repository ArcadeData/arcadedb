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
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11739: {@code create database} sent to a follower is forwarded to the leader, and the follower relayed the leader's 200
 * at once. It installs the database only when it applies the entry the leader committed, so the client's next request to the
 * same follower was refused with {@code DatabaseNotFoundException}. The follower's apply thread is held off the new database
 * here until the test has seen the leader create it and the follower's answer still pending, which makes that window certain.
 */
@Tag("slow")
class RaftCreateDatabaseOnFollowerIT extends BaseRaftHATest {
  private static final String   NEW_DATABASE     = "createdThroughAFollower";
  private static final String   DROPPED_DATABASE = "droppedThroughAFollower";
  private static final Duration HTTP_TIMEOUT     = Duration.ofSeconds(30);

  private final HttpClient httpClient = HttpClient.newBuilder().connectTimeout(HTTP_TIMEOUT).build();

  @AfterEach
  void closeHttpClient() {
    httpClient.close();
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM, "majority");
  }

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  void theFollowerAnswersTheCreationOnceItHoldsTheDatabase() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int followerIndex = (leaderIndex + 1) % getServerCount();

    final CountDownLatch release = new CountDownLatch(1);
    final Thread gateHolder = holdApply(followerIndex, NEW_DATABASE, release);
    final CompletableFuture<HttpResponse<String>> created;
    try {
      created = send(followerIndex, "/server", new JSONObject().put("command", "create database " + NEW_DATABASE));

      awaitOnServer(leaderIndex, NEW_DATABASE, true);
      awaitAnswerOrLocalWait(created);
      assertThat(created.isDone()).as("the follower must not answer the creation before it holds the database").isFalse();
    } finally {
      release.countDown();
      gateHolder.join(TimeUnit.SECONDS.toMillis(30));
    }

    final HttpResponse<String> answer = created.get(30, TimeUnit.SECONDS);
    assertThat(answer.statusCode()).as("create database failed: %s", answer.body()).isEqualTo(200);

    final HttpResponse<String> next = send(followerIndex, "/command/" + NEW_DATABASE,
        new JSONObject().put("language", "sql").put("command", "CREATE DOCUMENT TYPE AfterCreation")).get(30, TimeUnit.SECONDS);
    assertThat(next.statusCode()).as("the request right after the creation, to the same follower, was refused: %s", next.body())
        .isEqualTo(200);
  }

  /**
   * The mirror case: {@code drop database} forwarded by a follower was answered before the follower applied the drop, so
   * {@code GET /api/v1/exists} on the same follower still answered {@code true} (18 of 20 times on a 3-node cluster).
   */
  @Test
  void theFollowerAnswersTheDropOnceTheDatabaseIsGoneFromIt() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int followerIndex = (leaderIndex + 1) % getServerCount();

    final HttpResponse<String> created = send(leaderIndex, "/server",
        new JSONObject().put("command", "create database " + DROPPED_DATABASE)).get(30, TimeUnit.SECONDS);
    assertThat(created.statusCode()).as("create database failed: %s", created.body()).isEqualTo(200);
    awaitOnServer(followerIndex, DROPPED_DATABASE, true);

    final CountDownLatch release = new CountDownLatch(1);
    final Thread gateHolder = holdApply(followerIndex, DROPPED_DATABASE, release);
    final CompletableFuture<HttpResponse<String>> dropped;
    try {
      dropped = send(followerIndex, "/server", new JSONObject().put("command", "drop database " + DROPPED_DATABASE));

      awaitOnServer(leaderIndex, DROPPED_DATABASE, false);
      awaitAnswerOrLocalWait(dropped);
      assertThat(dropped.isDone()).as("the follower must not answer the drop while it still holds the database").isFalse();
    } finally {
      release.countDown();
      gateHolder.join(TimeUnit.SECONDS.toMillis(30));
    }

    final HttpResponse<String> answer = dropped.get(30, TimeUnit.SECONDS);
    assertThat(answer.statusCode()).as("drop database failed: %s", answer.body()).isEqualTo(200);
    assertThat(getServer(followerIndex).existsDatabase(DROPPED_DATABASE))
        .as("the follower that answered the drop must not list the database any more").isFalse();
  }

  /**
   * Holds {@code serverIndex}'s apply thread off {@code database} until {@code release} counts down: the install apply gate is
   * the lock it takes before applying any entry for a database, the install and drop entries included. The leader still
   * commits with the other follower.
   */
  private Thread holdApply(final int serverIndex, final String database, final CountDownLatch release)
      throws InterruptedException {
    final ArcadeStateMachine machine = getRaftPlugin(serverIndex).getRaftHAServer().getStateMachine();
    final CountDownLatch held = new CountDownLatch(1);
    final Thread holder = new Thread(() -> {
      final ArcadeStateMachine.InstallApplyGate gate = machine.installApplyGate(database);
      gate.lock();
      try {
        held.countDown();
        release.await(60, TimeUnit.SECONDS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      } finally {
        gate.unlock();
      }
    }, "issue11739-follower-gate");
    holder.start();
    assertThat(held.await(30, TimeUnit.SECONDS)).isTrue();
    return holder;
  }

  private void awaitOnServer(final int serverIndex, final String database, final boolean exists) throws InterruptedException {
    final long deadline = System.currentTimeMillis() + 30_000;
    while (getServer(serverIndex).existsDatabase(database) != exists && System.currentTimeMillis() < deadline)
      Thread.sleep(50);
    assertThat(getServer(serverIndex).existsDatabase(database)).as("database '%s' exists on server %d", database, serverIndex)
        .isEqualTo(exists);
  }

  /**
   * Waits until {@code request} is answered or a handler waits in {@link RaftHAPlugin#awaitLocally}, which it enters only once the
   * leader's answer to the command it forwarded has arrived. A follower that relays that answer at once ends the request instead,
   * so the check after this does not depend on how long the forwarded answer takes.
   */
  private static void awaitAnswerOrLocalWait(final CompletableFuture<?> request) throws InterruptedException {
    final long deadline = System.currentTimeMillis() + 30_000;
    while (!request.isDone() && !aHandlerAwaitsLocally() && System.currentTimeMillis() < deadline)
      Thread.sleep(20);
    assertThat(request.isDone() || aHandlerAwaitsLocally()).as("the follower got the leader's answer").isTrue();
  }

  /** Whether a thread of this JVM, which runs every server of the test, is in {@link RaftHAPlugin#awaitLocally}. */
  private static boolean aHandlerAwaitsLocally() {
    for (final StackTraceElement[] stack : Thread.getAllStackTraces().values())
      for (final StackTraceElement frame : stack)
        if (frame.getClassName().equals(RaftHAPlugin.class.getName()) && frame.getMethodName().equals("awaitLocally"))
          return true;
    return false;
  }

  private CompletableFuture<HttpResponse<String>> send(final int serverIndex, final String path, final JSONObject body) {
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create("http://127.0.0.1:" + getServer(serverIndex).getHttpServer().getPort() + "/api/v1" + path))
        .timeout(HTTP_TIMEOUT)
        .header("Authorization", "Basic " + Base64.getEncoder()
            .encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(body.toString()))
        .build();
    return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString());
  }
}
