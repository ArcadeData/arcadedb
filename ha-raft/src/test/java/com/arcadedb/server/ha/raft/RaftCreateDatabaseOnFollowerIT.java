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
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11739: {@code create database} sent to a follower is forwarded to the leader, and the follower relayed the leader's 200
 * at once. It installs the database only when it applies the entry the leader committed, so the client's next request to the
 * same follower was refused with {@code DatabaseNotFoundException}. The follower's apply thread is held off the new database
 * here until the test has seen the leader create it and the follower's answer still pending, which makes that window certain.
 * <p>
 * The follower answers once it has applied what the leader had committed, which it asks the leader for once the forwarded
 * command is answered. The tests also pin the cases a wait on the state of the database would get wrong: a database of the
 * same name still on the follower (dropped on the leader and created again), and one the follower closed, which is not
 * registered on it although its files are.
 */
@Tag("slow")
class RaftCreateDatabaseOnFollowerIT extends BaseRaftHATest {
  private static final String       NEW_DATABASE         = "createdThroughAFollower";
  private static final String       DROPPED_DATABASE     = "droppedThroughAFollower";
  private static final String       RECREATED_DATABASE   = "recreatedThroughAFollower";
  private static final String       CLOSED_DATABASE      = "closedThenDroppedThroughAFollower";
  private static final List<String> TEST_DATABASES       = List.of(NEW_DATABASE, DROPPED_DATABASE, RECREATED_DATABASE, CLOSED_DATABASE);
  private static final Duration     HTTP_TIMEOUT         = Duration.ofSeconds(30);

  private final HttpClient httpClient = HttpClient.newBuilder().connectTimeout(HTTP_TIMEOUT).build();

  /** The databases the test created are dropped through the leader, and the client is closed whatever that did. */
  @AfterEach
  void dropTheDatabasesAndCloseTheClient() throws Exception {
    try {
      final int leaderIndex = findLeaderIndex();
      if (leaderIndex >= 0)
        for (final String database : TEST_DATABASES)
          if (getServer(leaderIndex).isDatabaseOfServer(database))
            send(leaderIndex, "/server", new JSONObject().put("command", "drop database " + database)).get(30, TimeUnit.SECONDS);
    } finally {
      httpClient.close();
    }
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
      awaitAnswerOrLocalWait(created, followerIndex);
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
      awaitAnswerOrLocalWait(dropped, followerIndex);
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
   * A creation is not answered because a database of that name is on the follower: when the leader dropped the database and
   * it was created again through a follower that had not applied the drop yet, the follower still holds the OLD copy, and
   * answering at once would let the client's next request run against it. The wait is for the entry of this creation.
   */
  @Test
  void aRecreationIsAnsweredOnlyOnceTheFollowerAppliedTheDropAndTheNewInstall() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int followerIndex = (leaderIndex + 1) % getServerCount();

    final HttpResponse<String> created = send(leaderIndex, "/server",
        new JSONObject().put("command", "create database " + RECREATED_DATABASE)).get(30, TimeUnit.SECONDS);
    assertThat(created.statusCode()).as("create database failed: %s", created.body()).isEqualTo(200);
    final HttpResponse<String> typed = send(leaderIndex, "/command/" + RECREATED_DATABASE,
        new JSONObject().put("language", "sql").put("command", "CREATE DOCUMENT TYPE OldGeneration")).get(30, TimeUnit.SECONDS);
    assertThat(typed.statusCode()).as("CREATE DOCUMENT TYPE failed: %s", typed.body()).isEqualTo(200);
    awaitUntil("the follower holds the first generation of '" + RECREATED_DATABASE + "'",
        () -> getServer(followerIndex).existsDatabase(RECREATED_DATABASE) && hasType(followerIndex, RECREATED_DATABASE, "OldGeneration"));

    final CountDownLatch release = new CountDownLatch(1);
    final Thread gateHolder = holdApply(followerIndex, RECREATED_DATABASE, release);
    final CompletableFuture<HttpResponse<String>> recreated;
    try {
      final HttpResponse<String> dropped = send(leaderIndex, "/server",
          new JSONObject().put("command", "drop database " + RECREATED_DATABASE)).get(30, TimeUnit.SECONDS);
      assertThat(dropped.statusCode()).as("drop database failed: %s", dropped.body()).isEqualTo(200);
      assertThat(getServer(followerIndex).existsDatabase(RECREATED_DATABASE))
          .as("the follower has not applied the drop: it still holds the first generation").isTrue();

      recreated = send(followerIndex, "/server", new JSONObject().put("command", "create database " + RECREATED_DATABASE));

      awaitOnServer(leaderIndex, RECREATED_DATABASE, true);
      awaitAnswerOrLocalWait(recreated, followerIndex);
      assertThat(recreated.isDone())
          .as("the follower must not answer the creation while the database it holds is the one that was dropped").isFalse();
    } finally {
      release.countDown();
      gateHolder.join(TimeUnit.SECONDS.toMillis(30));
    }

    final HttpResponse<String> answer = recreated.get(30, TimeUnit.SECONDS);
    assertThat(answer.statusCode()).as("create database failed: %s", answer.body()).isEqualTo(200);
    assertThat(hasType(followerIndex, RECREATED_DATABASE, "OldGeneration"))
        .as("the follower that answered the creation holds the new database, not the dropped one").isFalse();
  }

  /**
   * A database the follower closed with {@code close database} is not registered on it, but its files remain and
   * {@code GET /api/v1/exists} still answers {@code true}. The drop is therefore answered once the follower applied the
   * entry, not because the database already looked gone.
   */
  @Test
  void aDropOfADatabaseTheFollowerClosedIsAnsweredOnlyOnceTheFollowerAppliedIt() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int followerIndex = (leaderIndex + 1) % getServerCount();

    final HttpResponse<String> created = send(leaderIndex, "/server",
        new JSONObject().put("command", "create database " + CLOSED_DATABASE)).get(30, TimeUnit.SECONDS);
    assertThat(created.statusCode()).as("create database failed: %s", created.body()).isEqualTo(200);
    awaitOnServer(followerIndex, CLOSED_DATABASE, true);

    final HttpResponse<String> closed = send(followerIndex, "/server",
        new JSONObject().put("command", "close database " + CLOSED_DATABASE)).get(30, TimeUnit.SECONDS);
    assertThat(closed.statusCode()).as("close database failed: %s", closed.body()).isEqualTo(200);
    assertThat(getServer(followerIndex).existsDatabase(CLOSED_DATABASE)).as("the closed database is not registered").isFalse();
    assertThat(getServer(followerIndex).isDatabaseOfServer(CLOSED_DATABASE)).as("its files are still the server's").isTrue();

    final CountDownLatch release = new CountDownLatch(1);
    final Thread gateHolder = holdApply(followerIndex, CLOSED_DATABASE, release);
    final CompletableFuture<HttpResponse<String>> dropped;
    try {
      dropped = send(followerIndex, "/server", new JSONObject().put("command", "drop database " + CLOSED_DATABASE));

      awaitOnServer(leaderIndex, CLOSED_DATABASE, false);
      awaitAnswerOrLocalWait(dropped, followerIndex);
      assertThat(dropped.isDone()).as("the follower must not answer the drop while the closed database is still on it").isFalse();
    } finally {
      release.countDown();
      gateHolder.join(TimeUnit.SECONDS.toMillis(30));
    }

    final HttpResponse<String> answer = dropped.get(30, TimeUnit.SECONDS);
    assertThat(answer.statusCode()).as("drop database failed: %s", answer.body()).isEqualTo(200);

    final HttpResponse<String> exists = get(followerIndex, "/exists/" + CLOSED_DATABASE).get(30, TimeUnit.SECONDS);
    assertThat(new JSONObject(exists.body()).getBoolean("result", true))
        .as("GET /exists on the follower that answered the drop: %s", exists.body()).isFalse();
  }

  /**
   * Holds {@code serverIndex}'s apply thread off {@code database} until {@code release} counts down: the install apply gate is
   * the lock it takes before applying any entry for a database, the install and drop entries included. The leader still
   * commits with the other follower. Once it returns, releasing the holder is the caller's; when the gate is not taken in time
   * (or the wait is interrupted) the holder is released here, since the caller never gets it to release.
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
    boolean taken = false;
    try {
      taken = held.await(30, TimeUnit.SECONDS);
    } finally {
      if (!taken) {
        // A holder that takes the gate late would otherwise block the follower's apply for the rest of the 60 s teardown
        release.countDown();
        holder.join(TimeUnit.SECONDS.toMillis(30));
      }
    }
    assertThat(taken).as("the apply gate of '%s' on server %d was not taken in time", database, serverIndex).isTrue();
    return holder;
  }

  private void awaitOnServer(final int serverIndex, final String database, final boolean exists) throws InterruptedException {
    awaitUntil("database '" + database + "' " + (exists ? "exists" : "is gone") + " on server " + serverIndex,
        () -> getServer(serverIndex).existsDatabase(database) == exists);
  }

  private static void awaitUntil(final String what, final BooleanSupplier condition) throws InterruptedException {
    final long deadline = System.currentTimeMillis() + 30_000;
    while (!condition.getAsBoolean() && System.currentTimeMillis() < deadline)
      Thread.sleep(50);
    assertThat(condition.getAsBoolean()).as(what).isTrue();
  }

  private boolean hasType(final int serverIndex, final String database, final String typeName) {
    return getServer(serverIndex).getDatabase(database).getSchema().existsType(typeName);
  }

  /**
   * Waits until {@code request} is answered or a handler of server {@code followerIndex} waits in
   * {@link RaftHAPlugin#awaitLeaderCommitApplied}, which it enters only once the leader's answer to the command it forwarded
   * has arrived. A follower that relays that answer at once ends the request instead, so the check after this does not depend
   * on how long the forwarded answer takes.
   */
  private void awaitAnswerOrLocalWait(final CompletableFuture<?> request, final int followerIndex) throws InterruptedException {
    final RaftHAPlugin plugin = getRaftPlugin(followerIndex);
    final long deadline = System.currentTimeMillis() + 30_000;
    while (!request.isDone() && plugin.localWaiters() == 0 && System.currentTimeMillis() < deadline)
      Thread.sleep(20);
    assertThat(request.isDone() || plugin.localWaiters() > 0).as("the follower got the leader's answer").isTrue();
  }

  private CompletableFuture<HttpResponse<String>> send(final int serverIndex, final String path, final JSONObject body) {
    return httpClient.sendAsync(requestTo(serverIndex, path).header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(body.toString())).build(), HttpResponse.BodyHandlers.ofString());
  }

  private CompletableFuture<HttpResponse<String>> get(final int serverIndex, final String path) {
    return httpClient.sendAsync(requestTo(serverIndex, path).GET().build(), HttpResponse.BodyHandlers.ofString());
  }

  private HttpRequest.Builder requestTo(final int serverIndex, final String path) {
    return HttpRequest.newBuilder()
        .uri(URI.create("http://127.0.0.1:" + getServer(serverIndex).getHttpServer().getPort() + "/api/v1" + path))
        .timeout(HTTP_TIMEOUT)
        .header("Authorization", "Basic " + Base64.getEncoder()
            .encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)));
  }
}
