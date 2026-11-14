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
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11739: {@code create database} sent to a follower is forwarded to the leader, and the follower relayed the leader's 200
 * at once. It installs the database only when it applies the entry the leader committed, so the client's next request to the
 * same follower was refused with {@code DatabaseNotFoundException}. The follower's apply thread is held off the new database
 * here for two seconds, which makes that window certain.
 */
@Tag("slow")
class RaftCreateDatabaseOnFollowerIT extends BaseRaftHATest {
  private static final String   NEW_DATABASE = "createdThroughAFollower";
  private static final Duration HTTP_TIMEOUT = Duration.ofSeconds(30);

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

    // Hold the follower's apply thread off the new database for two seconds: the install apply gate is the lock it takes before
    // applying an entry for a database, the one that installs it included. The leader still commits with the other follower.
    final ArcadeStateMachine followerMachine = getRaftPlugin(followerIndex).getRaftHAServer().getStateMachine();
    final CountDownLatch held = new CountDownLatch(1);
    final Thread gateHolder = new Thread(() -> {
      final ArcadeStateMachine.InstallApplyGate gate = followerMachine.installApplyGate(NEW_DATABASE);
      gate.lock();
      try {
        held.countDown();
        Thread.sleep(2_000);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      } finally {
        gate.unlock();
      }
    }, "issue11739-follower-gate");
    gateHolder.start();
    assertThat(held.await(30, TimeUnit.SECONDS)).isTrue();

    try {
      final HttpResponse<String> created = post(followerIndex, "/server", new JSONObject().put("command", "create database " + NEW_DATABASE));
      assertThat(created.statusCode()).as("create database failed: %s", created.body()).isEqualTo(200);

      final HttpResponse<String> next = post(followerIndex, "/command/" + NEW_DATABASE,
          new JSONObject().put("language", "sql").put("command", "CREATE DOCUMENT TYPE AfterCreation"));
      assertThat(next.statusCode()).as("the request right after the creation, to the same follower, was refused: %s", next.body())
          .isEqualTo(200);
    } finally {
      gateHolder.join(TimeUnit.SECONDS.toMillis(30));
    }
  }

  private HttpResponse<String> post(final int serverIndex, final String path, final JSONObject body) throws Exception {
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create("http://127.0.0.1:" + getServer(serverIndex).getHttpServer().getPort() + "/api/v1" + path))
        .timeout(HTTP_TIMEOUT)
        .header("Authorization", "Basic " + Base64.getEncoder()
            .encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(body.toString()))
        .build();
    return HttpClient.newBuilder().connectTimeout(HTTP_TIMEOUT).build().send(request, HttpResponse.BodyHandlers.ofString());
  }
}
