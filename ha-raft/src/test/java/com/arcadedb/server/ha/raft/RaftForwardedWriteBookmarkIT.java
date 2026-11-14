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
 * Issue #11740: a write sent to a follower is forwarded to the leader, and the follower answered it with its own applied index as
 * the {@code X-ArcadeDB-Commit-Index} read-your-writes bookmark. With a majority quorum the leader can commit the write before the
 * follower has applied it, so the bookmark stood below the write and a {@code read_your_writes} read that waited for it, on any
 * node, could miss the write. The follower's apply thread is held off the database here, which makes that window certain.
 */
@Tag("slow")
class RaftForwardedWriteBookmarkIT extends BaseRaftHATest {
  private static final String   TYPE_NAME    = "ForwardedBookmark";
  private static final String   BOOKMARK     = "X-ArcadeDB-Commit-Index";
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
  void aWriteForwardedByAFollowerIsAnsweredWithABookmarkThatCoversIt() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int followerIndex = (leaderIndex + 1) % getServerCount();
    final String dbName = getDatabaseName();

    assertThat(post(leaderIndex, "/command/" + dbName, "CREATE DOCUMENT TYPE " + TYPE_NAME + " IF NOT EXISTS", null).statusCode())
        .isEqualTo(200);
    waitForAllServers();

    // Hold the follower's apply thread off the database: the install apply gate is the lock it takes before applying an entry
    // for it (as in Issue8454SnapshotSourceBehindFollowerIT). The leader still commits with the other follower.
    final RaftHAServer followerRaft = getRaftPlugin(followerIndex).getRaftHAServer();
    final CountDownLatch held = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    final Thread gateHolder = new Thread(() -> {
      final ArcadeStateMachine.InstallApplyGate gate = followerRaft.getStateMachine().installApplyGate(dbName);
      gate.lock();
      try {
        held.countDown();
        release.await(120, TimeUnit.SECONDS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      } finally {
        gate.unlock();
      }
    }, "issue11740-follower-gate");
    gateHolder.start();

    final long bookmark;
    try {
      assertThat(held.await(30, TimeUnit.SECONDS)).isTrue();
      final long leaderAppliedBefore = getRaftPlugin(leaderIndex).getRaftHAServer().getLastAppliedIndex();

      final HttpResponse<String> write = post(followerIndex, "/command/" + dbName, "INSERT INTO " + TYPE_NAME + " SET k = 1", null);
      assertThat(write.statusCode()).as("the forwarded INSERT failed: %s", write.body()).isEqualTo(200);
      assertThat(followerRaft.getLastAppliedIndex()).as("the follower must not have applied the write, or this proves nothing")
          .isLessThanOrEqualTo(leaderAppliedBefore);

      bookmark = Long.parseLong(write.headers().firstValue(BOOKMARK).orElse("-1"));
      assertThat(bookmark).as("the bookmark of a forwarded write must be at or past the write, which comes after index %d",
          leaderAppliedBefore).isGreaterThan(leaderAppliedBefore);
    } finally {
      release.countDown();
      gateHolder.join(TimeUnit.SECONDS.toMillis(30));
    }

    final HttpResponse<String> read = post(followerIndex, "/query/" + dbName, "SELECT FROM " + TYPE_NAME + " WHERE k = 1",
        String.valueOf(bookmark));
    assertThat(read.statusCode()).isEqualTo(200);
    assertThat(new JSONObject(read.body()).getJSONArray("result").length())
        .as("a read_your_writes read with the bookmark must see the write").isEqualTo(1);
  }

  private HttpResponse<String> post(final int serverIndex, final String path, final String sql, final String readAfter)
      throws Exception {
    final HttpRequest.Builder request = HttpRequest.newBuilder()
        .uri(URI.create("http://127.0.0.1:" + getServer(serverIndex).getHttpServer().getPort() + "/api/v1" + path))
        .timeout(HTTP_TIMEOUT)
        .header("Authorization", "Basic " + Base64.getEncoder()
            .encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(new JSONObject().put("language", "sql").put("command", sql).toString()));
    if (readAfter != null)
      request.header("X-ArcadeDB-Read-Consistency", "read_your_writes").header("X-ArcadeDB-Read-After", readAfter);
    return HttpClient.newBuilder().connectTimeout(HTTP_TIMEOUT).build().send(request.build(), HttpResponse.BodyHandlers.ofString());
  }
}
