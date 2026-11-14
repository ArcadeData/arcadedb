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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11742: the read-your-writes bookmark was the leader's Ratis applied index, which also counts the metadata entry Ratis
 * appends after a write to record its commit. A follower learns that this metadata entry is committed only with the leader's
 * next append, so on a quiet cluster a read waiting for such a bookmark waited for the next heartbeat rather than for the
 * data. The test waits until the leader has applied that metadata entry, takes the bookmark a read on the leader answers with
 * then, and reads the write on a follower with it. The heartbeat is set to about 5 s here (half the minimum election
 * timeout), so a read that waits for it cannot pass for one that waits for the write. The Raft log is the persistent one a
 * deployment runs.
 */
@Tag("slow")
class RaftReadYourWritesQuietClusterIT extends BaseRaftHATest {
  private static final String   TYPE_NAME    = "QuietRyw";
  private static final Duration HTTP_TIMEOUT = Duration.ofSeconds(60);

  @Override
  protected boolean persistentRaftStorage() {
    return true;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM, "majority");
    config.setValue(GlobalConfiguration.HA_ELECTION_TIMEOUT_MIN, 10_000);
    config.setValue(GlobalConfiguration.HA_ELECTION_TIMEOUT_MAX, 12_000);
  }

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  void aBookmarkLeavesOutTheMetadataEntryRatisAppendsAfterAWrite() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int followerIndex = (leaderIndex + 1) % getServerCount();
    final String dbName = getDatabaseName();
    final RaftHAServer leaderRaft = getRaftPlugin(leaderIndex).getRaftHAServer();

    assertThat(post(leaderIndex, "/command/" + dbName, "CREATE DOCUMENT TYPE " + TYPE_NAME + " IF NOT EXISTS", null).statusCode())
        .isEqualTo(200);
    waitForAllServers();

    assertThat(post(leaderIndex, "/command/" + dbName, "INSERT INTO " + TYPE_NAME + " SET k = 1", null).statusCode()).isEqualTo(200);
    final long deadline = System.currentTimeMillis() + 10_000;
    while (leaderRaft.getLastAppliedIndex() <= leaderRaft.getStateMachine().readAppliedIndexCounter()
        && System.currentTimeMillis() < deadline)
      Thread.sleep(20);
    assertThat(leaderRaft.getLastAppliedIndex()).as("the leader must have applied the metadata entry after the write")
        .isGreaterThan(leaderRaft.getStateMachine().readAppliedIndexCounter());

    final HttpResponse<String> leaderRead = post(leaderIndex, "/query/" + dbName, "SELECT FROM " + TYPE_NAME, null);
    final String bookmark = leaderRead.headers().firstValue("X-ArcadeDB-Commit-Index").orElseThrow();

    final long start = System.nanoTime();
    final HttpResponse<String> read = post(followerIndex, "/query/" + dbName, "SELECT FROM " + TYPE_NAME + " WHERE k = 1", bookmark);
    final long elapsedMs = (System.nanoTime() - start) / 1_000_000;

    assertThat(read.statusCode()).isEqualTo(200);
    assertThat(new JSONObject(read.body()).getJSONArray("result").length()).as("the read must see the write").isEqualTo(1);
    assertThat(elapsedMs).as("a read_your_writes read waited %d ms for bookmark %s", elapsedMs, bookmark).isLessThan(2_000L);
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
