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
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11951: while a targeted leadership transfer is pending, Ratis refuses every write on the leader with
 * {@code LeaderSteppingDownException}, raised before the entry is appended. Those writes answered HTTP 500 "outcome unknown"
 * ({@code ReplicationDispatchedTimeoutException}), on the leader and through a follower that forwarded them, although none
 * of them was applied. They answer the retryable 503 of a {@link NeedRetryException} now, and the check below proves none
 * of them reached any node.
 * <p>
 * The window is made deterministic by transferring to a peer that is down: Ratis keeps the transfer pending until the
 * target wins or the transfer's own timeout elapses, and a stopped target cannot win, so the whole timeout refuses writes.
 */
@Tag("slow")
class Issue11951SteppingDownLeaderRefusesRetryablyIT extends BaseRaftHATest {
  private static final long TRANSFER_TIMEOUT_MS = 8_000L;

  @Override
  protected int getServerCount() {
    return 3;
  }

  /** The stopped transfer target rejoins at the end and must find its own Raft log. */
  @Override
  protected boolean persistentRaftStorage() {
    return true;
  }

  @Test
  void writesRefusedByASteppingDownLeaderAnswer503AndAreNotApplied() throws Exception {
    final int leader = requireLeaderIndex();
    final int follower = firstFollowerOf(leader);
    final int target = 3 - leader - follower;
    getServerDatabase(leader, getDatabaseName()).command("sqlscript",
        "CREATE DOCUMENT TYPE K; CREATE PROPERTY K.k STRING; CREATE INDEX ON K (k) UNIQUE");
    waitForReplicationIsCompleted(follower);
    waitForReplicationIsCompleted(target);

    getServer(target).stop();
    final RaftHAServer raft = getRaftPlugin(leader).getRaftHAServer();
    final AtomicReference<RuntimeException> transferOutcome = new AtomicReference<>();
    final Thread transfer = new Thread(() -> {
      try {
        raft.transferLeadership(peerIdForIndex(target), TRANSFER_TIMEOUT_MS);
      } catch (final RuntimeException e) {
        transferOutcome.set(e);
      }
    }, "issue-11951-transfer");
    transfer.start();

    // Writes on the leader and through the follower, for as long as the transfer runs: before Ratis starts refusing, while
    // it refuses, and after it gave up on the stopped target
    final List<String> acknowledged = new ArrayList<>();
    final List<String> refusedOnTheLeader = new ArrayList<>();
    final List<String> refusedThroughTheFollower = new ArrayList<>();
    final List<String> unexpected = new ArrayList<>();
    for (int n = 0; transfer.isAlive(); n++)
      for (final int node : new int[] { leader, follower }) {
        final String key = "n" + node + "-" + n;
        final HttpResult result = insert(node, key);
        if (result.code() == 200)
          acknowledged.add(key);
        else if (result.code() == 503 && result.body().contains(NeedRetryException.class.getName())
            && result.body().contains("stepping down"))
          (node == leader ? refusedOnTheLeader : refusedThroughTheFollower).add(key);
        else
          unexpected.add(key + " -> HTTP " + result.code() + " " + result.body());
      }
    transfer.join();
    testLog("%d writes acknowledged, %d refused on the leader, %d refused through the follower, %d other answers",
        acknowledged.size(), refusedOnTheLeader.size(), refusedThroughTheFollower.size(), unexpected.size());

    assertThat(transferOutcome.get()).as("a transfer to a stopped peer cannot succeed").isNotNull();
    assertThat(unexpected).as("every write refused during the transfer is a retryable 503, none an 'outcome unknown' 500")
        .isEmpty();
    assertThat(refusedOnTheLeader).as("the leader refused writes while the transfer was pending").isNotEmpty();
    assertThat(refusedThroughTheFollower).as("so did the follower that forwarded writes to it").isNotEmpty();

    // Once Ratis gave up on the stopped target the leader takes writes again, directly and forwarded
    for (final int node : new int[] { leader, follower }) {
      final String key = "after-" + node;
      final HttpResult result = insert(node, key);
      assertThat(result.code()).as("a write on server %d once the transfer gave up: %s", node, result.body()).isEqualTo(200);
      acknowledged.add(key);
    }

    // The stopped target rejoins and catches up: a refused write must be on no node, an acknowledged one on every node
    restartServer(target);
    for (int i = 0; i < getServerCount(); i++) {
      waitForReplicationIsCompleted(i);
      final Database db = getServerDatabase(i, getDatabaseName());
      for (final String key : refusedOnTheLeader)
        assertThat(count(db, key)).as("server %d: refused write %s", i, key).isZero();
      for (final String key : refusedThroughTheFollower)
        assertThat(count(db, key)).as("server %d: refused write %s", i, key).isZero();
      for (final String key : acknowledged)
        assertThat(count(db, key)).as("server %d: acknowledged write %s", i, key).isEqualTo(1L);
    }
  }

  private static long count(final Database db, final String key) {
    try (final ResultSet rs = db.query("sql", "SELECT count(*) AS c FROM K WHERE k = ?", key)) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  private HttpResult insert(final int serverIndex, final String key) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(
        getServerHttpUrl(serverIndex, "/api/v1/command/" + getDatabaseName())).toURL().openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Content-Type", "application/json");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)));
    try {
      formatPayload(connection, new JSONObject().put("language", "sql").put("command", "INSERT INTO K SET k = '" + key + "'"));
      final int code = connection.getResponseCode();
      final String body = code >= 400 ?
          (connection.getErrorStream() != null ? new String(connection.getErrorStream().readAllBytes(), StandardCharsets.UTF_8) : "") :
          new String(connection.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
      return new HttpResult(code, body);
    } finally {
      connection.disconnect();
    }
  }

  private record HttpResult(int code, String body) {
  }
}
