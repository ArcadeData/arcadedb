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
package com.arcadedb.redis;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ha.raft.BaseRaftHATest;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8278, the cluster-level counterpart of {@code RedisQueryLanguageTest.batchIsAnalyzedByEveryCommandNotByItsFirst}:
 * a redis newline batch that opens with a read and writes after it, sent to a FOLLOWER's {@code /api/v1/command}, must
 * be forwarded to the leader and replicated from there.
 * <p>
 * {@code RaftReplicatedDatabase.command(...)} runs a command locally on a follower whenever
 * {@code analyze(query).isIdempotent()} is true. Before #8247 the redis engine classified a batch by its first verb, so
 * {@code GET k\nHSET ...} was declared read-only and ran on the follower instead of the leader. The unit test pins the
 * analysis; this one pins the routing decision that consumes it, through both {@code command(...)} overloads the HTTP
 * handler can pick: named parameters (the default, no {@code params}) and positional ones ({@code params} as an array).
 * <p>
 * Each batch carries a {@code SET} as the witness of the node that executed it (see {@link #assertBatchRanOnTheLeader}),
 * and its record write is then checked on every node.
 */
@Tag("slow")
class Issue8278RedisBatchFollowerForwardingIT extends BaseRaftHATest {

  private static final String TYPE = "Person8278";

  @Override
  protected int getServerCount() {
    return 3;
  }

  /**
   * Map overload: the batch carries no {@code params}, so the handler calls {@code command(language, query, config, Map)}.
   */
  @Test
  void batchWithWriteBehindLeadingReadSentToFollowerIsAppliedByTheLeader() throws Exception {
    final int leader = leaderIndex();
    final int follower = followerOf(leader);
    createType(leader);

    final JSONObject response = postCommand(follower,
        "GET k8278a\nSET k8278a leader-ran-it\nHSET " + TYPE + " {\"name\":\"alice\"}", null);
    assertThat(response.has("result")).as("the follower must answer the batch: %s", response).isTrue();

    waitForAllServers();
    assertBatchRanOnTheLeader("k8278a", leader, follower);
    assertEveryServerCounts(1, leader, follower);
  }

  /**
   * Object... overload: positional {@code params} make the handler call {@code command(language, query, config, Object...)}.
   * The delete path, the issue's own example ({@code GET k\nSET k v\nHDEL ...}).
   */
  @Test
  void batchWithDeleteBehindLeadingReadAndPositionalParamsSentToFollowerIsAppliedByTheLeader() throws Exception {
    final int leader = leaderIndex();
    final int follower = followerOf(leader);
    createType(leader);

    final Database leaderDb = getServerDatabase(leader, getDatabaseName());
    leaderDb.transaction(() -> leaderDb.newDocument(TYPE).set("name", "bob").save());
    waitForAllServers();
    assertEveryServerCounts(1, leader, follower);

    // The redis engine ignores parameters: this value is never read, it only makes the handler pick the Object... overload.
    final JSONArray positional = new JSONArray().put("unused");
    final JSONObject response = postCommand(follower, "GET k8278b\nSET k8278b leader-ran-it\nHDEL " + TYPE + "[name] bob",
        positional);
    assertThat(response.has("result")).as("the follower must answer the batch: %s", response).isTrue();

    waitForAllServers();
    assertBatchRanOnTheLeader("k8278b", leader, follower);
    assertEveryServerCounts(0, leader, follower);
  }

  private int leaderIndex() {
    final int leader = findLeaderIndex();
    assertThat(leader).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    return leader;
  }

  private int followerOf(final int leader) {
    for (int i = 0; i < getServerCount(); i++)
      if (i != leader)
        return i;
    throw new IllegalStateException("no follower in a " + getServerCount() + "-node cluster");
  }

  /**
   * Created on the leader so schema replication is not mixed with the routing under test; the unique index is what
   * {@code HDEL Type[name] key} looks the record up by.
   */
  private void createType(final int leader) {
    final Database leaderDb = getServerDatabase(leader, getDatabaseName());
    leaderDb.command("sql", "CREATE DOCUMENT TYPE " + TYPE + " IF NOT EXISTS");
    leaderDb.command("sql", "CREATE PROPERTY " + TYPE + ".name IF NOT EXISTS STRING");
    leaderDb.command("sql", "CREATE INDEX IF NOT EXISTS ON " + TYPE + " (name) UNIQUE");
    waitForAllServers();
  }

  /**
   * Which node EXECUTED the batch. Record counts cannot say: a write a follower runs locally is still proposed through
   * Raft at commit and applied on every node, so a batch wrongly kept on the follower converges to the same counts. The
   * batch's {@code SET} can: it lands in the executing node's global variables, which are not replicated (issue #6560),
   * so it is visible on the leader only if the leader ran the batch, and on the follower only if the follower did.
   */
  private void assertBatchRanOnTheLeader(final String key, final int leader, final int follower) {
    assertThat(((DatabaseInternal) getServerDatabase(leader, getDatabaseName())).getGlobalVariable(key))
        .as("the batch sent to follower %d must have been executed by leader %d, which then holds its SET %s", follower, leader,
            key)
        .isEqualTo("leader-ran-it");
    assertThat(((DatabaseInternal) getServerDatabase(follower, getDatabaseName())).getGlobalVariable(key))
        .as("follower %d must not have executed the batch locally, so it must not hold its SET %s (this relies on global "
            + "variables NOT being replicated, issue #6560: if they now are, this witness no longer tells the nodes apart)",
            follower, key)
        .isNull();
  }

  private void assertEveryServerCounts(final long expected, final int leader, final int follower) {
    for (int i = 0; i < getServerCount(); i++)
      assertThat(getServerDatabase(i, getDatabaseName()).countType(TYPE, false))
          .as("server %d (leader=%d, batch sent to follower=%d) must count %d %s", i, leader, follower, expected, TYPE)
          .isEqualTo(expected);
  }

  private JSONObject postCommand(final int serverIndex, final String command, final JSONArray params) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) URI.create(
        "http://127.0.0.1:" + getServerHttpPort(serverIndex) + "/api/v1/command/" + getDatabaseName()).toURL().openConnection();
    try {
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)));
      connection.setRequestProperty("Content-Type", "application/json");
      connection.setDoOutput(true);

      final JSONObject request = new JSONObject().put("language", "redis").put("command", command);
      if (params != null)
        request.put("params", params);
      try (final OutputStream os = connection.getOutputStream()) {
        os.write(request.toString().getBytes(StandardCharsets.UTF_8));
      }

      final int code = connection.getResponseCode();
      if (code != 200) {
        final InputStream error = connection.getErrorStream();
        throw new AssertionError("HTTP " + code + " from server " + serverIndex + ": "
            + (error != null ? new String(error.readAllBytes(), StandardCharsets.UTF_8) : "<no response body>"));
      }
      return new JSONObject(new String(connection.getInputStream().readAllBytes(), StandardCharsets.UTF_8));
    } finally {
      connection.disconnect();
    }
  }
}
