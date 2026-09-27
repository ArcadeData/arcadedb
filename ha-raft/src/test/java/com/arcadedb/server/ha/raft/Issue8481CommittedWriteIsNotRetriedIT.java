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
import com.arcadedb.exception.TransactionCommittedRemotelyException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.log.LogManager;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * Issue #8481, end to end on a three-node cluster with the ALL quorum.
 * <p>
 * With one follower down, a write on the leader is committed by the MAJORITY and then misses the ALL confirmation: the
 * leader completes its local commit and reports {@link MajorityCommittedAllFailedException}. That exception used to be a
 * {@code NeedRetryException}, so {@code database.transaction(block, false, retries)} ran the block again and committed
 * the write once per attempt; the HTTP layer answered it 503, which the Java remote client resends on its own. Each
 * entry point must now commit the write exactly once and report it as committed, not as retryable.
 * <p>
 * The same holds for {@link ReplicationDispatchedTimeoutException}: the entry was dispatched and may still commit, so a
 * retry of the block commits the write a second time whenever it does.
 */
@Tag("slow")
class Issue8481CommittedWriteIsNotRetriedIT extends BaseRaftHATest {

  private static final String TYPE = "Issue8481Vertex";

  private static final HttpClient HTTP = HttpClient.newBuilder()
      .version(HttpClient.Version.HTTP_1_1)
      .connectTimeout(Duration.ofSeconds(10))
      .build();

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM, "all");
    // Bounds how long the ALL confirmation is awaited once a follower is down.
    config.setValue(GlobalConfiguration.HA_QUORUM_TIMEOUT, 3_000L);
  }

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected int[] getServerToCheck() {
    // One test stops a follower on purpose: compare only the servers still running.
    return startedServers();
  }

  @AfterEach
  void clearHooks() {
    RaftGroupCommitter.TEST_FORCE_DISPATCHED_TIMEOUT = null;
  }

  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  void aWriteTheMajorityCommittedIsRunOnceByEveryEntryPoint() throws Exception {
    final int leader = findLeaderIndex();
    assertThat(leader).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final Database leaderDb = getServerDatabase(leader, getDatabaseName());
    createTypeWithBaseline(leaderDb);

    final int stopped = leader == 0 ? 1 : 0;
    LogManager.instance().log(this, Level.INFO, "TEST: stopping follower %d so the ALL quorum cannot be reached", stopped);
    getServer(stopped).stop();

    // 1. The leader-local retry loop the issue names.
    final AtomicInteger attempts = new AtomicInteger();
    assertThatThrownBy(() -> leaderDb.transaction(() -> {
      attempts.incrementAndGet();
      final MutableVertex v = leaderDb.newVertex(TYPE);
      v.set("name", "local");
      v.save();
    }, false, 3)).isInstanceOf(MajorityCommittedAllFailedException.class)
        .isInstanceOf(TransactionCommittedRemotelyException.class);
    assertThat(attempts.get()).as("the block of a write the MAJORITY committed must not run again").isEqualTo(1);
    assertThat(leaderDb.countType(TYPE, true)).as("the write is committed on the leader exactly once").isEqualTo(2L);

    // 2. The HTTP answer: 409 "do not retry", not the 503 every HTTP client and load balancer reads as "retry".
    final HttpResponse<String> response = postCommand(leader, "INSERT INTO " + TYPE + " SET name = 'http'");
    assertThat(response.statusCode()).as("answer: %s", response.body()).isEqualTo(409);
    final JSONObject answer = new JSONObject(response.body());
    assertThat(answer.getString("exception", "")).isEqualTo(MajorityCommittedAllFailedException.class.getName());
    assertThat(answer.getString("error", "")).isEqualTo("Transaction committed cluster-wide - do not retry");
    assertThat(leaderDb.countType(TYPE, true)).isEqualTo(3L);

    // 3. The Java remote client, which resends every NeedRetryException / 503 on its own.
    try (final RemoteDatabase remote = new RemoteDatabase("127.0.0.1", getServer(leader).getHttpServer().getPort(),
        getDatabaseName(), "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {
      assertThatThrownBy(() -> remote.command("sql", "INSERT INTO " + TYPE + " SET name = 'remote'"))
          .isInstanceOf(TransactionCommittedRemotelyException.class);
    }
    assertThat(leaderDb.countType(TYPE, true)).as("the remote client must not resend a committed write").isEqualTo(4L);

    // The MAJORITY did commit every one of them: the follower still up holds exactly the same four.
    final int live = 3 - leader - stopped;
    waitForReplicationIsCompleted(live);
    await().atMost(Duration.ofSeconds(30))
        .untilAsserted(() -> assertThat(getServerDatabase(live, getDatabaseName()).countType(TYPE, true)).isEqualTo(4L));
  }

  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  void aWriteWhoseOutcomeIsUnknownIsNotRunAgainByTheRetryLoop() throws Exception {
    final int leader = findLeaderIndex();
    assertThat(leader).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final Database leaderDb = getServerDatabase(leader, getDatabaseName());
    createTypeWithBaseline(leaderDb);

    // One-shot: the next TX_ENTRY is dispatched to Raft for real (so it commits), but the committing thread abandons the
    // wait with the dispatched-timeout the production grace-expiry path raises (see Issue4790PhantomCommitOriginSkipIT).
    final AtomicBoolean fired = new AtomicBoolean();
    RaftGroupCommitter.TEST_FORCE_DISPATCHED_TIMEOUT = entry -> {
      try {
        final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(ByteString.copyFrom(entry));
        return decoded.type() == RaftLogEntryType.TX_ENTRY && getDatabaseName().equals(decoded.databaseName())
            && fired.compareAndSet(false, true);
      } catch (final Exception ignore) {
        return false;
      }
    };

    final AtomicInteger attempts = new AtomicInteger();
    boolean reportedUnknown = false;
    try {
      leaderDb.transaction(() -> {
        attempts.incrementAndGet();
        final MutableVertex v = leaderDb.newVertex(TYPE);
        v.set("name", "maybe");
        v.save();
      }, false, 3);
    } catch (final ReplicationDispatchedTimeoutException expected) {
      reportedUnknown = true;
    }
    assertThat(fired.get()).as("the dispatched-timeout fault must have fired").isTrue();
    // Since #6965 the apply thread may claim the transaction before the timeout is raised, in which case the commit
    // completes normally; otherwise the outcome is reported unknown. Either way the block must have run once only.
    LogManager.instance().log(this, Level.INFO, "TEST: the commit %s",
        reportedUnknown ? "reported an unknown outcome" : "completed");
    assertThat(attempts.get()).as("the block of a write that may have committed must not run again").isEqualTo(1);

    // The dispatched entry commits: every node converges to the baseline plus that ONE write, not one per attempt.
    for (int i = 0; i < getServerCount(); i++) {
      final int server = i;
      await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
        waitForReplicationIsCompleted(server);
        assertThat(getServerDatabase(server, getDatabaseName()).countType(TYPE, true)).as("server %d", server)
            .isEqualTo(2L);
      });
    }
  }

  private void createTypeWithBaseline(final Database leaderDb) {
    leaderDb.transaction(() -> {
      if (!leaderDb.getSchema().existsType(TYPE))
        leaderDb.getSchema().createVertexType(TYPE, 1);
    });
    leaderDb.transaction(() -> {
      final MutableVertex v = leaderDb.newVertex(TYPE);
      v.set("name", "baseline");
      v.save();
    });
    assertClusterConsistency();
  }

  private HttpResponse<String> postCommand(final int serverIndex, final String command) throws Exception {
    final String body = new JSONObject().put("language", "sql").put("command", command).toString();
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create("http://127.0.0.1:" + getServer(serverIndex).getHttpServer().getPort() + "/api/v1/command/"
            + getDatabaseName()))
        .timeout(Duration.ofSeconds(60))
        .header("Content-Type", "application/json")
        .header("Authorization", "Basic " + Base64.getEncoder().encodeToString(
            ("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)))
        .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
        .build();
    return HTTP.send(request, HttpResponse.BodyHandlers.ofString());
  }
}
