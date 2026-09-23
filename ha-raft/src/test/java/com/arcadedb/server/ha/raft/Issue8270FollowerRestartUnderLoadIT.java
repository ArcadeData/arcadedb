/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.utility.CodeUtils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #8270: a restarting HA node served database requests before its databases were wrapped
 * for replication.
 * <p>
 * {@code ArcadeDBServer.start()} opens the databases, starts the HTTP server, and only then starts the Raft plugin,
 * which wraps every database with {@code RaftReplicatedDatabase}. Between the HTTP server accepting requests and that
 * wrap, a write reached the plain {@code LocalDatabase}: it committed locally, was never sent through Raft, and was
 * answered 200. On the rest of the cluster the write does not exist. On the restarted node it collides with the
 * committed entries the Raft replay applies next - same page, same version, different bytes - and the equal-version
 * re-apply splices the two. That fits both shapes the chaos harness reported: extra rows and an unreadable unique
 * index on the restarted node alone, and acknowledged writes missing on every node when the splice erased them.
 * <p>
 * The contract asserted: a write a restarting node acknowledges is on the leader. Writes the node refuses while it
 * cannot replicate them are fine - the client is told to retry.
 */
@Tag("slow")
class Issue8270FollowerRestartUnderLoadIT extends BaseRaftHATest {

  private static final String     TYPE_NAME   = "Probe";
  private static final int        WRITERS     = 4;
  private static final long       WINDOW_MS   = 1_500;
  private static final HttpClient HTTP_CLIENT = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();

  private final AtomicLong    nextId              = new AtomicLong();
  private final AtomicBoolean running             = new AtomicBoolean(true);
  private final Set<Long>     acknowledged        = ConcurrentHashMap.newKeySet();
  private final AtomicInteger answeredDuringStart = new AtomicInteger();
  // True only while the restarting node's Raft plugin is held before wrapping its databases: the window under test.
  private volatile boolean    pluginHeld;

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected boolean persistentRaftStorage() {
    return true;
  }

  @Override
  protected void populateDatabase() {
    // The schema is created in the test, through the leader.
  }

  @Test
  void aWriteAcknowledgedByARestartingNodeIsReplicated() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);

    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    leaderDb.command("sql", "CREATE VERTEX TYPE " + TYPE_NAME);
    leaderDb.command("sql", "CREATE PROPERTY " + TYPE_NAME + ".id LONG");
    leaderDb.command("sql", "CREATE INDEX ON " + TYPE_NAME + " (id) UNIQUE");
    waitForAllServers();

    final int restarted = leaderIndex == getServerCount() - 1 ? getServerCount() - 2 : getServerCount() - 1;
    final ArcadeDBServer server = getServer(restarted);

    LogManager.instance().log(this, Level.INFO, "TEST: stopping node %d", restarted);
    server.stop();
    while (server.getStatus() == ArcadeDBServer.STATUS.SHUTTING_DOWN)
      CodeUtils.sleep(100);

    // Writers aimed at the node being restarted, hammering it from the moment its HTTP server binds a port: the
    // window under test opens there and closes when the Raft plugin has wrapped the databases.
    final List<Thread> writers = new ArrayList<>();
    for (int w = 0; w < WRITERS; w++) {
      final Thread t = new Thread(() -> writeLoop(server), "issue8270-writer-" + w);
      t.setDaemon(true);
      t.start();
      writers.add(t);
    }

    // In process the window lasts a few milliseconds (half a second in a container, where the chaos harness found
    // it), so the Raft plugin of the restarting node is held at its very first line, with the HTTP server already up.
    RaftHAPlugin.TEST_BEFORE_START_HOOK = starting -> {
      if (starting == server) {
        pluginHeld = true;
        try {
          CodeUtils.sleep(WINDOW_MS);
        } finally {
          pluginHeld = false;
        }
      }
    };
    try {
      LogManager.instance().log(this, Level.INFO, "TEST: restarting node %d under write load", restarted);
      server.start();
      CodeUtils.sleep(2_000);
    } finally {
      RaftHAPlugin.TEST_BEFORE_START_HOOK = null;
      running.set(false);
      for (final Thread t : writers)
        t.join(30_000);
    }

    assertThat(answeredDuringStart.get())
        .as("the writers must have had requests answered by node %d while its Raft plugin was held before wrapping the "
            + "databases, or the test proves nothing", restarted)
        .isGreaterThan(0);

    waitForAllServers();

    final Database leader = getServerDatabase(findLeaderIndex(), getDatabaseName());
    final List<Long> missing = new ArrayList<>();
    for (final long id : acknowledged)
      if (!leader.query("sql", "SELECT FROM " + TYPE_NAME + " WHERE id = ?", id).hasNext())
        missing.add(id);

    LogManager.instance().log(this, Level.INFO, "TEST: %d writes acknowledged by node %d, %d answered while its Raft plugin was held",
        acknowledged.size(), restarted, answeredDuringStart.get());

    assertThat(missing).as("writes node %d acknowledged that never reached the leader", restarted).isEmpty();

    // Refusing writes is for the window only: once started, the node takes writes again and they replicate.
    final long lastId = nextId.incrementAndGet();
    final HttpResponse<String> after = post(server, lastId);
    assertThat(after.statusCode()).as("a write on node %d after its restart: %s", restarted, after.body()).isEqualTo(200);
    waitForAllServers();
    assertThat(leader.query("sql", "SELECT FROM " + TYPE_NAME + " WHERE id = ?", lastId).hasNext())
        .as("the write node %d took after its restart must reach the leader", restarted).isTrue();

    final long leaderCount = leader.countType(TYPE_NAME, true);
    for (int i = 0; i < getServerCount(); i++)
      assertThat(getServerDatabase(i, getDatabaseName()).countType(TYPE_NAME, true))
          .as("count of node %d must equal the leader's", i).isEqualTo(leaderCount);

    assertClusterConsistency();
  }

  private void writeLoop(final ArcadeDBServer server) {
    while (running.get()) {
      final HttpServer http = server.getHttpServer();
      if (http == null || http.getPort() <= 0 || server.getStatus() == ArcadeDBServer.STATUS.OFFLINE) {
        Thread.onSpinWait();
        continue;
      }
      final long id = nextId.incrementAndGet();
      // Sent AND answered inside the held window: a request that merely started there could be answered after the
      // wrap, and would not exercise the unwrapped database at all.
      final boolean sentWhileHeld = pluginHeld;
      try {
        final HttpResponse<String> response = post(server, id);
        if (sentWhileHeld && pluginHeld)
          answeredDuringStart.incrementAndGet();
        if (response.statusCode() == 200)
          acknowledged.add(id);
      } catch (final Exception e) {
        // Not acknowledged: nothing to check for this id.
      }
    }
  }

  private HttpResponse<String> post(final ArcadeDBServer server, final long id) throws Exception {
    final String auth = "Basic " + Base64.getEncoder()
        .encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8));
    final String body = new JSONObject().put("language", "sql").put("command", "INSERT INTO " + TYPE_NAME + " SET id = " + id)
        .toString();
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(getServerHttpUrl(server, "/api/v1/command/" + getDatabaseName())))
        .timeout(Duration.ofSeconds(30))
        .header("Authorization", auth)
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
        .build();
    return HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
  }
}
