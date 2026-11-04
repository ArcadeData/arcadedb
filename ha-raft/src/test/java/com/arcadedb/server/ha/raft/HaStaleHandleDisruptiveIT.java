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
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.ServerDatabase;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A restarting HA node serves database requests, and hands out {@link ServerDatabase} handles, before its Raft plugin
 * has wrapped the databases with {@code RaftReplicatedDatabase}: {@code ArcadeDBServer.start()} opens the databases,
 * starts the HTTP server, and only then starts the Raft plugin. Each test restarts a follower and drives that window.
 * Merged from {@code Issue8270FollowerRestartUnderLoadIT}, {@code Issue8282StaleHandleCommitReplicatesIT} and
 * {@code Issue8383StaleHandleCommandForwardsIT}, which ran on the same fixture: three servers, persistent Raft storage,
 * no configuration override and an empty database.
 * <ul>
 * <li><b>#8270</b>: a write reaching the plain {@code LocalDatabase} in the window committed locally, was never sent
 * through Raft, and was answered 200; on the restarted node it then collided with the committed entries the replay
 * applied next. The contract: a write a restarting node acknowledges is on the leader.</li>
 * <li><b>#8282</b>: a handle resolved in the window (the Postgres, MongoDB, Bolt and gRPC executors keep theirs for the
 * connection's lifetime) committed every later transaction through {@code LocalDatabase.commit()}. The contract: that
 * write is on the leader and on every node.</li>
 * <li><b>#8383</b>: the #8282 fix routed only {@code commit()} of such a handle through the current wrapper, so
 * {@code command()} still ran on the plain database and never reached the follower's forward to the leader. A DDL
 * statement makes the difference observable: forwarded, every node gets the new type; executed locally, it is refused.</li>
 * </ul>
 * Why this keeps a fresh cluster per test ({@link BaseRaftHATest}) rather than the shared one of {@link HaStaleHandleIT}:
 * every test stops and restarts a follower, arms {@link RaftHAPlugin#TEST_BEFORE_START_HOOK}, and #8383 may transfer
 * leadership away from the restarted node; #8282 and #8383 also assert absolute counts of their types on every node.
 * <p>
 * Every source class was tagged {@code slow}, so this class is.
 */
@Tag("slow")
class HaStaleHandleDisruptiveIT extends BaseRaftHATest {

  /** #8270. */
  private static final String     TYPE_8270   = "Probe";
  private static final int        WRITERS     = 4;
  private static final long       WINDOW_MS   = 1_500;
  private static final HttpClient HTTP_CLIENT = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();

  /** #8282. */
  private static final String TYPE_8282 = "StaleHandleProbe";

  /** #8383. */
  private static final String TYPE_8383 = "StaleHandleForwardProbe";

  /** #8270. */
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
    // The schema is created in each test, through the leader or through the stale handle.
  }

  // ------------------------------------------------------------------ writes acknowledged in the window (#8270)

  /** Issue #8270, from {@code Issue8270FollowerRestartUnderLoadIT}. */
  @Test
  void aWriteAcknowledgedByARestartingNodeIsReplicated() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);

    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    leaderDb.command("sql", "CREATE VERTEX TYPE " + TYPE_8270);
    leaderDb.command("sql", "CREATE PROPERTY " + TYPE_8270 + ".id LONG");
    leaderDb.command("sql", "CREATE INDEX ON " + TYPE_8270 + " (id) UNIQUE");
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
      startServer(restarted);
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
      if (!leader.query("sql", "SELECT FROM " + TYPE_8270 + " WHERE id = ?", id).hasNext())
        missing.add(id);

    LogManager.instance().log(this, Level.INFO, "TEST: %d writes acknowledged by node %d, %d answered while its Raft plugin was held",
        acknowledged.size(), restarted, answeredDuringStart.get());

    assertThat(missing).as("writes node %d acknowledged that never reached the leader", restarted).isEmpty();

    // Refusing writes is for the window only: once started, the node takes writes again and they replicate.
    final long lastId = nextId.incrementAndGet();
    final HttpResponse<String> after = post(server, lastId);
    assertThat(after.statusCode()).as("a write on node %d after its restart: %s", restarted, after.body()).isEqualTo(200);
    waitForAllServers();
    assertThat(leader.query("sql", "SELECT FROM " + TYPE_8270 + " WHERE id = ?", lastId).hasNext())
        .as("the write node %d took after its restart must reach the leader", restarted).isTrue();

    final long leaderCount = leader.countType(TYPE_8270, true);
    for (int i = 0; i < getServerCount(); i++)
      assertThat(getServerDatabase(i, getDatabaseName()).countType(TYPE_8270, true))
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
    final String body = new JSONObject().put("language", "sql").put("command", "INSERT INTO " + TYPE_8270 + " SET id = " + id)
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

  // ------------------------------------------------------------------ commit on a stale handle (#8282)

  /**
   * Issue #8282, from {@code Issue8282StaleHandleCommitReplicatesIT}.
   * <p>
   * The handle is resolved while the restarting follower's plugin is held before the wrap; the transaction begins and
   * commits after the node is up, which is the case the write refusal of #8270 does not cover.
   */
  @Test
  void aCommitOnAHandleResolvedBeforeTheWrapReachesTheCluster() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);

    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    leaderDb.command("sql", "CREATE DOCUMENT TYPE " + TYPE_8282);
    leaderDb.command("sql", "CREATE PROPERTY " + TYPE_8282 + ".name STRING");
    waitForAllServers();

    final int restarted = leaderIndex == getServerCount() - 1 ? getServerCount() - 2 : getServerCount() - 1;
    final ArcadeDBServer server = getServer(restarted);

    server.stop();
    while (server.getStatus() == ArcadeDBServer.STATUS.SHUTTING_DOWN)
      CodeUtils.sleep(100);

    final AtomicReference<ServerDatabase> staleHandle = new AtomicReference<>();
    RaftHAPlugin.TEST_BEFORE_START_HOOK = starting -> {
      if (starting == server)
        staleHandle.set(starting.getDatabaseForOperator(getDatabaseName()));
    };
    try {
      startServer(restarted);
    } finally {
      RaftHAPlugin.TEST_BEFORE_START_HOOK = null;
    }

    final ServerDatabase handle = staleHandle.get();
    assertThat(handle).as("the handle must have been resolved while node %d was held before the wrap", restarted).isNotNull();
    assertThat(handle).as("the registry must hold a different, wrapped handle by now")
        .isNotSameAs(server.getDatabaseForOperator(getDatabaseName()));
    waitForAllServers();

    // What a Postgres connection opened in the window does next, on its own thread, for its whole lifetime.
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final Thread connection = new Thread(() -> {
      try {
        DatabaseContext.INSTANCE.init(handle);
        handle.begin();
        handle.newDocument(TYPE_8282).set("name", "stale-handle").save();
        handle.commit();
      } catch (final Throwable t) {
        failure.set(t);
      }
    }, "issue8282-connection");
    connection.setDaemon(true);
    connection.start();
    connection.join(30_000);
    assertThat(connection.isAlive()).as("the commit on the stale handle must complete").isFalse();
    assertThat(failure.get()).as("the commit on the stale handle").isNull();

    waitForAllServers();

    final Database leader = getServerDatabase(findLeaderIndex(), getDatabaseName());
    assertThat(leader.query("sql", "SELECT FROM " + TYPE_8282 + " WHERE name = 'stale-handle'").hasNext())
        .as("a write committed on node %d through a handle resolved before the wrap must reach the leader", restarted)
        .isTrue();

    for (int i = 0; i < getServerCount(); i++)
      assertThat(getServerDatabase(i, getDatabaseName()).countType(TYPE_8282, true))
          .as("count of node %d", i).isEqualTo(1L);

    assertClusterConsistency();
  }

  // ------------------------------------------------------------------ command on a stale handle (#8383)

  /** Issue #8383, from {@code Issue8383StaleHandleCommandForwardsIT}. */
  @Test
  void aCommandOnAHandleResolvedBeforeTheWrapIsForwardedToTheLeader() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);

    // A first write through the leader concludes the cluster's first-formation bootstrap, which otherwise refuses a
    // forwarded statement with a NeedRetryException.
    getServerDatabase(leaderIndex, getDatabaseName()).command("sql", "CREATE DOCUMENT TYPE " + TYPE_8383 + "Warmup");
    waitForAllServers();

    final int restarted = leaderIndex == getServerCount() - 1 ? getServerCount() - 2 : getServerCount() - 1;
    final ArcadeDBServer server = getServer(restarted);

    server.stop();
    while (server.getStatus() == ArcadeDBServer.STATUS.SHUTTING_DOWN)
      CodeUtils.sleep(100);

    final AtomicReference<ServerDatabase> staleHandle = new AtomicReference<>();
    RaftHAPlugin.TEST_BEFORE_START_HOOK = starting -> {
      if (starting == server)
        staleHandle.set(starting.getDatabaseForOperator(getDatabaseName()));
    };
    try {
      startServer(restarted);
    } finally {
      RaftHAPlugin.TEST_BEFORE_START_HOOK = null;
    }

    final ServerDatabase handle = staleHandle.get();
    assertThat(handle).as("the handle must have been resolved while node %d was held before the wrap", restarted).isNotNull();
    assertThat(handle).as("the registry must hold a different, wrapped handle by now")
        .isNotSameAs(server.getDatabaseForOperator(getDatabaseName()));
    waitForAllServers();

    // The restarted node can win an election on its way back; hand leadership off, since what is tested is a follower.
    for (int attempt = 0; attempt < 5 && findLeaderIndex() == restarted; attempt++) {
      getRaftPlugin(restarted).getRaftHAServer().transferLeadership(10_000);
      waitForAllServers();
    }
    final int currentLeader = findLeaderIndex();
    assertThat(currentLeader).as("node %d must still be a follower for the forward to be what is tested", restarted)
        .isNotEqualTo(restarted);

    // What a Postgres connection opened in the window does next, on its own thread, for its whole lifetime.
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final Thread connection = new Thread(() -> {
      try {
        DatabaseContext.INSTANCE.init(handle);
        runRetrying(() -> handle.command("sql", "CREATE DOCUMENT TYPE " + TYPE_8383).close());
        runRetrying(() -> handle.command("sql", "INSERT INTO " + TYPE_8383 + " SET name = 'stale-handle'").close());
      } catch (final Throwable t) {
        failure.set(t);
      }
    }, "issue8383-connection");
    connection.setDaemon(true);
    connection.start();
    connection.join(30_000);
    assertThat(connection.isAlive()).as("the commands on the stale handle must complete").isFalse();
    assertThat(failure.get()).as("the commands on the stale handle").isNull();

    waitForAllServers();

    final Database leader = getServerDatabase(findLeaderIndex(), getDatabaseName());
    assertThat(leader.getSchema().existsType(TYPE_8383))
        .as("a DDL statement issued on follower %d through a handle resolved before the wrap must be forwarded to the leader",
            restarted)
        .isTrue();

    for (int i = 0; i < getServerCount(); i++) {
      final Database db = getServerDatabase(i, getDatabaseName());
      assertThat(db.getSchema().existsType(TYPE_8383)).as("type on node %d", i).isTrue();
      assertThat(db.countType(TYPE_8383, true)).as("count of node %d", i).isEqualTo(1L);
    }

    assertClusterConsistency();
  }

  /** #8383: what a client does with a NeedRetryException the restarted node raises while it is still catching up. */
  private static void runRetrying(final Runnable statement) {
    for (int attempt = 1; ; attempt++) {
      try {
        statement.run();
        return;
      } catch (final NeedRetryException e) {
        if (attempt >= 60)
          throw e;
        CodeUtils.sleep(500);
      }
    }
  }
}
