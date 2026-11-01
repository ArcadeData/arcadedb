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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.log.LogManager;
import com.arcadedb.remote.RemoteServer;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The operator overrides that get the only voter of a Raft cluster out of a state no peer can heal, end to end on ONE
 * one-voter cluster shared by every test ({@link SharedClusterRaftHATest}). Merged from
 * {@code Issue9449AcceptDivergedDatabaseIT} and {@code Issue9498AcceptStaleSnapshotIT}, which ran on the same fixture:
 * one server with high availability turned on explicitly (the base class turns it on only for two servers or more) and
 * {@code /api/v1/ready} consulting the HA layer, as a Kubernetes deployment does.
 * <ul>
 * <li><b>#9449</b>: a quarantine standing on the only voter keeps {@code /api/v1/ready} at 503 for good, and
 * {@code POST /api/v1/cluster/accept-diverged/{database}} (and its {@link RemoteServer} client) is the way out.</li>
 * <li><b>#9498</b>: the node-wide stale-snapshot read floor (issue #6111) keeps {@code /api/v1/ready} at 503 on the only
 * voter for good, since a leader cannot resync from itself, and {@code POST /api/v1/cluster/accept-stale-snapshot} (and
 * its {@link RemoteServer} client) is the way out.</li>
 * </ul>
 * Why these share a cluster: each test raises the state it lifts (a quarantine of the test database, or the node-wide
 * floor) and lifts it through the override under test, so a passing test leaves the node as it found it; accepting on a
 * healthy node records the applied position it already has. A test that fails before its override leaves a quarantine
 * behind, which the shared base's health check sees and reboots; the floor is a state that check does not look at, so
 * each #9498 test drops a floor still standing in a {@code finally} ({@link #dropStaleSnapshotFloorLeftStanding()}),
 * which also keeps the "no floor stands yet" precondition of the first #9498 test independent of the order. No test
 * stops, restarts or isolates the node or changes a setting.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class SoleVoterOperatorOverrideIT extends SharedClusterRaftHATest {

  @Override
  protected int getServerCount() {
    return 1;
  }

  /** The base class turns HA on only for two servers or more; this cluster is one voter on purpose. */
  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_SERVER_LIST, getServerAddresses());
    config.setValue(GlobalConfiguration.HA_ENABLED, true);
    // /api/v1/ready consults the HA layer only when asked to, as a Kubernetes deployment does
    config.setValue(GlobalConfiguration.SERVER_READINESS_REQUIRES_HA, true);
  }

  // ------------------------------------------------------------------ accept a diverged database (#9449)

  /** Issue #9449, from {@code Issue9449AcceptDivergedDatabaseIT}. */
  @Test
  @Timeout(120)
  void aSoleVoterLiftsAStandingQuarantineOverHttpAndBecomesReadyAgain() throws Exception {
    final RaftHAServer raft = getRaftPlugin(0).getRaftHAServer();
    assertThat(raft.isSoleVoter()).as("the fixture is a one-voter cluster").isTrue();
    final ArcadeStateMachine sm = raft.getStateMachine();

    // The quarantine as #9308 leaves it standing: raised before the cluster shrank, or restored from disk
    sm.markStateDiverged(getDatabaseName(), DivergenceCause.APPLY_ERROR);
    assertThat(status("GET", "/api/v1/ready", "root")).as("a quarantined node is not ready").isEqualTo(503);

    final HttpURLConnection accepted = open("POST", PostAcceptDivergedHandler.ROUTE + getDatabaseName(), "root");
    assertThat(accepted.getResponseCode()).isEqualTo(200);
    final JSONObject body = new JSONObject(read(accepted.getInputStream()));
    assertThat(body.getString("database")).isEqualTo(getDatabaseName());
    assertThat(body.getString("divergenceCause")).isEqualTo(DivergenceCause.APPLY_ERROR.name());
    assertThat(body.getString("localServer")).isEqualTo(getServer(0).getServerName());

    assertThat(sm.isDatabaseDiverged(getDatabaseName())).isFalse();
    assertThat(status("GET", "/api/v1/ready", "root")).as("the node is ready again").isBetween(200, 299);

    assertThat(status("POST", PostAcceptDivergedHandler.ROUTE + getDatabaseName(), "root"))
        .as("nothing is left to accept").isEqualTo(404);
    assertThat(status("POST", PostAcceptDivergedHandler.ROUTE + "bad%20name", "root")).isEqualTo(400);
  }

  /** Issue #9449, from {@code Issue9449AcceptDivergedDatabaseIT}. */
  @Test
  @Timeout(120)
  void theRemoteServerClientReachesTheSameOverride() {
    final ArcadeStateMachine sm = getRaftPlugin(0).getRaftHAServer().getStateMachine();
    sm.markStateDiverged(getDatabaseName(), DivergenceCause.WAL_VERSION_GAP);

    final RemoteServer remote = new RemoteServer("127.0.0.1", getServerHttpPort(0), "root", DEFAULT_PASSWORD_FOR_TESTS);
    final JSONObject result = remote.acceptDivergedDatabase(getDatabaseName());

    assertThat(result.getString("divergenceCause")).isEqualTo(DivergenceCause.WAL_VERSION_GAP.name());
    assertThat(sm.isDatabaseDiverged(getDatabaseName())).isFalse();
    assertThat(sm.isResyncInProgress()).isFalse();

    assertThatThrownBy(() -> remote.acceptDivergedDatabase(getDatabaseName()))
        .as("a second call has nothing left to accept")
        .hasMessageContaining("not quarantined");
  }

  // ------------------------------------------------------------------ accept a stale snapshot (#9498)

  /** Issue #9498, from {@code Issue9498AcceptStaleSnapshotIT}. */
  @Test
  @Timeout(120)
  void aSoleVoterLiftsTheFloorOverHttpAndBecomesReadyAgain() throws Exception {
    try {
      final RaftHAServer raft = getRaftPlugin(0).getRaftHAServer();
      assertThat(raft.isSoleVoter()).as("the fixture is a one-voter cluster").isTrue();
      assertThat(status("POST", PostAcceptStaleSnapshotHandler.ROUTE, "root"))
          .as("no floor stands yet: nothing to accept").isEqualTo(404);

      raiseStaleSnapshotFloor(0, 0);
      assertThat(status("GET", "/api/v1/ready", "root")).as("a node holding the floor is not ready").isEqualTo(503);

      final HttpURLConnection accepted = open("POST", PostAcceptStaleSnapshotHandler.ROUTE, "root");
      assertThat(accepted.getResponseCode()).isEqualTo(200);
      final JSONObject body = new JSONObject(read(accepted.getInputStream()));
      assertThat(body.getString("localServer")).isEqualTo(getServer(0).getServerName());
      assertThat(body.getLong("readFloor")).isEqualTo(0L);
      assertThat(body.getString("result")).contains("not replayed");

      assertThat(getStaleSnapshotFloor(0)).isNegative();
      assertThat(status("GET", "/api/v1/ready", "root")).as("the node is ready again").isBetween(200, 299);

      assertThat(status("POST", PostAcceptStaleSnapshotHandler.ROUTE, "root"))
          .as("nothing is left to accept").isEqualTo(404);
    } finally {
      dropStaleSnapshotFloorLeftStanding();
    }
  }

  /** Issue #9498, from {@code Issue9498AcceptStaleSnapshotIT}. */
  @Test
  @Timeout(120)
  void theRemoteServerClientReachesTheSameStaleSnapshotOverride() {
    try {
      raiseStaleSnapshotFloor(0, 0);

      final RemoteServer remote = new RemoteServer("127.0.0.1", getServerHttpPort(0), "root", DEFAULT_PASSWORD_FOR_TESTS);
      final JSONObject result = remote.acceptStaleSnapshot();

      assertThat(result.getLong("readFloor")).isEqualTo(0L);
      assertThat(getStaleSnapshotFloor(0)).isNegative();
      assertThat(getRaftPlugin(0).getRaftHAServer().getStateMachine().isResyncInProgress()).isFalse();

      assertThatThrownBy(remote::acceptStaleSnapshot)
          .as("a second call has nothing left to accept")
          .hasMessageContaining("no stale-snapshot read floor");
    } finally {
      dropStaleSnapshotFloorLeftStanding();
    }
  }

  /**
   * #9498: drops a stale-snapshot floor a failed test left standing, so the next test of the shared cluster finds none.
   * The shared base's health check does not look at the floor. A failing drop is logged, never thrown, so it cannot mask
   * the failure of the test itself.
   */
  private void dropStaleSnapshotFloorLeftStanding() {
    try {
      if (getStaleSnapshotFloor(0) >= 0) {
        LogManager.instance().log(this, Level.WARNING, "TEST: dropping the stale-snapshot floor a test left standing");
        getRaftPlugin(0).getRaftHAServer().getStateMachine().clearStaleSnapshotFloor();
      }
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING, "TEST: could not drop the stale-snapshot floor", e);
    }
  }

  private int status(final String method, final String path, final String user) throws IOException {
    final HttpURLConnection connection = open(method, path, user);
    try {
      return connection.getResponseCode();
    } finally {
      connection.disconnect();
    }
  }

  private HttpURLConnection open(final String method, final String path, final String user) throws IOException {
    final HttpURLConnection connection = (HttpURLConnection) URI.create(getServerHttpUrl(0, path)).toURL().openConnection();
    connection.setRequestMethod(method);
    connection.setRequestProperty("Authorization", "Basic " + Base64.getEncoder()
        .encodeToString((user + ":" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)));
    if ("POST".equals(method)) {
      connection.setDoOutput(true);
      connection.getOutputStream().write("{}".getBytes(StandardCharsets.UTF_8));
    }
    return connection;
  }

  private static String read(final InputStream in) throws IOException {
    try (in) {
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }
  }
}
