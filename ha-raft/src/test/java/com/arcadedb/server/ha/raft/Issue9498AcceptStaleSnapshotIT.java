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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end regression test for issue #9498 on a real one-voter Raft cluster: the node-wide stale-snapshot read floor
 * (issue #6111) keeps {@code /api/v1/ready} at 503 on the only voter for good, since a leader cannot resync from itself,
 * and {@code POST /api/v1/cluster/accept-stale-snapshot} (and its {@link RemoteServer} client) is the way out.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue9498AcceptStaleSnapshotIT extends BaseRaftHATest {

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

  @Test
  @Timeout(120)
  void aSoleVoterLiftsTheFloorOverHttpAndBecomesReadyAgain() throws Exception {
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
  }

  @Test
  @Timeout(120)
  void theRemoteServerClientReachesTheSameOverride() {
    raiseStaleSnapshotFloor(0, 0);

    final RemoteServer remote = new RemoteServer("127.0.0.1", getServerHttpPort(0), "root", DEFAULT_PASSWORD_FOR_TESTS);
    final JSONObject result = remote.acceptStaleSnapshot();

    assertThat(result.getLong("readFloor")).isEqualTo(0L);
    assertThat(getStaleSnapshotFloor(0)).isNegative();
    assertThat(getRaftPlugin(0).getRaftHAServer().getStateMachine().isResyncInProgress()).isFalse();

    assertThatThrownBy(remote::acceptStaleSnapshot)
        .as("a second call has nothing left to accept")
        .hasMessageContaining("no stale-snapshot read floor");
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
