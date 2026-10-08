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
 * End-to-end regression test for issue #9449 on a real one-voter Raft cluster: a quarantine standing on the only voter
 * keeps {@code /api/v1/ready} at 503 for good, and {@code POST /api/v1/cluster/accept-diverged/{database}} (and its
 * {@link RemoteServer} client) is the way out.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue9449AcceptDivergedDatabaseIT extends BaseRaftHATest {

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
