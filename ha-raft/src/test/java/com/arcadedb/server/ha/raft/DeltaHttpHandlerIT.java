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

import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * IT for the WAL delta resync endpoint ({@code GET /api/v1/ha/delta/{database}}, issue #4147 phase
 * 6). Confirms the wire shape and error semantics that {@link BootstrapDeltaInstaller} relies on
 * for the "try delta, fall back to full" decision.
 * <p>
 * In phase 6a the endpoint always returns 412 (no WAL retention yet); these tests pin that
 * behaviour AND the response-body prefix the installer parses, so phase 6b can switch to real
 * delta serving without breaking the contract clients depend on.
 */
class DeltaHttpHandlerIT extends BaseRaftHATest {

  @Override
  protected int getServerCount() {
    return 2;
  }

  @Test
  void deltaReturns412WithMachineReadableNoDeltaBody() throws Exception {
    final int httpPort = 2480;
    final URL url = new URL("http://localhost:" + httpPort + "/api/v1/ha/delta/" + getDatabaseName() + "?fromTxId=0");
    final HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("GET");
    conn.setRequestProperty("X-ArcadeDB-Cluster-Token", clusterTokenForServer(0));

    try {
      assertThat(conn.getResponseCode()).isEqualTo(412);
      // Body MUST start with "no-delta:" so BootstrapDeltaInstaller can log a structured reason
      // and fall back without parsing English. Phase 6b's real delta serving will keep this
      // prefix for the same gap-too-big / no-WAL-retained sub-cases.
      final String body = new String(conn.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
      assertThat(body).startsWith("no-delta:");
    } finally {
      conn.disconnect();
    }
  }

  @Test
  void deltaRejectsMissingClusterToken() throws Exception {
    final int httpPort = 2480;
    final URL url = new URL("http://localhost:" + httpPort + "/api/v1/ha/delta/" + getDatabaseName() + "?fromTxId=0");
    final HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("GET");
    // No X-ArcadeDB-Cluster-Token header.

    try {
      assertThat(conn.getResponseCode()).isEqualTo(401);
    } finally {
      conn.disconnect();
    }
  }

  @Test
  void deltaReturns404ForUnknownDatabase() throws Exception {
    final int httpPort = 2480;
    final URL url = new URL("http://localhost:" + httpPort + "/api/v1/ha/delta/no-such-db?fromTxId=0");
    final HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("GET");
    conn.setRequestProperty("X-ArcadeDB-Cluster-Token", clusterTokenForServer(0));

    try {
      assertThat(conn.getResponseCode()).isEqualTo(404);
    } finally {
      conn.disconnect();
    }
  }

  @Test
  void deltaRejectsMissingFromTxIdParameter() throws Exception {
    final int httpPort = 2480;
    final URL url = new URL("http://localhost:" + httpPort + "/api/v1/ha/delta/" + getDatabaseName());
    final HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("GET");
    conn.setRequestProperty("X-ArcadeDB-Cluster-Token", clusterTokenForServer(0));

    try {
      assertThat(conn.getResponseCode()).isEqualTo(400);
    } finally {
      conn.disconnect();
    }
  }

  private String clusterTokenForServer(final int serverIndex) {
    final RaftHAPlugin plugin = getRaftPlugin(serverIndex);
    final String token = plugin.getRaftHAServer().getClusterToken();
    assertThat(token).as("cluster token must be present in HA test fixture").isNotBlank();
    return token;
  }
}
