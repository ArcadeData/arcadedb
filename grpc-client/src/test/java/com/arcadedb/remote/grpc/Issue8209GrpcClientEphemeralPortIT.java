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
package com.arcadedb.remote.grpc;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8209: the grpc-client tests started the gRPC plugin on the fixed production port 50051 and connected their
 * clients to it, so anything already listening there (a developer's own ArcadeDB, a concurrent build, another agent)
 * failed the whole module or, worse, answered the tests' connections. This test occupies 50051 itself before the
 * server starts, and proves the server still comes up on an operating-system assigned port that a client can use.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8209GrpcClientEphemeralPortIT extends BaseGrpcClientServerTest {
  private static final int PRODUCTION_DEFAULT_PORT = 50051;

  private ServerSocket squatter;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GRPC:com.arcadedb.server.grpc.GrpcServerPlugin");
    try {
      squatter = new ServerSocket(PRODUCTION_DEFAULT_PORT);
    } catch (final IOException e) {
      // Something else already holds the port: the condition this test reproduces is in place anyway.
      squatter = null;
    }
  }

  @AfterEach
  @Override
  public void endTest() {
    try {
      GlobalConfiguration.SERVER_PLUGINS.setValue("");
      super.endTest();
    } finally {
      if (squatter != null) {
        try {
          squatter.close();
        } catch (final IOException ignore) {
          // Nothing left to release.
        }
        squatter = null;
      }
    }
  }

  @Test
  void serverStartsAndAnswersOnAnAssignedPortWhileTheProductionDefaultIsTaken() {
    assertThat(getServer(0).isStarted()).isTrue();

    final int port = getServerGrpcPort();
    assertThat(port).isGreaterThan(0);
    assertThat(port).isNotEqualTo(PRODUCTION_DEFAULT_PORT);

    final RemoteGrpcServer grpcServer = new RemoteGrpcServer("localhost", port, "root", DEFAULT_PASSWORD_FOR_TESTS, true, List.of());
    try (final RemoteGrpcDatabase database = new RemoteGrpcDatabase(grpcServer, "localhost", port, getServerHttpPort(), getDatabaseName(),
        "root", DEFAULT_PASSWORD_FOR_TESTS)) {
      try (final ResultSet rs = database.query("sql", "SELECT count(*) AS total FROM " + VERTEX1_TYPE_NAME)) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(rs.next().<Long>getProperty("total")).isGreaterThan(0L);
      }
    } finally {
      grpcServer.close();
    }
  }
}
