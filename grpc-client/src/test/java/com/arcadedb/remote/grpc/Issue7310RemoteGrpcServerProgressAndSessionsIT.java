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
import com.arcadedb.engine.OperationProgress;
import com.arcadedb.engine.OperationProgressRegistry;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.grpc.OperationProgressInfo;
import com.arcadedb.server.grpc.SessionInfo;
import com.arcadedb.server.http.HttpAuthSession;
import com.arcadedb.server.security.ServerSecurityUser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7310, client side: an RPC no client can call is one nobody can use, so the two new admin RPCs
 * are driven here through {@link RemoteGrpcServer} against a live server - proto, service and client
 * proved to agree rather than only to compile.
 * <p>
 * {@link #progressIsPolledOverGrpcAndNotOverTheInheritedHttpRoute()} is the one that matters most.
 * {@link RemoteGrpcDatabase} extends {@code RemoteDatabase}, so before this change its inherited
 * {@code getProgress()} issued {@code GET /api/v1/progress/{database}} over the HTTP port - the very
 * thing a gRPC-only deployment does not open. That test hands the database a dead HTTP port, so it can
 * only pass if the poll really travels over gRPC.
 */
class Issue7310RemoteGrpcServerProgressAndSessionsIT extends BaseGraphServerTest {

  private static final int GRPC_PORT = 50051;
  /** A port deliberately left closed, so any fallback to the inherited HTTP route fails loudly. */
  private static final int DEAD_HTTP_PORT = 59998;

  private RemoteGrpcServer server;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GRPC:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @BeforeEach
  void connect() {
    server = new RemoteGrpcServer("localhost", GRPC_PORT, "root", DEFAULT_PASSWORD_FOR_TESTS, true, List.of());
  }

  @AfterEach
  void disconnect() {
    if (server != null) {
      server.close();
      server = null;
    }
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
  }

  @Test
  void progressListsTheRunningOperationsOfOneDatabase() {
    final OperationProgress operation = OperationProgressRegistry.instance()
        .register(getDatabaseName(), "check database fix");
    try {
      operation.onProgress("Checking edges 'Knows'", 2, 5, 30, 60);

      final List<OperationProgressInfo> operations = server.getProgress(getDatabaseName());

      assertThat(operations).hasSize(1);
      assertThat(operations.getFirst().getOperation()).isEqualTo("check database fix");
      assertThat(operations.getFirst().getStepName()).isEqualTo("Checking edges 'Knows'");
      assertThat(operations.getFirst().getPercentage()).isEqualTo(50);
    } finally {
      OperationProgressRegistry.instance().unregister(operation);
    }
  }

  @Test
  void progressIsEmptyWhenNothingIsRunning() {
    assertThat(server.getProgress(getDatabaseName())).isEmpty();
  }

  /**
   * The database-scoped client method, with the HTTP port pointed at nothing. A {@code RemoteGrpcDatabase}
   * that still fell back to {@code RemoteDatabase.getProgress()} would fail to connect instead of answering.
   */
  @Test
  void progressIsPolledOverGrpcAndNotOverTheInheritedHttpRoute() {
    final OperationProgress operation = OperationProgressRegistry.instance()
        .register(getDatabaseName(), "compact index");
    try (final RemoteGrpcDatabase database = new RemoteGrpcDatabase(server, "localhost", GRPC_PORT, DEAD_HTTP_PORT,
        getDatabaseName(), "root", DEFAULT_PASSWORD_FOR_TESTS)) {
      operation.onProgress("Compacting page 10", 1, 1, 10, 40);

      final List<JSONObject> operations = database.getProgress();

      assertThat(operations).hasSize(1);
      assertThat(operations.getFirst().getString("operation", "")).isEqualTo("compact index");
      assertThat(operations.getFirst().getString("stepName", "")).isEqualTo("Compacting page 10");
      assertThat(operations.getFirst().getInt("stepIndex", -1)).isEqualTo(1);
      assertThat(operations.getFirst().getInt("totalSteps", -1)).isEqualTo(1);
      assertThat(operations.getFirst().getLong("done", -1L)).isEqualTo(10);
      assertThat(operations.getFirst().getLong("total", -1L)).isEqualTo(40);
      assertThat(operations.getFirst().getInt("percentage", -1)).isEqualTo(25);
    } finally {
      OperationProgressRegistry.instance().unregister(operation);
    }
  }

  @Test
  void sessionsAreListedWithTheirClientMetadata() {
    final ServerSecurityUser user = getServer(0).getSecurity().authenticate("root", DEFAULT_PASSWORD_FOR_TESTS, null);
    final HttpAuthSession session = getServer(0).getHttpServer().getAuthSessionManager()
        .createSession(user, "192.0.2.7", "arcade-client/1.0", "IT", "Turin");
    try {
      final List<SessionInfo> sessions = server.listSessions();

      final SessionInfo info = sessions.stream().filter(s -> session.getToken().equals(s.getToken()))
          .findFirst().orElseThrow();
      assertThat(info.getUser()).isEqualTo("root");
      assertThat(info.getSourceIp()).isEqualTo("192.0.2.7");
      assertThat(info.getUserAgent()).isEqualTo("arcade-client/1.0");
    } finally {
      getServer(0).getHttpServer().getAuthSessionManager().removeSession(session.getToken());
    }
  }
}
