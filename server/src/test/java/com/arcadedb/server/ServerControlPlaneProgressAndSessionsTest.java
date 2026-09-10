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
package com.arcadedb.server;

import com.arcadedb.engine.OperationProgress;
import com.arcadedb.engine.OperationProgressRegistry;
import com.arcadedb.server.http.HttpAuthSession;
import com.arcadedb.server.http.HttpAuthSessionManager;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityUser;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * The two control-plane reads issue #7310 added, at the layer both transports share.
 * <p>
 * The case a live server cannot easily produce is the one worth pinning here: a deployment running gRPC
 * without the HTTP listener. {@code ArcadeDBServer.getHttpServer()} is then null - the state
 * {@code GrpcServerPlugin} already handles when it wires the auth interceptor - and
 * {@code ListSessions} has to answer "no HTTP sessions" rather than fault, because that is the true
 * answer for a server that has no HTTP sessions to have.
 */
class ServerControlPlaneProgressAndSessionsTest {

  @Test
  void sessionsAreEmptyWhenTheServerRunsWithoutAnHttpListener() {
    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getHttpServer()).thenReturn(null);

    assertThat(new ServerControlPlane(server).listHttpSessions()).isEmpty();
  }

  @Test
  void sessionsAreReadLiveFromTheHttpSessionManager() {
    final HttpAuthSessionManager manager = new HttpAuthSessionManager(60_000);
    try {
      final ArcadeDBServer server = mock(ArcadeDBServer.class);
      final HttpServer httpServer = mock(HttpServer.class);
      when(httpServer.getAuthSessionManager()).thenReturn(manager);
      when(server.getHttpServer()).thenReturn(httpServer);

      final ServerControlPlane controlPlane = new ServerControlPlane(server);
      assertThat(controlPlane.listHttpSessions()).isEmpty();

      final ServerSecurityUser user = mock(ServerSecurityUser.class);
      when(user.getName()).thenReturn("root");
      final HttpAuthSession session = manager.createSession(user);

      assertThat(controlPlane.listHttpSessions()).extracting(HttpAuthSession::getToken).contains(session.getToken());

      manager.removeSession(session.getToken());
      assertThat(controlPlane.listHttpSessions()).extracting(HttpAuthSession::getToken)
          .doesNotContain(session.getToken());
    } finally {
      manager.close();
    }
  }

  @Test
  void progressReportsTheOperationsOfTheNamedDatabaseOnly() {
    final ServerControlPlane controlPlane = new ServerControlPlane(mock(ArcadeDBServer.class));

    final OperationProgress operation = OperationProgressRegistry.instance().register("cp7310db", "check database");
    try {
      operation.onProgress("Checking", 1, 4, 5, 10);

      final List<OperationProgress> operations = controlPlane.getProgress("cp7310db");
      assertThat(operations).hasSize(1);
      assertThat(operations.getFirst().getOperation()).isEqualTo("check database");

      assertThat(controlPlane.getProgress("cp7310db-other")).isEmpty();
    } finally {
      OperationProgressRegistry.instance().unregister(operation);
    }
  }

  /**
   * A missing database name is the caller's mistake, not an empty result: the registry is keyed by name,
   * so answering "nothing is running" for a request that named nothing would be a lie. HTTP maps this to
   * 400 and gRPC to INVALID_ARGUMENT.
   */
  @Test
  void progressRefusesAMissingDatabaseName() {
    final ServerControlPlane controlPlane = new ServerControlPlane(mock(ArcadeDBServer.class));

    assertThatThrownBy(() -> controlPlane.getProgress(null)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> controlPlane.getProgress("")).isInstanceOf(IllegalArgumentException.class);
  }
}
