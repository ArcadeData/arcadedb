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
package com.arcadedb.server.http.handler;

import com.arcadedb.engine.OperationProgress;
import com.arcadedb.engine.OperationProgressRegistry;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityUser;

import io.undertow.server.HttpServerExchange;
import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * The progress endpoint (issue #5372) returns the running maintenance operations of a database and enforces
 * the per-database authorization model.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class GetProgressHandlerTest {

  /**
   * A mocked {@link HttpServer} that also answers {@link HttpServer#getServer()}. The base handler's
   * {@code checkAuthorizationOnDatabase} binds the authenticated principal onto the database's
   * {@code DatabaseContext} so the engine's per-type ACL layer enforces on the ungated handlers
   * (GHSA-c23x-pqcj-7hfm), and that binding reaches through to the server. This fixture owns no database, so
   * {@code existsDatabase} answers false and the binding is correctly skipped; the progress registry these
   * tests drive is server-independent. The binding itself is covered end-to-end, against a real server, by
   * {@code UngatedHandlerPerTypeAclIT}.
   */
  private HttpServer httpServer() {
    final HttpServer httpServer = mock(HttpServer.class);
    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.existsDatabase(anyString())).thenReturn(false);
    when(httpServer.getServer()).thenReturn(server);
    return httpServer;
  }

  private HttpServerExchange exchangeFor(final String databaseName) {
    final HttpServerExchange exchange = mock(HttpServerExchange.class);
    final Map<String, Deque<String>> params = new HashMap<>();
    if (databaseName != null) {
      final Deque<String> db = new ArrayDeque<>();
      db.add(databaseName);
      params.put("database", db);
    }
    when(exchange.getQueryParameters()).thenReturn(params);
    return exchange;
  }

  private ServerSecurityUser userAuthorizedOn(final String... databases) {
    final ServerSecurityUser user = mock(ServerSecurityUser.class);
    final Set<String> authorized = Set.of(databases);
    when(user.canAccessToDatabase(anyString()))
        .thenAnswer(invocation -> authorized.contains("*") || authorized.contains((String) invocation.getArgument(0)));
    return user;
  }

  @Test
  void returnsRunningOperationsForAuthorizedUser() {
    final GetProgressHandler handler = new GetProgressHandler(httpServer());

    final OperationProgress op = OperationProgressRegistry.instance().register("progressdb", "check database fix");
    try {
      op.onProgress("Checking vertices 'Account'", 3, 9, 42, 100);

      final ExecutionResponse response = handler.execute(exchangeFor("progressdb"), userAuthorizedOn("progressdb"), null);

      assertThat(response.getCode()).isEqualTo(200);
      final JSONArray result = new JSONObject(response.getResponse()).getJSONArray("result");
      assertThat(result.length()).isEqualTo(1);
      final JSONObject json = result.getJSONObject(0);
      assertThat(json.getString("operation", "")).isEqualTo("check database fix");
      assertThat(json.getString("stepName", "")).isEqualTo("Checking vertices 'Account'");
      assertThat(json.getInt("stepIndex", -1)).isEqualTo(3);
      assertThat(json.getInt("totalSteps", -1)).isEqualTo(9);
      assertThat(json.getInt("percentage", -1)).isEqualTo(42);
    } finally {
      OperationProgressRegistry.instance().unregister(op);
    }
  }

  @Test
  void emptyResultWhenNothingIsRunning() {
    final GetProgressHandler handler = new GetProgressHandler(httpServer());

    final ExecutionResponse response = handler.execute(exchangeFor("idledb"), userAuthorizedOn("*"), null);

    assertThat(response.getCode()).isEqualTo(200);
    assertThat(new JSONObject(response.getResponse()).getJSONArray("result").length()).isEqualTo(0);
  }

  @Test
  void unauthorizedDatabaseIsRejected() {
    final GetProgressHandler handler = new GetProgressHandler(httpServer());

    // The consolidated checkAuthorizationOnDatabase gate throws SecurityException, mapped to HTTP 403 by the
    // base handler's catch chain.
    assertThatThrownBy(() -> handler.execute(exchangeFor("someoneelsesdb"), userAuthorizedOn("progressdb"), null))
        .isInstanceOf(SecurityException.class);
  }

  @Test
  void missingDatabaseParameterIsRejected() {
    final GetProgressHandler handler = new GetProgressHandler(httpServer());

    // A missing database name throws IllegalArgumentException, mapped to HTTP 400 by the base handler.
    assertThatThrownBy(() -> handler.execute(exchangeFor(null), userAuthorizedOn("*"), null))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
