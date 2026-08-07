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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.serializer.json.JSONException;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityUser;

import io.micrometer.observation.ObservationRegistry;
import io.undertow.io.Sender;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HeaderMap;
import io.undertow.util.Methods;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Regression test for the wire-facing half of issue #5935. A handler reads its request payload through the
 * {@link JSONObject} getters, and those now report a missing property, an explicit JSON null and a type mismatch with
 * {@link JSONException} instead of leaking GSON's own exception types. All three describe a malformed request, so the
 * catch chain must answer HTTP 400: without a dedicated arm they fell through to the generic {@code catch (Throwable)}
 * and became a 500, telling the client the server was at fault for the client's own payload.
 * <p>
 * Drives the REAL {@code handleRequest} catch chain with a handler whose {@code execute()} throws, the same seam the
 * production exception propagates through, mirroring {@code Issue5064CommittedRemotelyHttpStatusTest}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5935InvalidJsonPayloadHttpStatusTest {

  @Test
  void missingPropertyMapsTo400() {
    final HandledResponse response = handle(new JSONException("JSONObject[database] not found"));

    assertThat(response.statusCode)
        .as("a payload missing a required property is a client error (body=%s)", response.body)
        .isEqualTo(400);

    final JSONObject json = new JSONObject(response.body);
    assertThat(json.getString("error")).isEqualTo("Invalid JSON payload");
    assertThat(json.getString("exception")).isEqualTo(JSONException.class.getName());
    assertThat(json.getString("detail")).contains("database");
  }

  @Test
  void explicitNullMapsTo400() {
    final HandledResponse response = handle(new JSONException("JSONObject[database] is null"));

    assertThat(response.statusCode).isEqualTo(400);
    assertThat(new JSONObject(response.body).getString("detail")).contains("is null");
  }

  @Test
  void typeMismatchMapsTo400() {
    final HandledResponse response = handle(new JSONException("JSONObject[overwrite] is not a boolean (\"yes\")"));

    assertThat(response.statusCode)
        .as("a loosely-typed value used to answer a silent default or a 500 (body=%s)", response.body)
        .isEqualTo(400);
    assertThat(new JSONObject(response.body).getString("detail")).contains("overwrite");
  }

  @Test
  void wrappedInCommandExecutionExceptionKeeps400() {
    // Command planners wrap exceptions in CommandExecutionException: the client-error status must survive it.
    final HandledResponse response = handle(new CommandExecutionException("Error on command execution",
        new JSONException("JSONObject[params] is not a JSON object (JSON array)")));

    assertThat(response.statusCode).isEqualTo(400);
    assertThat(new JSONObject(response.body).getString("exception")).isEqualTo(JSONException.class.getName());
  }

  @Test
  void wrappedInTransactionExceptionKeeps400() {
    // The auto-commit wrapper in DatabaseAbstractHandler wraps any Exception thrown by execute() in a plain
    // TransactionException; without the symmetric arm the 400 degraded back to "Error on transaction commit" 500.
    final HandledResponse response = handle(new TransactionException("Error on transaction commit",
        new JSONException("JSONObject[value] is null")));

    assertThat(response.statusCode).isEqualTo(400);
    assertThat(new JSONObject(response.body).getString("exception")).isEqualTo(JSONException.class.getName());
  }

  @Test
  void unrelatedFailureStillMapsTo500() {
    // Guards the catch ORDER and scope: only JSONException moved, a genuine server fault keeps its 500.
    final HandledResponse response = handle(new IllegalStateException("simulated internal failure"));

    assertThat(response.statusCode).isEqualTo(500);
  }

  private record HandledResponse(int statusCode, String body) {
  }

  /**
   * Runs the real {@link AbstractServerHttpHandler#handleRequest} against a handler whose {@code execute()} throws the
   * given exception, and captures the status code and JSON body the catch chain produces.
   */
  private HandledResponse handle(final RuntimeException toThrow) {
    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getObservationRegistry()).thenReturn(ObservationRegistry.create());
    when(server.getConfiguration()).thenReturn(new ContextConfiguration());
    when(server.getServerName()).thenReturn("test");

    final HttpServer httpServer = mock(HttpServer.class);
    when(httpServer.getServer()).thenReturn(server);

    final Sender sender = mock(Sender.class);
    final HttpServerExchange exchange = mock(HttpServerExchange.class);
    final int[] statusCode = { 200 };
    when(exchange.setStatusCode(anyInt())).thenAnswer(invocation -> {
      statusCode[0] = invocation.getArgument(0);
      return exchange;
    });
    when(exchange.getStatusCode()).thenAnswer(invocation -> statusCode[0]);
    when(exchange.getRequestHeaders()).thenReturn(new HeaderMap());
    when(exchange.getResponseHeaders()).thenReturn(new HeaderMap());
    when(exchange.getRequestMethod()).thenReturn(Methods.POST);
    when(exchange.getRelativePath()).thenReturn("/command/graph");
    when(exchange.getResponseSender()).thenReturn(sender);

    new ThrowingHandler(httpServer, toThrow).handleRequest(exchange);

    final ArgumentCaptor<String> body = ArgumentCaptor.forClass(String.class);
    verify(sender).send(body.capture());
    return new HandledResponse(statusCode[0], body.getValue());
  }

  /** Handler whose execute() throws, standing in for a payload read that rejects the client's JSON. */
  private static final class ThrowingHandler extends AbstractServerHttpHandler {
    private final RuntimeException toThrow;

    private ThrowingHandler(final HttpServer httpServer, final RuntimeException toThrow) {
      super(httpServer);
      this.toThrow = toThrow;
    }

    @Override
    protected ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user,
        final JSONObject payload) {
      throw toThrow;
    }

    @Override
    public boolean isRequireAuthentication() {
      // Skip the Authorization machinery: this test targets the error-mapping catch chain only.
      return false;
    }
  }
}
