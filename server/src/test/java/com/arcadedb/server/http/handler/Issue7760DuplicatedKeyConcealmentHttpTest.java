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
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.RID;
import com.arcadedb.exception.DuplicatedKeyException;
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
 * Issue #7760 on the HTTP transport: production mode conceals the duplicated-key VALUES in {@code exceptionArgs}
 * (stored data), while the class, the index name and the RID stay so the remote driver can still rebuild a typed
 * {@code DuplicatedKeyException}. The gRPC twin is {@code Issue7760GrpcDuplicatedKeyConcealmentTest}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7760DuplicatedKeyConcealmentHttpTest {
  private static final String SECRET = "alice@example.com";

  private static DuplicatedKeyException duplicate() {
    return new DuplicatedKeyException("User[email]", "[" + SECRET + "]", new RID(3, 7));
  }

  @Test
  void productionModeConcealsTheKeyValuesButKeepsTheTypedShape() {
    final HandledResponse response = handle(duplicate(), "production");

    assertThat(response.statusCode).isEqualTo(409);
    assertThat(response.body).doesNotContain(SECRET);

    final JSONObject json = new JSONObject(response.body);
    assertThat(json.getString("exception")).isEqualTo(DuplicatedKeyException.class.getName());
    final String[] parts = json.getString("exceptionArgs").split("\\|");
    assertThat(parts).containsExactly("User[email]", ArcadeDBServer.CONCEALED_DUPLICATED_KEYS, "#3:7");
  }

  @Test
  void developmentModeKeepsTheKeyValues() {
    final HandledResponse response = handle(duplicate(), "development");

    assertThat(new JSONObject(response.body).getString("exceptionArgs")).isEqualTo("User[email]|[" + SECRET + "]|#3:7");
  }

  private record HandledResponse(int statusCode, String body) {
  }

  private HandledResponse handle(final RuntimeException toThrow, final String serverMode) {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.SERVER_MODE, serverMode);

    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getObservationRegistry()).thenReturn(ObservationRegistry.create());
    when(server.getConfiguration()).thenReturn(configuration);
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
    when(exchange.getRelativePath()).thenReturn("/command/db");
    when(exchange.getResponseSender()).thenReturn(sender);

    new ThrowingHandler(httpServer, toThrow).handleRequest(exchange);

    final ArgumentCaptor<String> body = ArgumentCaptor.forClass(String.class);
    verify(sender).send(body.capture());
    return new HandledResponse(statusCode[0], body.getValue());
  }

  /** Handler whose execute() throws, standing in for a write refused with a duplicated key. */
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
