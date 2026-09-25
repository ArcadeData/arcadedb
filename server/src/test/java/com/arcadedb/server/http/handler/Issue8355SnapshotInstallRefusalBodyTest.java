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
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.http.RetryLaterException;
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
 * Issue #8355: the refusal a node installing a snapshot answers every request with, before any handler runs, names its
 * exception and carries its back-off in {@code exceptionArgs}. That is what lets a follower that forwarded a SQL write
 * to it tell this refusal - nothing ran - apart from an anonymous 503 that something between the two nodes may have
 * produced after the write ran, and answer its own client 503 + {@code Retry-After} only for the former.
 */
class Issue8355SnapshotInstallRefusalBodyTest {

  @Test
  void theSnapshotInstallRefusalIsATypedServiceUnavailableWithItsRetryAfter() {
    final Sender sender = mock(Sender.class);
    final HttpServerExchange exchange = mock(HttpServerExchange.class);
    final int[] statusCode = { 200 };
    when(exchange.setStatusCode(anyInt())).thenAnswer(invocation -> {
      statusCode[0] = invocation.getArgument(0);
      return exchange;
    });
    when(exchange.getRequestHeaders()).thenReturn(new HeaderMap());
    final HeaderMap responseHeaders = new HeaderMap();
    when(exchange.getResponseHeaders()).thenReturn(responseHeaders);
    when(exchange.getRequestMethod()).thenReturn(Methods.POST);
    when(exchange.getRelativePath()).thenReturn("/command/graph");
    when(exchange.getResponseSender()).thenReturn(sender);

    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getObservationRegistry()).thenReturn(ObservationRegistry.create());
    when(server.getConfiguration()).thenReturn(new ContextConfiguration());
    when(server.getServerName()).thenReturn("test");
    when(server.isSnapshotInstallInProgress()).thenReturn(true);
    final HttpServer httpServer = mock(HttpServer.class);
    when(httpServer.getServer()).thenReturn(server);

    final boolean[] executed = { false };
    new AbstractServerHttpHandler(httpServer) {
      @Override
      protected ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user,
          final JSONObject payload) {
        executed[0] = true;
        return new ExecutionResponse(200, "{}");
      }

      @Override
      public boolean isRequireAuthentication() {
        return false;
      }
    }.handleRequest(exchange);

    final ArgumentCaptor<String> sent = ArgumentCaptor.forClass(String.class);
    verify(sender).send(sent.capture());
    final JSONObject body = new JSONObject(sent.getValue());

    assertThat(executed[0]).as("the request is refused before any handler runs").isFalse();
    assertThat(statusCode[0]).isEqualTo(503);
    assertThat(responseHeaders.getFirst("Retry-After"))
        .isEqualTo(String.valueOf(RetryLaterException.SNAPSHOT_INSTALL_RETRY_AFTER_SECONDS));
    assertThat(body.getString("error")).isEqualTo(RetryLaterException.SNAPSHOT_INSTALL_REFUSAL);
    assertThat(body.getString("exception")).isEqualTo(RetryLaterException.class.getName());
    assertThat(body.getString("exceptionArgs"))
        .isEqualTo(String.valueOf(RetryLaterException.SNAPSHOT_INSTALL_RETRY_AFTER_SECONDS));
    assertThat(body.getString("detail"))
        .as("a client reading the typed body's detail - RemoteHttpComponent does - keeps the reason")
        .isEqualTo(RetryLaterException.SNAPSHOT_INSTALL_REFUSAL);
  }
}
