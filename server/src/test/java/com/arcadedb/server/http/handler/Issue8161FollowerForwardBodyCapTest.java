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
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.HAServerPlugin;
import com.arcadedb.server.http.FakeLeader;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;
import io.undertow.server.RequestTooBigException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Regression test for issue #8161: a chunked batch upload over {@code arcadedb.server.httpBodyContentMaxSize} sent
 * to a FOLLOWER tripped the capped {@link PostBatchHandler.CountingInputStream} while the JDK client was relaying
 * it to the leader. The refusal surfaced out of {@code HttpClient.send} wrapped in a plain {@link IOException} and
 * landed in {@code forwardBatchToLeader}'s generic arm, which RETURNED a hand-built 503 "Error forwarding batch to
 * leader" - a retryable status, blaming the leader, for a request that can only ever be refused. The leader answers
 * the same request with the documented JSON 413, built by {@code sendMappedErrorResponse}; the follower now
 * rethrows the {@link RequestTooBigException} so the same arm builds the same answer.
 */
class Issue8161FollowerForwardBodyCapTest {

  private static final long CAP_BYTES  = 1_024L;
  private static final int  BODY_BYTES = 256 * 1_024;

  private static PostBatchHandler handler() {
    final ContextConfiguration cfg = new ContextConfiguration();
    cfg.setValue(GlobalConfiguration.HA_PROXY_CONNECT_TIMEOUT, 5_000L);
    cfg.setValue(GlobalConfiguration.HA_PROXY_BATCH_READ_TIMEOUT, 60_000L);
    cfg.setValue(GlobalConfiguration.SERVER_HTTP_BODY_CONTENT_MAX_SIZE, CAP_BYTES);
    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getConfiguration()).thenReturn(cfg);
    final HttpServer httpServer = mock(HttpServer.class);
    when(httpServer.getServer()).thenReturn(server);
    return new PostBatchHandler(httpServer);
  }

  private static HAServerPlugin haPointingAt(final String leaderAddress) {
    final HAServerPlugin ha = mock(HAServerPlugin.class);
    when(ha.getLeaderAddress()).thenReturn(leaderAddress);
    when(ha.getClusterToken()).thenReturn("test-token");
    return ha;
  }

  private static ServerSecurityUser rootUser() {
    final ServerSecurityUser user = mock(ServerSecurityUser.class);
    when(user.getName()).thenReturn("root");
    return user;
  }

  private static byte[] ndjson(final int size) {
    final StringBuilder sb = new StringBuilder(size + 64);
    int i = 0;
    while (sb.length() < size)
      sb.append("{\"@type\":\"vertex\",\"@id\":\"v").append(i++).append("\",\"@class\":\"V\"}\n");
    return sb.toString().getBytes(StandardCharsets.UTF_8);
  }

  /**
   * The follower relays a body with no declared length (a bare exchange answers -1, as a chunked upload does) that
   * is far over this node's cap. Both encodings take the same path: the cap trips while the leader is still
   * reading, before it has sent any response header, so nothing is on the wire yet and the status is still ours.
   */
  @ParameterizedTest(name = "streaming={0}")
  @ValueSource(booleans = { false, true })
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  void anOverCapChunkedBodyIsRefusedAs413NotAnsweredAs503(final boolean streaming) throws Exception {
    try (final FakeLeader leader = FakeLeader.draining()) {
      final HttpServerExchange exchange = new HttpServerExchange(null);
      assertThat(exchange.getRequestContentLength()).isEqualTo(-1L);
      final PostBatchHandler.CountingInputStream body = new PostBatchHandler.CountingInputStream(exchange,
          new ByteArrayInputStream(ndjson(BODY_BYTES)), CAP_BYTES);

      final PostBatchHandler handler = handler();
      final Throwable thrown = catchThrowable(() -> handler.forwardBatchToLeader(exchange,
          haPointingAt(leader.address()), "mydb", rootUser(), "application/x-ndjson", body, streaming));
      assertThat(thrown).isInstanceOf(RequestTooBigException.class)
          .hasMessageContaining(GlobalConfiguration.SERVER_HTTP_BODY_CONTENT_MAX_SIZE.getKey());

      // The user-visible contract, not just the intermediate step: the rethrown refusal goes through the same
      // classification sendMappedErrorResponse sends from, and comes out as the leader's 413 naming the setting.
      final AbstractServerHttpHandler.ErrorClassification classification = handler.classifyError(thrown);
      assertThat(classification.status()).isEqualTo(413);
      assertThat(classification.message()).contains(GlobalConfiguration.SERVER_HTTP_BODY_CONTENT_MAX_SIZE.getKey());
      assertThat(classification.exceptionArgs()).isEqualTo(String.valueOf(CAP_BYTES));

      assertThat(body.hasBodyFailed()).isTrue();
      // The kernel backlog can take the relayed bytes before accept() returns on the fixture thread: wait for it.
      assertThat(leader.awaitFirstConnection(30, TimeUnit.SECONDS)).isTrue();
    }
  }

  /**
   * The discrimination has to be on WHY the forward failed, not on the fact that it did: a leader that drops the
   * connection while a body UNDER the cap is being relayed is still a leader-side failure and keeps its 503.
   */
  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  void aLeaderThatDropsTheConnectionStillAnswers503() throws Exception {
    try (final FakeLeader leader = FakeLeader.dropping()) {
      final HttpServerExchange exchange = new HttpServerExchange(null);
      final PostBatchHandler.CountingInputStream body = new PostBatchHandler.CountingInputStream(exchange,
          new ByteArrayInputStream(ndjson(512)), CAP_BYTES);

      final ExecutionResponse response = handler().forwardBatchToLeader(exchange, haPointingAt(leader.address()),
          "mydb", rootUser(), "application/x-ndjson", body, false);

      assertThat(response.getCode()).isEqualTo(503);
      assertThat(new JSONObject(response.getResponse()).getString("error", "")).startsWith("Error forwarding batch to leader");
      assertThat(body.hasBodyFailed()).isFalse();
    }
  }
}
