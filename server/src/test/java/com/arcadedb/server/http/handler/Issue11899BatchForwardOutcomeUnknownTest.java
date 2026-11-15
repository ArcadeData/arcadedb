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
package com.arcadedb.server.http.handler;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.exception.TransactionOutcomeUnknownException;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.HAServerPlugin;
import com.arcadedb.server.TestServerHelper;
import com.arcadedb.server.UnstartedHttpServers;
import com.arcadedb.server.http.FakeLeader;
import io.undertow.server.HttpServerExchange;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Issue #11899: a load a follower forwards to the leader over {@code POST /api/v1/batch}, whose connection to the leader is
 * lost after the load went out, may have been applied by the leader. It was answered with an untyped 503, which tells a
 * client the load was refused before it ran; measured with a killed leader, 6 of 2,792 batches answered so were stored
 * whole. It is the outcome-unknown 504 with {@link TransactionOutcomeUnknownException} now, as a forwarded SQL write is
 * answered (issue #11887), and so is a leader that does not answer within the deadline.
 */
class Issue11899BatchForwardOutcomeUnknownTest {
  @RegisterExtension
  static final UnstartedHttpServers HTTP_SERVERS = new UnstartedHttpServers();

  private static final String LOAD = "{\"@type\":\"vertex\",\"@class\":\"B\",\"k\":\"a\"}\n";

  @Test
  void aLeaderConnectionLostAfterTheLoadWasSentIsAnOutcomeUnknown() throws Exception {
    assertOutcomeUnknownWhenTheLeaderDropsTheLoad(false);
  }

  @Test
  void theStreamingEncodingAnswersTheSameBeforeAnyLineWasRelayed() throws Exception {
    assertOutcomeUnknownWhenTheLeaderDropsTheLoad(true);
  }

  @Test
  void aLeaderThatDoesNotAnswerInTimeIsAnOutcomeUnknown() throws Exception {
    try (final FakeLeader leader = FakeLeader.draining()) {
      final ExecutionResponse response = forward(leader, 1_000L, false);

      assertThat(response.getCode()).isEqualTo(504);
      final JSONObject body = new JSONObject(response.getResponse());
      assertThat(body.getString("exception")).isEqualTo(TransactionOutcomeUnknownException.class.getName());
      assertThat(body.getString("error")).contains(leader.address()).contains("may have been applied");
    }
  }

  private static void assertOutcomeUnknownWhenTheLeaderDropsTheLoad(final boolean streaming) throws Exception {
    // Reads the whole request head, then closes the connection without a byte of answer: a leader killed mid-load
    try (final FakeLeader leader = FakeLeader.scripted(out -> out.close())) {
      final ExecutionResponse response = forward(leader, 30_000L, streaming);

      assertThat(leader.awaitRequestReceived(5, TimeUnit.SECONDS)).as("the leader read the forwarded load").isTrue();
      assertThat(response.getCode()).as("body=%s", response.getResponse()).isEqualTo(504);
      final JSONObject body = new JSONObject(response.getResponse());
      assertThat(body.getString("exception")).isEqualTo(TransactionOutcomeUnknownException.class.getName());
      assertThat(body.getString("error")).contains(leader.address()).contains("may have been applied");
    }
  }

  private static ExecutionResponse forward(final FakeLeader leader, final long batchReadTimeoutMs, final boolean streaming)
      throws Exception {
    final ContextConfiguration cfg = new ContextConfiguration();
    cfg.setValue(GlobalConfiguration.HA_PROXY_CONNECT_TIMEOUT, 5_000L);
    cfg.setValue(GlobalConfiguration.HA_PROXY_BATCH_READ_TIMEOUT, batchReadTimeoutMs);
    final ArcadeDBServer server = TestServerHelper.unstartedServer((String) null, cfg);
    final PostBatchHandler handler = new PostBatchHandler(HTTP_SERVERS.of(server));

    final HAServerPlugin ha = mock(HAServerPlugin.class);
    when(ha.getLeaderAddress()).thenReturn(leader.address());
    when(ha.getClusterToken()).thenReturn("test-token");

    final HttpServerExchange exchange = new HttpServerExchange(null);
    final PostBatchHandler.CountingInputStream body = new PostBatchHandler.CountingInputStream(exchange,
        new ByteArrayInputStream(LOAD.getBytes(StandardCharsets.UTF_8)));
    return handler.forwardBatchToLeader(exchange, ha, "mydb", TestServerHelper.securityUser("root"), "application/x-ndjson",
        body, streaming);
  }
}
