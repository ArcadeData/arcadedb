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
package com.arcadedb.server.ws;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #8065, follow-up to #7909: the {@code /ws} frame budget reached the other opcode.
 * <p>
 * {@code WebSocketReceiveListener} overrode {@code getMaxTextBufferSize()} and nothing else, so BINARY frames
 * kept Undertow's {@code getMaxBinaryBufferSize()} default of {@code -1} - unbounded. An authenticated client
 * could open a binary frame, never send its final fragment, and pin an arbitrary amount of heap for the life of
 * the connection: the same denial of service the text caps exist to prevent, reached through the opcode nobody
 * had bounded.
 * <p>
 * The caps are set absurdly low here so the tests exercise the refusal rather than the heap.
 */
class Issue8065BinaryFrameBudgetIT extends BaseGraphServerTest {
  private static final int CONTROL_CAP = 2 * 1024;
  private static final int INSERT_CAP  = 64 * 1024;

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    config.setValue(GlobalConfiguration.SERVER_WS_MAX_CONTROL_FRAME_SIZE, (long) CONTROL_CAP);
    config.setValue(GlobalConfiguration.SERVER_WS_MAX_INSERT_FRAME_SIZE, (long) INSERT_CAP);
  }

  /**
   * The bug as reported: a binary frame over the cap is refused by closing the connection, not accumulated and
   * complained about afterwards. {@code BufferedBinaryMessage} checks the cap as it reads, so what is bounded is
   * what is actually allocated.
   */
  @Test
  void anOversizeBinaryFrameKillsTheConnectionInsteadOfBeingBuffered() throws Throwable {
    try (final var client = new WebSocketClientHelper(wsUrl(), "root", DEFAULT_PASSWORD_FOR_TESTS)) {
      // The connection works first, so what follows is the binary frame being refused and not a broken fixture.
      assertThat(new JSONObject(client.send(subscribe("V1"))).getString("result", "")).isEqualTo("ok");

      client.sendBinaryWithoutWaiting(new byte[CONTROL_CAP * 4]);

      assertConnectionIsClosed(client);
    }
  }

  /**
   * A binary frame is charged the control budget even on a connection that has an insert session open, where a
   * text frame of the same size would go through. No binary frame can ever be a {@code chunk} - the insert
   * protocol is dispatched from {@code onFullTextMessage} alone - so the larger budget would be a hole with no
   * legitimate user.
   */
  @Test
  void aBinaryFrameNeverEarnsTheLargerInsertBudget() throws Throwable {
    try (final var client = new WebSocketClientHelper(wsUrl(), "root", DEFAULT_PASSWORD_FOR_TESTS)) {
      final String sessionId = new JSONObject(client.send(start())).getString("sessionId");
      assertThat(sessionId).isNotNull();

      // Comfortably inside the insert cap and comfortably over the control cap: the refusal can only be the
      // control budget being applied.
      client.sendBinaryWithoutWaiting(new byte[CONTROL_CAP * 4]);

      assertConnectionIsClosed(client);
    }

    // The session the connection left behind is rolled back by the close hook, as for any dropped connection.
    Awaitility.await().atMost(Duration.ofSeconds(30))
        .until(() -> getServer(0).getHttpServer().getInsertSessionManager().getOpenSessionCount() == 0);
  }

  /**
   * A binary frame inside the budget is answered with an error naming the contract rather than discarded in
   * silence, and it leaves the connection usable: a stray binary frame is a client mistake, not an attack, and
   * it must not cost an established subscription its connection.
   */
  @Test
  void aBinaryFrameInsideTheBudgetIsRefusedWithAnErrorAndTheConnectionSurvives() throws Throwable {
    try (final var client = new WebSocketClientHelper(wsUrl(), "root", DEFAULT_PASSWORD_FOR_TESTS)) {
      assertThat(new JSONObject(client.send(subscribe("V1"))).getString("result", "")).isEqualTo("ok");

      client.sendBinaryWithoutWaiting(new byte[16]);

      final String answer = client.popMessage();
      assertThat(answer).as("a binary frame inside the budget must be answered, not dropped").isNotNull();
      final JSONObject error = new JSONObject(answer);
      assertThat(error.getString("result", "")).isEqualTo("error");
      assertThat(error.getString("error", "")).containsIgnoringCase("binary");

      // Still alive: the next text frame is answered exactly as before.
      assertThat(new JSONObject(client.send(subscribe("V1"))).getString("result", "")).isEqualTo("ok");
    }
  }

  /**
   * The contract is stated once per connection, not once per frame. Answering every binary frame would trade the
   * buffering amplification this issue is about for a queueing one: a six-byte frame on the wire would cost the
   * server a ~150-byte frame queued towards a peer that may never read it.
   */
  @Test
  void theBinaryRefusalIsSentOncePerConnectionAndNotOncePerFrame() throws Throwable {
    try (final var client = new WebSocketClientHelper(wsUrl(), "root", DEFAULT_PASSWORD_FOR_TESTS)) {
      client.sendBinaryWithoutWaiting(new byte[16]);
      assertThat(client.popMessage()).as("the first binary frame is answered").isNotNull();

      for (int i = 0; i < 5; i++)
        client.sendBinaryWithoutWaiting(new byte[16]);

      // A text frame sent last: its answer is what proves the binary frames ahead of it produced none, since
      // frames leave the server in the order they were queued.
      assertThat(new JSONObject(client.send(subscribe("V1"))).getString("result", "")).isEqualTo("ok");
      assertThat(client.popMessage(500)).as("only the first binary frame may be answered").isNull();
    }
  }

  /**
   * A binary frame with no payload at all. {@link #onFullBinaryMessage} hands the pooled buffer back before it
   * does anything else, and freeing an empty one has to be as safe as freeing a full one - raised in review of
   * #8065 as the edge case the other tests do not reach, since every one of them sends bytes.
   */
  @Test
  void aZeroLengthBinaryFrameIsAnsweredAndFreedLikeAnyOther() throws Throwable {
    try (final var client = new WebSocketClientHelper(wsUrl(), "root", DEFAULT_PASSWORD_FOR_TESTS)) {
      client.sendBinaryWithoutWaiting(new byte[0]);

      final String answer = client.popMessage();
      assertThat(answer).as("a zero-length binary frame must be answered like any other").isNotNull();
      assertThat(new JSONObject(answer).getString("result", "")).isEqualTo("error");

      // Nothing was broken by freeing an empty payload: the connection still serves text frames.
      assertThat(new JSONObject(client.send(subscribe("V1"))).getString("result", "")).isEqualTo("ok");
    }
  }

  // ---------------------------------------------------------------------------------------------------------

  /**
   * The server answers an over-budget frame with a 1009 TOO_BIG close, which is asynchronous: poll the write
   * side until it refuses rather than assuming the close has already landed.
   */
  private static void assertConnectionIsClosed(final WebSocketClientHelper client) {
    Awaitility.await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(200))
        .untilAsserted(() -> assertThatThrownBy(() -> client.sendWithoutWaiting("{\"action\":\"unsubscribe\"}"))
            .as("the connection must be closed after an over-budget binary frame")
            .isInstanceOf(IOException.class));
  }

  private String wsUrl() {
    return "ws://localhost:" + getServer(0).getHttpServer().getPort() + "/ws";
  }

  private String subscribe(final String type) {
    return new JSONObject().put("action", "subscribe").put("database", getDatabaseName()).put("type", type).toString();
  }

  private String start() {
    return new JSONObject()
        .put("action", "start")
        .put("database", getDatabaseName())
        .put("options", new JSONObject().put("targetType", "Person"))
        .toString();
  }
}
