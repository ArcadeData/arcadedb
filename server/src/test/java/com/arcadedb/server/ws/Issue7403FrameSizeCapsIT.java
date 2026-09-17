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
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Part 2 of issue #7403: the {@code /ws} text-frame size caps.
 * <p>
 * {@code WebSocketReceiveListener} used to inherit Undertow's {@code getMaxTextBufferSize()} of {@code -1},
 * unbounded, so every text frame was accumulated whole on the heap before {@code onFullTextMessage} saw it and an
 * authenticated client could pin an arbitrary amount of it per connection. Issue #7382 changed the SIZE of that
 * hole rather than opening it: before it there was no reason to send a large frame, and a {@code chunk} frame now
 * legitimately carries a whole batch.
 * <p>
 * The caps are set absurdly low here so the test exercises the refusal rather than the heap.
 */
class Issue7403FrameSizeCapsIT extends BaseGraphServerTest {
  private static final int CONTROL_CAP = 2 * 1024;
  private static final int INSERT_CAP  = 64 * 1024;
  private static final int MAX_ROWS    = 5;

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    config.setValue(GlobalConfiguration.SERVER_WS_MAX_CONTROL_FRAME_SIZE, (long) CONTROL_CAP);
    config.setValue(GlobalConfiguration.SERVER_WS_MAX_INSERT_FRAME_SIZE, (long) INSERT_CAP);
    config.setValue(GlobalConfiguration.SERVER_WS_MAX_INSERT_CHUNK_ROWS, MAX_ROWS);
  }

  /**
   * A control frame over the cap is refused by closing the connection, not by buffering it and complaining
   * afterwards - which is the whole point: {@code BufferedTextMessage} checks the cap as it reads, so the bound
   * is on what is actually allocated.
   */
  @Test
  void anOversizeControlFrameKillsTheConnectionInsteadOfBeingBuffered() throws Throwable {
    try (final var client = new WebSocketClientHelper(wsUrl(), "root", DEFAULT_PASSWORD_FOR_TESTS)) {
      // The same frame, small, is answered - so what follows is the SIZE being refused and not the frame.
      final JSONObject ack = new JSONObject(client.send(subscribe("V1")));
      assertThat(ack.getString("result", "")).isEqualTo("ok");

      assertThat(client.send(subscribe("V".repeat(CONTROL_CAP * 4))))
          .as("a control frame over the cap must not be answered").isNull();

      // And the connection is gone, not merely silent about that one frame: the server answered the breach with
      // a 1009 TOO_BIG close, so the next write does not even reach it.
      assertThatThrownBy(() -> client.send(subscribe("V1")))
          .as("the connection must be closed after the breach")
          .isInstanceOf(IOException.class);
    }
  }

  /**
   * The budget a connection is charged follows its {@code start} frame: a {@code chunk} many times over the
   * control cap goes through, because the connection asked for an insert session first. Without the raise the
   * same frame would be killed at {@link #CONTROL_CAP}.
   */
  @Test
  void aChunkOverTheControlCapIsAcceptedOnceTheSessionHasStarted() throws Throwable {
    try (final var client = new WebSocketClientHelper(wsUrl(), "root", DEFAULT_PASSWORD_FOR_TESTS)) {
      final String sessionId = new JSONObject(client.send(start())).getString("sessionId");

      // One record whose payload alone is ten times the control cap, and well inside the insert cap.
      final JSONArray records = new JSONArray();
      records.put(new JSONObject().put("name", "x".repeat(CONTROL_CAP * 10)));

      final JSONObject ack = new JSONObject(client.send(chunk(sessionId, 1, records)));
      assertThat(ack.getString("action", "")).isEqualTo("batchAck");
      assertThat(ack.getLong("inserted", -1)).isEqualTo(1);

      new JSONObject(client.send(control("rollback", sessionId)));
    }
  }

  /** A chunk over the INSERT cap is still refused: the larger budget is a budget, not an exemption. */
  @Test
  void aChunkOverTheInsertCapKillsTheConnection() throws Throwable {
    try (final var client = new WebSocketClientHelper(wsUrl(), "root", DEFAULT_PASSWORD_FOR_TESTS)) {
      final String sessionId = new JSONObject(client.send(start())).getString("sessionId");

      final JSONArray records = new JSONArray();
      records.put(new JSONObject().put("name", "x".repeat(INSERT_CAP * 2)));

      assertThat(client.send(chunk(sessionId, 1, records)))
          .as("a chunk over the insert cap must not be answered").isNull();
    }

    // The abandoned session is rolled back by the connection-close hook, as any other dropped connection is.
    org.awaitility.Awaitility.await().atMost(java.time.Duration.ofSeconds(10))
        .until(() -> getServer(0).getHttpServer().getInsertSessionManager().getOpenSessionCount() == 0);
  }

  /**
   * The rows-per-chunk cap is the number a client actually reasons about, and unlike the byte caps a breach of
   * it leaves the session open: the client splits the batch and resends it under the SAME sequence, because the
   * watermark never moved.
   */
  @Test
  void aChunkOverTheRowCapIsRefusedAndTheSessionSurvivesIt() throws Throwable {
    try (final var client = new WebSocketClientHelper(wsUrl(), "root", DEFAULT_PASSWORD_FOR_TESTS)) {
      final String sessionId = new JSONObject(client.send(start())).getString("sessionId");

      final JSONObject refused = new JSONObject(client.send(chunk(sessionId, 1, names(MAX_ROWS + 1))));
      assertThat(refused.getString("result", "")).isEqualTo("error");
      assertThat(refused.getString("detail", ""))
          .contains("more than the " + MAX_ROWS)
          .contains(GlobalConfiguration.SERVER_WS_MAX_INSERT_CHUNK_ROWS.getKey());

      final JSONObject ack = new JSONObject(client.send(chunk(sessionId, 1, names(MAX_ROWS))));
      assertThat(ack.getString("action", "")).isEqualTo("batchAck");
      assertThat(ack.getLong("inserted", -1)).isEqualTo(MAX_ROWS);
      assertThat(ack.getBoolean("replay", false)).as("the refused chunk must not have moved the watermark").isFalse();

      new JSONObject(client.send(control("rollback", sessionId)));
    }
  }

  // ---------------------------------------------------------------------------------------------------------

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

  private static JSONArray names(final int count) {
    final JSONArray records = new JSONArray();
    for (int i = 0; i < count; i++)
      records.put(new JSONObject().put("name", "row-" + i));
    return records;
  }

  private static String chunk(final String sessionId, final long chunkSeq, final JSONArray records) {
    return new JSONObject()
        .put("action", "chunk")
        .put("sessionId", sessionId)
        .put("chunkSeq", chunkSeq)
        .put("records", records)
        .toString();
  }

  private static String control(final String action, final String sessionId) {
    return new JSONObject().put("action", action).put("sessionId", sessionId).toString();
  }
}
