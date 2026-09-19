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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7909: the larger {@code /ws} text-frame budget was granted from a frame's {@code action} string alone,
 * on the I/O thread, before anything had looked at the frame - and lowered only by a later frame whose action was
 * literally {@code commit} or {@code rollback}.
 * <p>
 * So one {@code {"action":"start"}} frame the server REFUSED - an unknown database, a principal without access,
 * a session id already taken, unparseable options, a dead external transaction, or a frame simply dropped
 * because the in-flight queue was full - multiplied that connection's per-frame heap budget by 256 for the rest
 * of its life. {@code SERVER_WS_MAX_INSERT_FRAME_SIZE} documents the opposite: "a connection that never opens
 * an insert session is never charged more than 'wsMaxControlFrameSize'".
 * <p>
 * Measured the way {@code Issue7403FrameSizeCapsIT} measures the caps, because that is the only thing an
 * operator can observe: a frame between the two caps is accepted when the larger budget is in force and closes
 * the connection with a 1009 when it is not. The caps are set absurdly low here so the test exercises the
 * refusal rather than the heap.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7909RefusedStartDoesNotRaiseTheFrameBudgetIT extends BaseGraphServerTest {
  private static final int CONTROL_CAP = 2 * 1024;
  private static final int INSERT_CAP  = 256 * 1024;

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    config.setValue(GlobalConfiguration.SERVER_WS_MAX_CONTROL_FRAME_SIZE, (long) CONTROL_CAP);
    config.setValue(GlobalConfiguration.SERVER_WS_MAX_INSERT_FRAME_SIZE, (long) INSERT_CAP);
  }

  /** The report's own case: a {@code start} naming a database that does not exist. */
  @Test
  void aStartRefusedForAnUnknownDatabaseLeavesTheConnectionOnTheControlBudget() throws Throwable {
    try (final var client = new WebSocketClientHelper(wsUrl(), "root", DEFAULT_PASSWORD_FOR_TESTS)) {
      final JSONObject refused = new JSONObject(client.send(start("no-such-database-7909", null)));
      assertThat(refused.getString("result", "")).isEqualTo("error");

      assertThat(client.send(betweenTheCaps()))
          .as("a refused start must not have bought the connection the insert budget").isNull();
    }
  }

  /**
   * A refusal from a different stage: the options are parsed on the worker, well after the budget used to be
   * raised, so an {@code update} conflict mode with nothing to match on is refused there.
   */
  @Test
  void aStartRefusedForUnusableOptionsLeavesTheConnectionOnTheControlBudget() throws Throwable {
    try (final var client = new WebSocketClientHelper(wsUrl(), "root", DEFAULT_PASSWORD_FOR_TESTS)) {
      final JSONObject refused = new JSONObject(
          client.send(start(getDatabaseName(), new JSONObject().put("conflictMode", "update"))));
      assertThat(refused.getString("result", "")).isEqualTo("error");
      assertThat(refused.getString("detail", "")).contains("keyColumns");

      assertThat(client.send(betweenTheCaps()))
          .as("a refused start must not have bought the connection the insert budget").isNull();
    }
  }

  /**
   * The control that keeps the two above meaningful: a start the server ACCEPTS does raise the budget, the
   * connection keeps it while its session is open, and it is charged the control budget again once the session
   * has ended. Without this, "refuse every large frame" would pass.
   */
  @Test
  void anAcceptedStartRaisesTheBudgetAndEndingTheSessionLowersItAgain() throws Throwable {
    try (final var client = new WebSocketClientHelper(wsUrl(), "root", DEFAULT_PASSWORD_FOR_TESTS)) {
      final JSONObject started = new JSONObject(client.send(start(getDatabaseName(), personOptions())));
      assertThat(started.getString("action", "")).isEqualTo("started");
      final String sessionId = started.getString("sessionId");

      final JSONObject ack = new JSONObject(client.send(bigChunk(sessionId, 1)));
      assertThat(ack.getString("action", "")).as("a chunk between the two caps must be accepted").isEqualTo("batchAck");

      final JSONObject committed = new JSONObject(client.send(control("rollback", sessionId)));
      assertThat(committed.getString("action", "")).isEqualTo("committed");

      assertThat(client.send(betweenTheCaps()))
          .as("the session has ended, so the connection is back on the control budget").isNull();
    }
  }

  /**
   * The reason the in-flight-{@code start} half of the answer exists at all, and the case a budget keyed only on
   * a registered session would lose (claude-review on PR #7936). The client pipelines its first {@code chunk}
   * behind {@code start} without waiting for {@code started}, so the chunk's size is decided on the I/O thread
   * while the start is still on a worker - or has only just finished. Both orderings must grant the budget: the
   * grant is taken before the frame is queued and released only once the session is registered, so there is no
   * instant between them at which the connection is charged the control budget.
   */
  @Test
  void aChunkPipelinedBehindAStartIsAlreadyOnTheInsertBudget() throws Throwable {
    try (final var client = new WebSocketClientHelper(wsUrl(), "root", DEFAULT_PASSWORD_FOR_TESTS)) {
      final String sessionId = "pipelined-7909";
      final JSONObject options = personOptions();

      // Neither send waits for its answer, so the chunk reaches the I/O thread while the start is still queued.
      client.sendWithoutWaiting(new JSONObject().put("action", "start").put("database", getDatabaseName())
          .put("sessionId", sessionId).put("options", options).toString());
      client.sendWithoutWaiting(bigChunk(sessionId, 1));

      final JSONObject started = new JSONObject(client.popMessage());
      assertThat(started.getString("action", "")).isEqualTo("started");

      final JSONObject ack = new JSONObject(client.popMessage());
      assertThat(ack.getString("action", "")).as("the pipelined chunk must not have been refused for its size: %s", ack)
          .isEqualTo("batchAck");
      assertThat(ack.getLong("inserted", -1)).isEqualTo(1);

      new JSONObject(client.send(control("rollback", sessionId)));
    }
  }

  // ---------------------------------------------------------------------------------------------------------

  private String wsUrl() {
    return "ws://localhost:" + getServer(0).getHttpServer().getPort() + "/ws";
  }

  /**
   * A frame comfortably over the control cap and comfortably under the insert one, so which of the two is in
   * force decides whether it is answered at all. A {@code subscribe} rather than a {@code chunk}: it needs no
   * session, which is the state every test here is asserting about.
   */
  private String betweenTheCaps() {
    return new JSONObject().put("action", "subscribe").put("database", getDatabaseName())
        .put("type", "V".repeat(CONTROL_CAP * 8)).toString();
  }

  private String start(final String databaseName, final JSONObject options) {
    final JSONObject message = new JSONObject().put("action", "start").put("database", databaseName);
    if (options != null)
      message.put("options", options);
    return message.toString();
  }

  private static JSONObject personOptions() {
    return new JSONObject().put("targetType", "Person");
  }

  private static String bigChunk(final String sessionId, final long chunkSeq) {
    final JSONArray records = new JSONArray();
    records.put(new JSONObject().put("name", "x".repeat(CONTROL_CAP * 8)));
    return new JSONObject().put("action", "chunk").put("sessionId", sessionId).put("chunkSeq", chunkSeq)
        .put("records", records).toString();
  }

  private static String control(final String action, final String sessionId) {
    return new JSONObject().put("action", action).put("sessionId", sessionId).toString();
  }
}
