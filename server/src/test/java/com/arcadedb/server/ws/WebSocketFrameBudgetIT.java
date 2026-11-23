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
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The per-connection frame budgets of {@code /ws}: the text-frame size caps, the larger insert budget that only an
 * accepted {@code start} earns, and the same bound on binary frames. Each fixture sets its own caps, so each source
 * class is kept whole as a nested class with its own server configuration.
 */
class WebSocketFrameBudgetIT {

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
  @Nested
  class FrameSizeCaps extends BaseGraphServerTest {
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
      Awaitility.await().atMost(Duration.ofSeconds(10))
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
   * Measured the way {@code FrameSizeCaps} measures the caps, because that is the only thing an
   * operator can observe: a frame between the two caps is accepted when the larger budget is in force and closes
   * the connection with a 1009 when it is not. The caps are set absurdly low here so the test exercises the
   * refusal rather than the heap.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class RefusedStartDoesNotRaiseTheBudget extends BaseGraphServerTest {
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
     * A client that pipelines its first {@code chunk} behind {@code start}, without waiting for {@code started},
     * is served - which is the behaviour an operator cares about and the reason the budget has two halves at all.
     * <p>
     * It does NOT, on its own, prove the in-flight-{@code start} half: over a real socket the worker may have
     * registered the session before the chunk's size is decided, in which case the session half admits the frame
     * (CodeRabbit on PR #7936). That half is pinned deterministically, by holding the worker, in
     * {@code InsertSessionStartTest}. Kept here because the end-to-end path is worth
     * exercising whichever half answers.
     */
    @Test
    void aChunkPipelinedBehindAStartIsServed() throws Throwable {
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
  @Nested
  class BinaryFrameBudget extends BaseGraphServerTest {
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
}
