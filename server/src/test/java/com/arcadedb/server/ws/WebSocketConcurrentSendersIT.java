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

import com.arcadedb.database.Database;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.http.ws.WebSocketFrameSender;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7423: one {@code /ws} connection with three independent senders - the change-stream fan-out on the
 * database watcher thread, the insert session's answers on an Undertow worker thread, and the subscription
 * acknowledgement on the I/O thread - and nothing of ours serializing them. Every frame the client receives
 * must still be one complete JSON object; {@link WebSocketFrameSender} records why Undertow guarantees that.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class WebSocketConcurrentSendersIT extends BaseGraphServerTest {
  private static final int CHUNKS          = 40;
  private static final int ROWS_PER_CHUNK  = 5;
  private static final int BACKGROUND_ROWS = 300;

  @Test
  void changeEventsAndBatchAcksInterleaveOnOneConnectionAsWholeFrames() throws Throwable {
    final Database database = getServerDatabase(0, getDatabaseName());
    final int expectedEvents = BACKGROUND_ROWS + CHUNKS * ROWS_PER_CHUNK;

    final String url = "ws://localhost:" + getServer(0).getHttpServer().getPort() + "/ws";
    try (final var client = new WebSocketClientHelper(url, "root", DEFAULT_PASSWORD_FOR_TESTS, expectedEvents + CHUNKS + 16)) {
      final JSONObject subscribed = new JSONObject(client.send(new JSONObject().put("action", "subscribe")
          .put("database", getDatabaseName()).put("type", "Person").toString()));
      assertThat(subscribed.getString("result", "")).isEqualTo("ok");

      final JSONObject started = new JSONObject(client.send(new JSONObject().put("action", "start")
          .put("database", getDatabaseName())
          .put("options", new JSONObject().put("targetType", "Person").put("transactionMode", "per_batch")).toString()));
      assertThat(started.getString("action", "")).isEqualTo("started");
      final String sessionId = started.getString("sessionId");

      // A writer that is not this connection at all, so the watcher thread pushes events while the worker thread
      // answers chunks. The session's own per_batch inserts feed the same watcher, so both senders are busy on
      // this one channel for the whole run.
      final CountDownLatch writerDone = new CountDownLatch(1);
      final AtomicReference<Throwable> writerFailure = new AtomicReference<>();
      final Thread writer = new Thread(() -> {
        try {
          for (int i = 0; i < BACKGROUND_ROWS; i++)
            database.transaction(() -> database.newDocument("Person").set("name", "background").save());
        } catch (final Throwable t) {
          writerFailure.set(t);
        } finally {
          writerDone.countDown();
        }
      }, "issue-7423-writer");
      writer.start();

      // The chunks are pipelined without waiting for their acknowledgements: the point is to have as many frames
      // in flight from as many senders as possible, not to drive the session politely.
      for (int seq = 1; seq <= CHUNKS; seq++) {
        final JSONArray records = new JSONArray();
        for (int r = 0; r < ROWS_PER_CHUNK; r++)
          records.put(new JSONObject().put("name", "chunk-" + seq + "-" + r));
        client.sendWithoutWaiting(new JSONObject().put("action", "chunk").put("sessionId", sessionId)
            .put("chunkSeq", seq).put("records", records).toString());
      }

      assertThat(writerDone.await(60, TimeUnit.SECONDS)).isTrue();
      assertThat(writerFailure.get()).isNull();

      // Every frame received, whatever thread sent it, parses as one complete JSON object with an action or a
      // changeType of its own: a torn or interleaved frame would fail the parse or carry neither.
      final List<JSONObject> acks = new ArrayList<>();
      int events = 0;
      String frame;
      while ((frame = client.popMessage(5_000)) != null) {
        final JSONObject json = new JSONObject(frame);
        if (json.has("changeType")) {
          assertThat(json.getString("changeType", "")).isEqualTo("create");
          assertThat(json.getJSONObject("record").getString("@type", "")).isEqualTo("Person");
          events++;
        } else {
          assertThat(json.getString("action", "")).as(frame).isEqualTo("batchAck");
          assertThat(json.getLong("inserted", -1)).isEqualTo(ROWS_PER_CHUNK);
          acks.add(json);
        }
        if (acks.size() == CHUNKS && events >= expectedEvents)
          break;
      }

      assertThat(acks).hasSize(CHUNKS);
      // At least: a per_batch chunk whose commit is retried on a page conflict with the background writer fires
      // its create events again, so the stream may carry more than the rows that ended up durable.
      assertThat(events).isGreaterThanOrEqualTo(expectedEvents);

      // The loop above stops as soon as the counts match, but a retried chunk's events can still be in flight, so the
      // next frame is not necessarily the commit reply: skip change events until a frame with an action arrives.
      client.sendWithoutWaiting(new JSONObject().put("action", "commit").put("sessionId", sessionId).toString());
      JSONObject committed = null;
      while ((frame = client.popMessage(5_000)) != null) {
        final JSONObject json = new JSONObject(frame);
        if (json.has("action")) {
          committed = json;
          break;
        }
        assertThat(json.has("changeType")).as("only change events may precede the commit reply: " + frame).isTrue();
      }
      assertThat(committed).as("no commit reply arrived").isNotNull();
      assertThat(committed.getString("action", "")).isEqualTo("committed");
      assertThat(committed.getJSONObject("summary").getLong("inserted", -1)).isEqualTo(CHUNKS * ROWS_PER_CHUNK);
    }

    assertThat(database.countType("Person", false)).isEqualTo(expectedEvents);
  }
}
