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
package com.arcadedb.mcp;

import com.arcadedb.database.Database;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.security.ServerSecurityUser;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A call whose reply cannot be written out is answered like any other failed call, and the session goes on: the stdio process
 * serves one client for as long as its pipe is open, so a call that ended it would end every call after it.
 * <p>
 * The value that makes the reply unwritable reaches the call in two ways: stored in a record that a query returns, and held in
 * the memory of the dispatcher when a caller hands it a request object it built itself. Both are covered here, on a thread with
 * a small stack so that the value needed stays far below what a record or a request can hold.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class MCPUnwritableReplyTest extends BaseGraphServerTest {
  private static final String TYPE_NAME   = "LayeredValueHolder";
  private static final int    LAYERS      = 4_000;
  private static final long   STACK_BYTES = 192L * 1024;

  private MCPConfiguration   config;
  private ServerSecurityUser user;

  @BeforeEach
  void setupMCP() {
    config = MCPPlugin.of(getServer(0)).getConfiguration();
    config.setEnabled(true);
    config.setAllowReads(true);
    config.setToolProfile(MCPConfiguration.ToolProfile.ALL);
    final JSONObject reset = new JSONObject();
    reset.put("principalProfiles", (Object) null);
    config.updateFrom(reset);
    user = getServer(0).getSecurity().authenticate("root", DEFAULT_PASSWORD_FOR_TESTS, null);
  }

  @Test
  void aToolResultThatCannotBeWrittenIsAnsweredAsAFailedCall() {
    storeLayeredRecord();

    final String[] answers = runStdio(queryLine(1) + "\n" + pingLine(2) + "\n");

    assertThat(answers).as("one answer per request: the failed call and the ping after it").hasSize(2);

    final JSONObject failed = new JSONObject(answers[0]);
    assertThat(failed.opt("id")).isEqualTo(1);
    assertThat(failed.has("result")).isTrue();
    final JSONObject result = failed.getJSONObject("result");
    assertThat(result.getBoolean("isError", false)).isTrue();
    assertThat(result.getJSONArray("content").getJSONObject(0).getString("text")).contains("nested too deeply");

    final JSONObject ping = new JSONObject(answers[1]);
    assertThat(ping.opt("id")).isEqualTo(2);
    assertThat(ping.has("result")).isTrue();
  }

  @Test
  void aBatchKeepsAnsweringAfterAToolResultThatCannotBeWritten() {
    storeLayeredRecord();

    final JSONArray batch = new JSONArray()
        .put(new JSONObject(queryLine(1)))
        .put(new JSONObject(pingLine(2)));
    final String[] answers = runStdio(batch + "\n" + pingLine(3) + "\n");

    assertThat(answers).hasSize(2);

    final JSONArray replies = new JSONArray(answers[0]);
    assertThat(replies.length()).isEqualTo(2);
    assertThat(replies.getJSONObject(0).getJSONObject("result").getBoolean("isError", false)).isTrue();
    assertThat(replies.getJSONObject(1).has("result")).isTrue();

    assertThat(new JSONObject(answers[1]).opt("id")).isEqualTo(3);
  }

  @Test
  void aRequestWithAnOversizedArgumentIsAnsweredWithAnError() {
    // Built in memory: a request read from the pipe or the socket is parsed with a depth limit of its own, so it never gets here
    final JSONObject arguments = new JSONObject()
        .put("database", getDatabaseName())
        .put("language", "sql")
        .put("query", "SELECT 1")
        .put("extra", layeredJson(LAYERS));
    final JSONObject request = new JSONObject()
        .put("jsonrpc", "2.0")
        .put("id", 7)
        .put("method", "tools/call")
        .put("params", new JSONObject().put("name", "query").put("arguments", arguments));

    final AtomicReference<MCPDispatcher.MCPResponse> response = new AtomicReference<>();
    runOnStack(() -> response.set(new MCPDispatcher(getServer(0), config, "stdio").dispatch(request, user, MCPStdioServer.STDIO_CLIENT)),
        STACK_BYTES);

    assertThat(response.get()).isNotNull();
    final JSONObject json = response.get().json();
    assertThat(json.opt("id")).isEqualTo(7);
    assertThat(json.has("error")).isTrue();
    assertThat(json.getJSONObject("error").getInt("code")).isEqualTo(-32603);
  }

  @Test
  void aFailureOfALineDoesNotEndTheLoop() {
    // The line-level guard of the loop: whatever a line makes the writer throw, the next line is still read
    final AtomicBoolean thrown = new AtomicBoolean();
    final ByteArrayOutputStream captured = new ByteArrayOutputStream();
    final PrintStream failingOnce = new PrintStream(captured, true) {
      @Override
      public void println(final String line) {
        if (thrown.compareAndSet(false, true))
          throw new StackOverflowError();
        super.println(line);
      }
    };

    final ByteArrayInputStream in = new ByteArrayInputStream((pingLine(1) + "\n" + pingLine(2) + "\n").getBytes(StandardCharsets.UTF_8));
    final boolean cleanEnd = new MCPStdioServer(getServer(0), config, user, in, failingOnce).run();

    assertThat(cleanEnd).as("the loop reached the end of the input").isTrue();
    final String[] answers = captured.toString(StandardCharsets.UTF_8).trim().split("\n");
    assertThat(answers).hasSize(2);
    assertThat(new JSONObject(answers[0]).getJSONObject("error").getInt("code")).isEqualTo(-32603);
    assertThat(new JSONObject(answers[1]).opt("id")).isEqualTo(2);
  }

  // ---- Helpers ----

  private void storeLayeredRecord() {
    final Database database = getServerDatabase(0, getDatabaseName());
    if (!database.getSchema().existsType(TYPE_NAME))
      database.getSchema().createDocumentType(TYPE_NAME);
    // Built and written on a thread with a stack large enough for it: only the reply is meant to run out
    runOnStack(() -> database.transaction(() -> database.newDocument(TYPE_NAME).set("value", layeredMap(LAYERS)).save()),
        64L * 1024 * 1024);
  }

  private String queryLine(final int id) {
    return new JSONObject()
        .put("jsonrpc", "2.0")
        .put("id", id)
        .put("method", "tools/call")
        .put("params", new JSONObject()
            .put("name", "query")
            .put("arguments", new JSONObject()
                .put("database", getDatabaseName())
                .put("language", "sql")
                .put("query", "SELECT FROM " + TYPE_NAME)))
        .toString();
  }

  private static String pingLine(final int id) {
    return new JSONObject().put("jsonrpc", "2.0").put("id", id).put("method", "ping").put("params", new JSONObject()).toString();
  }

  /** Runs the loop over {@code input} on the small stack and returns the lines it wrote. */
  private String[] runStdio(final String input) {
    final ByteArrayInputStream in = new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8));
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    final AtomicBoolean cleanEnd = new AtomicBoolean();

    runOnStack(() -> cleanEnd.set(new MCPStdioServer(getServer(0), config, user, in, new PrintStream(out, true)).run()), STACK_BYTES);

    assertThat(cleanEnd.get()).as("the loop ran to the end of its input").isTrue();
    return out.toString(StandardCharsets.UTF_8).trim().split("\n");
  }

  private static void runOnStack(final Runnable task, final long stackBytes) {
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final Thread thread = new Thread(null, () -> {
      try {
        task.run();
      } catch (final Throwable t) {
        failure.set(t);
      }
    }, "sized-stack-test", stackBytes);
    thread.start();
    try {
      thread.join();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
    if (failure.get() instanceof Error error)
      throw error;
    if (failure.get() != null)
      throw new IllegalStateException(failure.get());
  }

  private static Map<String, Object> layeredMap(final int layers) {
    Map<String, Object> value = new HashMap<>();
    value.put("leaf", 1);
    for (int i = 0; i < layers; i++) {
      final Map<String, Object> wrapper = new HashMap<>();
      wrapper.put("n", value);
      value = wrapper;
    }
    return value;
  }

  private static JSONObject layeredJson(final int layers) {
    JSONObject value = new JSONObject().put("leaf", 1);
    for (int i = 0; i < layers; i++)
      value = new JSONObject().put("n", value);
    return value;
  }
}
