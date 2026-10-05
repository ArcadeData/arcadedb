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
package com.arcadedb.server.ai;

import com.arcadedb.log.DefaultLogger;
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #8642: when the AI gateway's stream broke AFTER the streamed chat had already answered
 * Studio {@code 200} and started relaying events, the handler returned a 503/504 {@code ExecutionResponse} anyway.
 * Sending it set a status code on a response already on the wire, so every dropped gateway connection logged
 * {@code IllegalStateException: UT000002: The response has already been started} at WARNING with a full stack trace
 * (which read as a crash to the reporter), and Studio got a stream that simply stopped, with no reason given.
 * <p>
 * A failure once the stream has started can only be reported inside it: the stream now ends with an {@code error}
 * event naming what happened, and nothing tries to send a second response.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue8642AiChatStreamInterruptedTest extends BaseGraphServerTest {

  /** A hang detector, not a latency bound: the client's read timeout on this server's answer. */
  private static final int    HANG_DETECT_MS = 60_000;
  private static final String SSE_HEADERS    =
      "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n";

  private final long            savedStreamSilence = AiChatHandler.streamSilenceMs;
  private final List<Throwable> loggedThrowables   = new CopyOnWriteArrayList<>();
  private final List<String>    loggedWarnings     = new CopyOnWriteArrayList<>();
  /**
   * Opened by the client once this server has relayed its first frame. A gateway script that drops the connection
   * waits on it first: the JDK client this server reads the gateway with discards bytes it has received but not yet
   * handed to the reader as soon as it learns the connection broke, so a drop racing the relay could swallow the very
   * event the test expects to see relayed (seen on CI: the tool_call frame vanished, and neither tool_start nor tool_end
   * was ever written).
   */
  private final CountDownLatch  firstFrameRelayed  = new CountDownLatch(1);
  private       ServerSocket    gateway;

  @BeforeEach
  void captureLogs() {
    final Logger delegate = new DefaultLogger();
    LogManager.instance().setLogger(new Logger() {
      private void record(final Level level, final String message, final Throwable throwable) {
        if (throwable != null)
          loggedThrowables.add(throwable);
        if (message != null && level.intValue() >= Level.WARNING.intValue())
          loggedWarnings.add(message);
      }

      @Override
      public void log(final Object requester, final Level level, final String message, final Throwable throwable,
          final String context, final Object arg1, final Object arg2, final Object arg3, final Object arg4,
          final Object arg5, final Object arg6, final Object arg7, final Object arg8, final Object arg9,
          final Object arg10, final Object arg11, final Object arg12, final Object arg13, final Object arg14,
          final Object arg15, final Object arg16, final Object arg17) {
        record(level, message, throwable);
        delegate.log(requester, level, message, throwable, context, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9,
            arg10, arg11, arg12, arg13, arg14, arg15, arg16, arg17);
      }

      @Override
      public void log(final Object requester, final Level level, final String message, final Throwable throwable,
          final String context, final Object... args) {
        record(level, message, throwable);
        delegate.log(requester, level, message, throwable, context, args);
      }

      @Override
      public void flush() {
        delegate.flush();
      }
    });
  }

  @AfterEach
  void restore() throws IOException {
    // The sanctioned restore (see DefaultLogger): a fresh DefaultLogger, not the instance replaced above.
    LogManager.instance().setLogger(new DefaultLogger());
    AiChatHandler.streamSilenceMs = savedStreamSilence;
    if (gateway != null)
      gateway.close();
    // Same clean-up as AiChatHandlerStreamingTest: the chats directory is shared by every test in this module.
    final File[] chats = new File("./target/chats/" + ChatStorage.hashUsername("root")).listFiles();
    if (chats != null)
      for (final File f : chats)
        //noinspection ResultOfMethodCallIgnored
        f.delete();
  }

  /** The reporter's case: the gateway's connection is closed under the relay ("AI gateway I/O error: closed"). */
  @Test
  void aGatewayThatDropsMidStreamEndsTheStreamWithAnErrorEvent() throws Exception {
    startGateway((in, out) -> {
      write(out, SSE_HEADERS);
      writeEvent(out, new JSONObject().put("type", "session").put("sessionId", "s-1"));
      writeEvent(out, new JSONObject().put("type", "text").put("n", 1));
      awaitFirstRelayedFrame();
      // No terminating chunk: the connection is dropped under the relay.
    });

    final List<JSONObject> events = streamedChat();

    assertThat(events).as("the event relayed before the drop still reaches the caller")
        .anyMatch(e -> e.getInt("n", 0) == 1);
    assertThat(events).noneMatch(e -> "done".equals(e.getString("type", "")));
    final JSONObject last = events.get(events.size() - 1);
    assertThat(last.getString("type", "")).as("the stream says why it ended, instead of just stopping").isEqualTo("error");
    assertThat(last.getString("code", "")).isEqualTo("gateway_interrupted");
    assertThat(last.getString("error", "")).isNotBlank();

    assertNoSecondResponseWasAttempted();
  }

  /** The silence bound of issue #8473 fires mid-stream: the same shape, reported as a timeout. */
  @Test
  void aGatewayThatFallsSilentMidStreamEndsTheStreamWithATimeoutEvent() throws Exception {
    AiChatHandler.streamSilenceMs = 1_000L;
    startGateway((in, out) -> {
      write(out, SSE_HEADERS);
      writeEvent(out, new JSONObject().put("type", "session").put("sessionId", "s-1"));
      // then silence, with the connection held open until this server gives up on it and closes it
      while (in.read() >= 0) {
        // discard
      }
    });

    final List<JSONObject> events = streamedChat();

    assertThat(events).noneMatch(e -> "done".equals(e.getString("type", "")));
    final JSONObject last = events.get(events.size() - 1);
    assertThat(last.getString("type", "")).isEqualTo("error");
    assertThat(last.getString("code", "")).isEqualTo("gateway_timeout");

    assertNoSecondResponseWasAttempted();
  }

  /** A drop right after a tool ran: Studio has already drawn the tool, and still needs to be told the answer is lost. */
  @Test
  void aGatewayThatDropsAfterAToolCallEndsTheStreamWithAnErrorEvent() throws Exception {
    startGateway((in, out) -> {
      write(out, SSE_HEADERS);
      writeEvent(out, new JSONObject().put("type", "session").put("sessionId", "s-1"));
      writeEvent(out, new JSONObject().put("type", "tool_call").put("id", "tc-1").put("name", "get_schema")
          .put("arguments", new JSONObject().put("database", getDatabaseName())));
      // tool_start is relayed once the tool_call line has been read, so the tool runs and tool_end follows whatever
      // the drop does to the bytes after it.
      awaitFirstRelayedFrame();
    });

    final List<JSONObject> events = streamedChat();

    assertThat(events).anyMatch(e -> "tool_end".equals(e.getString("type", "")));
    final JSONObject last = events.get(events.size() - 1);
    assertThat(last.getString("type", "")).isEqualTo("error");
    assertThat(last.getString("code", "")).isEqualTo("gateway_interrupted");

    assertNoSecondResponseWasAttempted();
  }

  /** A gateway, or a proxy in front of it, that ends the stream properly but never sent 'done'. */
  @Test
  void aGatewayThatClosesCleanlyWithoutDoneEndsTheStreamWithAnErrorEvent() throws Exception {
    startGateway((in, out) -> {
      write(out, SSE_HEADERS);
      writeEvent(out, new JSONObject().put("type", "session").put("sessionId", "s-1"));
      writeEvent(out, new JSONObject().put("type", "text").put("n", 1));
      write(out, "0\r\n\r\n");
    });

    final List<JSONObject> events = streamedChat();

    assertThat(events).noneMatch(e -> "done".equals(e.getString("type", "")));
    final JSONObject last = events.get(events.size() - 1);
    assertThat(last.getString("type", "")).isEqualTo("error");
    assertThat(last.getString("code", "")).isEqualTo("gateway_interrupted");

    assertNoSecondResponseWasAttempted();
  }

  /** A drop AFTER 'done': the answer was delivered, so nothing may follow it. */
  @Test
  void aGatewayThatDropsAfterDoneAddsNoErrorEvent() throws Exception {
    startGateway((in, out) -> {
      write(out, SSE_HEADERS);
      writeEvent(out, new JSONObject().put("type", "session").put("sessionId", "s-1"));
      writeEvent(out, new JSONObject().put("type", "done").put("response", "all good"));
      awaitFirstRelayedFrame();
      // No terminating chunk: the connection is dropped right after the answer.
    });

    final List<JSONObject> events = streamedChat();

    assertThat(events.get(events.size() - 1).getString("type", "")).isEqualTo("done");
    assertThat(events).noneMatch(e -> "error".equals(e.getString("type", "")));
    assertNoSecondResponseWasAttempted();
  }

  /** The gateway reports its own failure as an 'error' event and closes: relayed once, with no second 'error'. */
  @Test
  void aGatewayErrorEventIsTheOnlyErrorEvent() throws Exception {
    startGateway((in, out) -> {
      write(out, SSE_HEADERS);
      writeEvent(out, new JSONObject().put("type", "session").put("sessionId", "s-1"));
      writeEvent(out, new JSONObject().put("type", "error").put("error", "model overloaded"));
      write(out, "0\r\n\r\n");
    });

    final List<JSONObject> events = streamedChat();

    assertThat(events.stream().filter(e -> "error".equals(e.getString("type", ""))).toList())
        .singleElement()
        .satisfies(e -> assertThat(e.getString("error", "")).isEqualTo("model overloaded"));
    assertNoSecondResponseWasAttempted();
  }

  /** A complete stream carries no 'error' event. */
  @Test
  void aCompleteStreamEndsWithDoneAndNoErrorEvent() throws Exception {
    startGateway((in, out) -> {
      write(out, SSE_HEADERS);
      writeEvent(out, new JSONObject().put("type", "session").put("sessionId", "s-1"));
      writeEvent(out, new JSONObject().put("type", "done").put("response", "all good"));
      write(out, "0\r\n\r\n");
    });

    final List<JSONObject> events = streamedChat();

    assertThat(events.get(events.size() - 1).getString("type", "")).isEqualTo("done");
    assertThat(events).noneMatch(e -> "error".equals(e.getString("type", "")));
  }

  /**
   * The rest of the issue: a CDN in front of either hop drops a connection that carries no bytes for about 100
   * seconds, and an LLM working on a long answer can be silent for longer. A heartbeat is an SSE comment line; the relay
   * used to drop every line that was not {@code data:}, so a heartbeat kept the gateway's hop alive but never reached
   * the client's.
   */
  @Test
  void aHeartbeatCommentFromTheGatewayIsRelayedToTheClient() throws Exception {
    startGateway((in, out) -> {
      write(out, SSE_HEADERS);
      writeEvent(out, new JSONObject().put("type", "session").put("sessionId", "s-1"));
      writeChunk(out, ": ping from the gateway\n\n");
      writeEvent(out, new JSONObject().put("type", "done").put("response", "all good"));
      write(out, "0\r\n\r\n");
    });

    final String body = streamedChatBody();

    assertThat(body).as("a fixed heartbeat frame, not the gateway's comment text").contains(": keepalive\n\n")
        .doesNotContain("ping from the gateway");
    assertThat(body).contains("all good");
    assertThat(body.indexOf(": keepalive")).as("relayed where it arrived, not after the answer")
        .isLessThan(body.indexOf("all good"));
  }

  private void assertNoSecondResponseWasAttempted() {
    assertThat(loggedThrowables)
        .as("no second response is sent on a stream that has already started (UT000002)")
        .noneMatch(t -> t instanceof IllegalStateException);
    assertThat(loggedWarnings)
        .as("the failure is not reported as a failed command")
        .noneMatch(m -> m.startsWith("Error on command execution"));
  }

  // ------------------------------------------------------------------------------------------------------------
  // Fixtures
  // ------------------------------------------------------------------------------------------------------------

  @FunctionalInterface
  private interface Script {
    void answer(InputStream in, OutputStream out) throws Exception;
  }

  /**
   * A gateway that answers the chat request with the script and then CLOSES the connection, which is what a proxy
   * in front of the real gateway does when it gives up on a long answer. Tool results posted back are acknowledged.
   */
  private void startGateway(final Script script) throws Exception {
    gateway = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
    final Thread acceptor = new Thread(() -> {
      while (!gateway.isClosed()) {
        try {
          final Socket connection = gateway.accept();
          final Thread handler = new Thread(() -> {
            try (connection) {
              final InputStream in = connection.getInputStream();
              final String path = readRequest(in);
              final OutputStream out = connection.getOutputStream();
              if (path.startsWith("/api/chat/tool_result/"))
                write(out, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 11\r\n\r\n{\"ok\":true}");
              else
                script.answer(in, out);
            } catch (final Exception ignored) {
              // The test asserts on the client side
            }
          }, "issue8642-gateway-connection");
          handler.setDaemon(true);
          handler.start();
        } catch (final IOException e) {
          return;
        }
      }
    }, "issue8642-gateway");
    acceptor.setDaemon(true);
    acceptor.start();

    final AiConfiguration config = getServer(0).getAiConfiguration();
    config.activate("test-token", "127.0.0.1", "hw-test", "test-version");
    // No public setter: the URL is normally persisted by activation. Same injection as AiChatHandlerStreamingTest.
    final var field = AiConfiguration.class.getDeclaredField("gatewayUrl");
    field.setAccessible(true);
    field.set(config, "http://127.0.0.1:" + gateway.getLocalPort());
  }

  private List<JSONObject> streamedChat() throws Exception {
    final String body = streamedChatBody();
    final List<JSONObject> events = new ArrayList<>();
    for (final String frame : body.split("\n\n")) {
      final String trimmed = frame.trim();
      if (trimmed.startsWith("data: "))
        events.add(new JSONObject(trimmed.substring(6)));
    }
    assertThat(events).as("stream body: %s", body).isNotEmpty();
    return events;
  }

  private String streamedChatBody() throws Exception {
    final HttpURLConnection conn = (HttpURLConnection) new URI(getServerHttpUrl(0, "/api/v1/ai/chat/stream")).toURL()
        .openConnection();
    try {
      conn.setRequestMethod("POST");
      conn.setRequestProperty("Authorization", "Basic " + Base64.getEncoder()
          .encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)));
      conn.setRequestProperty("Content-Type", "application/json");
      conn.setReadTimeout(HANG_DETECT_MS);
      conn.setDoOutput(true);
      try (final OutputStream out = conn.getOutputStream()) {
        out.write(new JSONObject().put("database", getDatabaseName()).put("message", "What types exist?").toString()
            .getBytes(StandardCharsets.UTF_8));
      }
      assertThat(conn.getResponseCode()).isEqualTo(200);
      try (final InputStream in = conn.getInputStream()) {
        final ByteArrayOutputStream body = new ByteArrayOutputStream();
        final byte[] buffer = new byte[8192];
        int read;
        while ((read = in.read(buffer)) >= 0) {
          body.write(buffer, 0, read);
          if (firstFrameRelayed.getCount() > 0 && body.toString(StandardCharsets.UTF_8).contains("data: "))
            firstFrameRelayed.countDown();
        }
        return body.toString(StandardCharsets.UTF_8);
      }
    } finally {
      conn.disconnect();
    }
  }

  /**
   * Holds a drop back until the client has the first relayed frame. Bounded by the hang detector, so a relay that
   * never writes still ends: the drop happens and the assertions say what was missing.
   */
  private void awaitFirstRelayedFrame() throws InterruptedException {
    if (!firstFrameRelayed.await(HANG_DETECT_MS, TimeUnit.MILLISECONDS))
      // the drop goes ahead; this line is what tells a failing assertion below apart from a relay that never wrote
      LogManager.instance().log(this, Level.WARNING, "No relayed frame reached the client within %d ms: dropping anyway",
          HANG_DETECT_MS);
  }

  private static void write(final OutputStream out, final String text) throws IOException {
    out.write(text.getBytes(StandardCharsets.UTF_8));
    out.flush();
  }

  private static void writeEvent(final OutputStream out, final JSONObject event) throws IOException {
    writeChunk(out, "data: " + event + "\n\n");
  }

  private static void writeChunk(final OutputStream out, final String text) throws IOException {
    final byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
    write(out, Integer.toHexString(bytes.length) + "\r\n");
    out.write(bytes);
    write(out, "\r\n");
  }

  /** Reads the request whole and returns its path. */
  private static String readRequest(final InputStream in) throws IOException {
    final StringBuilder headers = new StringBuilder();
    while (!headers.toString().endsWith("\r\n\r\n")) {
      final int b = in.read();
      if (b < 0)
        return "";
      headers.append((char) b);
    }
    final String[] lines = headers.toString().split("\r\n");
    long length = 0;
    for (final String line : lines)
      if (line.toLowerCase(Locale.ROOT).startsWith("content-length:"))
        length = Long.parseLong(line.substring("content-length:".length()).trim());
    for (long i = 0; i < length; i++)
      if (in.read() < 0)
        break;
    final String[] requestLine = lines[0].split(" ");
    return requestLine.length > 1 ? requestLine[1] : "";
  }
}
