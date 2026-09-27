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

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #8473: the AI handlers read the external gateway's answer with {@code HttpClient.send}
 * bounded only by the request timeout ({@code HttpRequest.Builder.timeout}), and what that covers depends on the JDK:
 * <ul>
 * <li>on JDK 21-25 it stops at the response HEADERS, so a gateway that answers {@code 200} with a
 * {@code Content-Length} and then stalls inside its body parks an HTTP worker thread of this server with no bound at
 * all;</li>
 * <li>on JDK 26+ it covers the whole body, so on the streamed chat it caps the total length of a conversation that is
 * working.</li>
 * </ul>
 * Each call to the gateway is driven here through its own endpoint, with the handlers' bounds shortened to one second.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue8473AiGatewayStalledAnswerTest extends BaseGraphServerTest {

  private static final long   BUDGET_MS        = 1_000L;
  /** The tripwire between "the bound fired" and "the read is unbounded" (forever, without the fix). */
  private static final long   GAVE_UP_BOUND_MS = 15_000L;
  /** A hang detector, not a latency bound: the client's read timeout on this server's answer. */
  private static final int    HANG_DETECT_MS   = 60_000;
  /** Headers promising 100 bytes, then five of them, then silence. */
  private static final String STALLED_BODY     =
      "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 100\r\n\r\n{\"res";
  private static final String SSE_HEADERS      =
      "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n";

  private final long savedActivate       = AiActivateHandler.gatewayTimeoutMs;
  private final long savedAnalyze        = AiAnalyzeProfilerHandler.gatewayTimeoutMs;
  private final long savedChat           = AiChatHandler.gatewayTimeoutMs;
  private final long savedToolResult     = AiChatHandler.toolResultTimeoutMs;
  private final long savedStreamSilence  = AiChatHandler.streamSilenceMs;
  private ScriptedGateway gateway;

  @BeforeEach
  void shortenTheBounds() {
    AiActivateHandler.gatewayTimeoutMs = BUDGET_MS;
    AiAnalyzeProfilerHandler.gatewayTimeoutMs = BUDGET_MS;
    AiChatHandler.gatewayTimeoutMs = BUDGET_MS;
    AiChatHandler.toolResultTimeoutMs = BUDGET_MS;
    AiChatHandler.streamSilenceMs = BUDGET_MS;
  }

  @AfterEach
  void restore() throws IOException {
    AiActivateHandler.gatewayTimeoutMs = savedActivate;
    AiAnalyzeProfilerHandler.gatewayTimeoutMs = savedAnalyze;
    AiChatHandler.gatewayTimeoutMs = savedChat;
    AiChatHandler.toolResultTimeoutMs = savedToolResult;
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

  // ------------------------------------------------------------------------------------------------------------
  // Buffered answers
  // ------------------------------------------------------------------------------------------------------------

  @Test
  void aChatWhoseGatewayStallsInsideItsBodyIsAnswered504() throws Exception {
    startGateway((path, out) -> write(out, STALLED_BODY));

    final StallAwareStopwatch watch = StallAwareStopwatch.start();
    final Answer answer = post("/chat", chatRequest());
    watch.assertGaveUpWithin(GAVE_UP_BOUND_MS, "a 1s bound on the whole answer from an unbounded body read");

    assertThat(answer.status).isEqualTo(504);
    assertThat(new JSONObject(answer.body).getString("code", "")).isEqualTo("gateway_timeout");
    assertThat(gateway.closedByClient.await(GAVE_UP_BOUND_MS, TimeUnit.MILLISECONDS)).isTrue();
  }

  @Test
  void aProfilerAnalysisWhoseGatewayStallsInsideItsBodyIsAnswered504() throws Exception {
    startGateway((path, out) -> write(out, STALLED_BODY));

    final StallAwareStopwatch watch = StallAwareStopwatch.start();
    final Answer answer = post("/analyze-profiler", new JSONObject().put("profilerData", new JSONObject()));
    watch.assertGaveUpWithin(GAVE_UP_BOUND_MS, "a 1s bound on the whole answer from an unbounded body read");

    assertThat(answer.status).isEqualTo(504);
    assertThat(new JSONObject(answer.body).getString("code", "")).isEqualTo("gateway_timeout");
  }

  @Test
  void anActivationWhoseGatewayStallsInsideItsBodyFails() throws Exception {
    startGateway((path, out) -> write(out, STALLED_BODY));

    final StallAwareStopwatch watch = StallAwareStopwatch.start();
    final Answer answer = post("/activate", new JSONObject().put("subscriptionKey", "key"));
    watch.assertGaveUpWithin(GAVE_UP_BOUND_MS, "a 1s bound on the whole answer from an unbounded body read");

    assertThat(answer.status).isEqualTo(500);
    assertThat(new JSONObject(answer.body).getString("error", "")).startsWith("Activation failed");
  }

  // ------------------------------------------------------------------------------------------------------------
  // The streamed chat
  // ------------------------------------------------------------------------------------------------------------

  @Test
  void aStreamedChatWhoseGatewayStallsMidStreamEnds() throws Exception {
    startGateway((path, out) -> {
      write(out, SSE_HEADERS);
      writeEvent(out, new JSONObject().put("type", "session").put("sessionId", "s-1"));
    });

    final StallAwareStopwatch watch = StallAwareStopwatch.start();
    final Answer answer = post("/chat/stream", chatRequest());
    watch.assertGaveUpWithin(GAVE_UP_BOUND_MS, "a 1s bound on the gateway's silence from an unbounded body read");

    assertThat(answer.status).isEqualTo(200);
    assertThat(answer.body).doesNotContain("\"done\"");
    assertThat(gateway.closedByClient.await(GAVE_UP_BOUND_MS, TimeUnit.MILLISECONDS))
        .as("the stalled gateway's connection is closed, not left open").isTrue();
  }

  /** The request timeout is gone from the streamed chat, so the header wait has to be bounded some other way. */
  @Test
  void aStreamedChatWhoseGatewayNeverAnswersIsAnswered504() throws Exception {
    startGateway((path, out) -> {
      // headers never come
    });

    final StallAwareStopwatch watch = StallAwareStopwatch.start();
    final Answer answer = post("/chat/stream", chatRequest());
    watch.assertGaveUpWithin(GAVE_UP_BOUND_MS, "a 1s bound on the wait for the gateway's headers");

    assertThat(answer.status).isEqualTo(504);
  }

  /** The JDK 26+ half: a conversation that runs for several budgets but is never silent for one is relayed whole. */
  @Test
  void aStreamedChatLongerThanTheBudgetButNeverSilentIsRelayedWhole() throws Exception {
    final int events = 10;
    startGateway((path, out) -> {
      write(out, SSE_HEADERS);
      writeEvent(out, new JSONObject().put("type", "session").put("sessionId", "s-1"));
      for (int i = 0; i < events; i++) {
        sleep(BUDGET_MS / 4);
        writeEvent(out, new JSONObject().put("type", "text").put("n", i));
      }
      writeEvent(out, new JSONObject().put("type", "done").put("response", "all good"));
      write(out, "0\r\n\r\n");
    });

    final Answer answer = post("/chat/stream", chatRequest());

    assertThat(answer.status).isEqualTo(200);
    for (int i = 0; i < events; i++)
      assertThat(answer.body).contains("\"n\":" + i);
    assertThat(answer.body).contains("all good");
  }

  /**
   * The tool result posted back to the gateway mid-stream: a gateway that stalls inside that answer must not park the
   * stream behind it. The chat carries on and relays the events the gateway sends next.
   */
  @Test
  void aToolResultWhoseGatewayStallsInsideItsBodyDoesNotParkTheStream() throws Exception {
    // The stream waits for the tool result to be given up on, so its own silence bound has to outlast that one.
    AiChatHandler.streamSilenceMs = GAVE_UP_BOUND_MS * 2;
    final CountDownLatch toolResultAbandoned = new CountDownLatch(1);
    startGateway((path, out) -> {
      if (path.startsWith("/api/chat/tool_result/")) {
        write(out, STALLED_BODY);
        return;
      }
      write(out, SSE_HEADERS);
      writeEvent(out, new JSONObject().put("type", "session").put("sessionId", "s-1"));
      writeEvent(out, new JSONObject().put("type", "tool_call").put("id", "tc-1").put("name", "get_schema")
          .put("arguments", new JSONObject().put("database", getDatabaseName())));
      if (toolResultAbandoned.await(HANG_DETECT_MS, TimeUnit.MILLISECONDS))
        writeEvent(out, new JSONObject().put("type", "done").put("response", "after the tool"));
      write(out, "0\r\n\r\n");
    });
    gateway.onClosedByClient = toolResultAbandoned::countDown;

    final StallAwareStopwatch watch = StallAwareStopwatch.start();
    final Answer answer = post("/chat/stream", chatRequest());
    watch.assertGaveUpWithin(GAVE_UP_BOUND_MS, "a 1s bound on the tool result's answer from an unbounded body read");

    assertThat(answer.status).isEqualTo(200);
    assertThat(answer.body).contains("tool_end").contains("after the tool");
  }

  // ------------------------------------------------------------------------------------------------------------
  // Fixtures
  // ------------------------------------------------------------------------------------------------------------

  @FunctionalInterface
  private interface Script {
    void answer(String path, OutputStream out) throws Exception;
  }

  private record Answer(int status, String body) {
  }

  private void startGateway(final Script script) throws Exception {
    gateway = new ScriptedGateway(script);
    final AiConfiguration config = getServer(0).getAiConfiguration();
    config.activate("test-token", "127.0.0.1", "hw-test", "test-version");
    // No public setter: the URL is normally persisted by activation. Same injection as AiChatHandlerStreamingTest.
    final var field = AiConfiguration.class.getDeclaredField("gatewayUrl");
    field.setAccessible(true);
    field.set(config, "http://127.0.0.1:" + gateway.port);
  }

  private JSONObject chatRequest() {
    return new JSONObject().put("database", getDatabaseName()).put("message", "What types exist?");
  }

  private Answer post(final String path, final JSONObject body) throws Exception {
    final HttpURLConnection conn = (HttpURLConnection) new URI(
        "http://127.0.0.1:" + getServer(0).getHttpServer().getPort() + "/api/v1/ai" + path).toURL().openConnection();
    try {
      conn.setRequestMethod("POST");
      conn.setRequestProperty("Authorization", "Basic " + Base64.getEncoder()
          .encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)));
      conn.setRequestProperty("Content-Type", "application/json");
      conn.setReadTimeout(HANG_DETECT_MS);
      conn.setDoOutput(true);
      try (final OutputStream out = conn.getOutputStream()) {
        out.write(body.toString().getBytes(StandardCharsets.UTF_8));
      }
      final int status = conn.getResponseCode();
      try (final InputStream in = status < 400 ? conn.getInputStream() : conn.getErrorStream()) {
        return new Answer(status, in == null ? "" : new String(in.readAllBytes(), StandardCharsets.UTF_8));
      }
    } finally {
      conn.disconnect();
    }
  }

  private static void write(final OutputStream out, final String text) throws IOException {
    out.write(text.getBytes(StandardCharsets.UTF_8));
    out.flush();
  }

  private static void writeEvent(final OutputStream out, final JSONObject event) throws IOException {
    final byte[] bytes = ("data: " + event + "\n\n").getBytes(StandardCharsets.UTF_8);
    write(out, Integer.toHexString(bytes.length) + "\r\n");
    out.write(bytes);
    write(out, "\r\n");
  }

  private static void sleep(final long ms) {
    try {
      Thread.sleep(ms);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /**
   * A gateway that reads each request whole, answers it with a script, and then holds the connection open until the
   * client closes it - which it records, so a test can tell a cancelled exchange from an abandoned one.
   */
  private static final class ScriptedGateway implements AutoCloseable {
    final CountDownLatch closedByClient = new CountDownLatch(1);
    final int            port;
    volatile Runnable    onClosedByClient = () -> {
    };
    private final ServerSocket socket;
    private final List<Socket> accepted = new CopyOnWriteArrayList<>();
    private final Script       script;

    ScriptedGateway(final Script script) throws IOException {
      this.script = script;
      this.socket = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
      this.port = socket.getLocalPort();
      final Thread acceptor = new Thread(this::acceptLoop, "issue8473-gateway");
      acceptor.setDaemon(true);
      acceptor.start();
    }

    private void acceptLoop() {
      while (!socket.isClosed()) {
        try {
          final Socket connection = socket.accept();
          accepted.add(connection);
          final Thread handler = new Thread(() -> serve(connection), "issue8473-gateway-connection");
          handler.setDaemon(true);
          handler.start();
        } catch (final IOException e) {
          return;
        }
      }
    }

    private void serve(final Socket connection) {
      try {
        final InputStream in = connection.getInputStream();
        final String path = readRequest(in);
        script.answer(path, connection.getOutputStream());
        // Hold the connection until the client closes it.
        while (in.read() >= 0) {
          // discard
        }
        clientClosed();
      } catch (final SocketException e) {
        clientClosed();
      } catch (final Exception ignored) {
        // The test asserts on the client side
      }
    }

    private void clientClosed() {
      closedByClient.countDown();
      onClosedByClient.run();
    }

    /** Reads the request whole and returns its path, so the answer follows the request. */
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

    @Override
    public void close() throws IOException {
      socket.close();
      for (final Socket s : new ArrayList<>(accepted))
        s.close();
    }
  }
}
