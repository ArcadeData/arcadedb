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
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #9627: every answer the AI Assistant read from its gateway was buffered whole with no byte cap. The
 * streamed chat read each SSE line with {@code BufferedReader.readLine()} (the silence bound ends a stream that goes quiet, not
 * one that keeps sending), the two refusal bodies of the streamed chat with {@code readAllBytes()}, and the four buffered calls
 * with {@code BodyHandlers.ofString()} (the deadline bounds the time, not the bytes).
 * <p>
 * Each test points one of those reads at a gateway that sends {@link #FLOOD} bytes, with the caps shortened, and asserts what the
 * server answered AND that the gateway could not deliver most of the flood: the reader stopped and closed the connection instead
 * of taking it all onto the heap. The flood is finite, so a server without the caps buffers it and fails the assertions rather
 * than the JVM.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue9627AiGatewayUnboundedReadTest extends BaseGraphServerTest {

  private static final long   FLOOD          = 48L * 1024 * 1024;
  private static final int    CAP            = 64 * 1024;
  private static final int    HANG_DETECT_MS = 60_000;
  private static final String SSE_HEADERS    =
      "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n";

  private final long savedMaxResponse = AiChatHandler.maxResponseBytes;
  private final int  savedMaxEvent    = AiPortalChat.maxEventChars;
  private ScriptedGateway gateway;

  @BeforeEach
  void shortenTheCaps() {
    AiChatHandler.maxResponseBytes = CAP;
    AiPortalChat.maxEventChars = CAP;
  }

  @AfterEach
  void restore() throws IOException {
    AiChatHandler.maxResponseBytes = savedMaxResponse;
    AiPortalChat.maxEventChars = savedMaxEvent;
    if (gateway != null)
      gateway.close();
    final File[] chats = new File("./target/chats/" + ChatStorage.hashUsername("root")).listFiles();
    if (chats != null)
      for (final File f : chats)
        //noinspection ResultOfMethodCallIgnored
        f.delete();
  }

  // ------------------------------------------------------------------------------------------------------------
  // The streamed chat
  // ------------------------------------------------------------------------------------------------------------

  /** The issue's main shape: an SSE line that never ends. */
  @Test
  void aStreamedChatLineLongerThanTheCapEndsTheStream() throws Exception {
    startGateway((path, out, sent) -> {
      write(out, SSE_HEADERS);
      writeChunk(out, ("data: " + new JSONObject().put("type", "session").put("sessionId", "s-1") + "\n\n").getBytes(
          StandardCharsets.UTF_8));
      writeChunk(out, "data: ".getBytes(StandardCharsets.UTF_8));
      flood(out, sent, true);
      writeChunk(out, ("\n\ndata: " + new JSONObject().put("type", "done").put("response", "never") + "\n\n").getBytes(
          StandardCharsets.UTF_8));
      write(out, "0\r\n\r\n");
    });

    final Answer answer = post("/chat/stream", chatRequest());

    assertThat(answer.status).isEqualTo(200);
    assertThat(answer.body).doesNotContain("never").contains("error");
    assertThat(answer.body.length()).isLessThan(CAP * 4);
    assertThat(gateway.floodSent("/api/chat")).isLessThan(FLOOD / 2);
  }

  @Test
  void aStreamedChatRefusedWithAnEndlessBodyReadsOnlyItsStart() throws Exception {
    startGateway((path, out, sent) -> {
      write(out, "HTTP/1.1 500 Internal Server Error\r\nContent-Type: text/plain\r\nContent-Length: " + FLOOD + "\r\n\r\n");
      flood(out, sent, false);
    });

    final Answer answer = post("/chat/stream", chatRequest());

    assertThat(answer.status).isEqualTo(500);
    assertThat(gateway.floodSent("/api/chat")).isLessThan(FLOOD / 2);
  }

  @Test
  void aStreamedChatUnauthorizedWithAnEndlessBodyReadsOnlyItsStart() throws Exception {
    startGateway((path, out, sent) -> {
      write(out, "HTTP/1.1 401 Unauthorized\r\nContent-Type: application/json\r\nContent-Length: " + FLOOD + "\r\n\r\n");
      flood(out, sent, false);
    });

    final Answer answer = post("/chat/stream", chatRequest());

    // The body cut at the cap is no JSON: the refusal still reads as a token problem, with the default code
    assertThat(answer.status).isEqualTo(502);
    assertThat(new JSONObject(answer.body).getString("code", "")).isEqualTo("token_invalid");
    assertThat(gateway.floodSent("/api/chat")).isLessThan(FLOOD / 2);
  }

  /** The tool result posted back mid-stream is a buffered call too. */
  @Test
  void aToolResultAnsweredWithAnEndlessBodyIsCapped() throws Exception {
    startGateway((path, out, sent) -> {
      if (path.startsWith("/api/chat/tool_result/")) {
        write(out, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: " + FLOOD + "\r\n\r\n");
        flood(out, sent, false);
        return;
      }
      write(out, SSE_HEADERS);
      writeChunk(out, ("data: " + new JSONObject().put("type", "session").put("sessionId", "s-1") + "\n\n").getBytes(
          StandardCharsets.UTF_8));
      writeChunk(out, ("data: " + new JSONObject().put("type", "tool_call").put("id", "tc-1").put("name", "get_schema")
          .put("arguments", new JSONObject().put("database", getDatabaseName())) + "\n\n").getBytes(StandardCharsets.UTF_8));
      gateway.awaitFloodDone("/api/chat/tool_result/s-1");
      writeChunk(out, ("data: " + new JSONObject().put("type", "done").put("response", "after the tool") + "\n\n").getBytes(
          StandardCharsets.UTF_8));
      write(out, "0\r\n\r\n");
    });

    final Answer answer = post("/chat/stream", chatRequest());

    assertThat(answer.status).isEqualTo(200);
    assertThat(answer.body).contains("tool_end").contains("after the tool");
    assertThat(gateway.floodSent("/api/chat/tool_result/s-1")).isLessThan(FLOOD / 2);
  }

  // ------------------------------------------------------------------------------------------------------------
  // Buffered answers
  // ------------------------------------------------------------------------------------------------------------

  @Test
  void aBufferedChatAnswerLargerThanTheCapIsRefused() throws Exception {
    startGateway((path, out, sent) -> {
      write(out, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: " + FLOOD + "\r\n\r\n");
      flood(out, sent, false);
    });

    final Answer answer = post("/chat", chatRequest());

    assertThat(answer.status).isEqualTo(503);
    assertThat(gateway.floodSent("/api/chat")).isLessThan(FLOOD / 2);
  }

  @Test
  void aProfilerAnalysisAnswerLargerThanTheCapIsRefused() throws Exception {
    startGateway((path, out, sent) -> {
      write(out, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: " + FLOOD + "\r\n\r\n");
      flood(out, sent, false);
    });

    final Answer answer = post("/analyze-profiler", new JSONObject().put("profilerData", new JSONObject()));

    assertThat(answer.status).isEqualTo(503);
    assertThat(gateway.floodSent("/api/analyze-profiler")).isLessThan(FLOOD / 2);
  }

  @Test
  void anActivationAnswerLargerThanTheCapIsRefused() throws Exception {
    startGateway((path, out, sent) -> {
      write(out, "HTTP/1.1 400 Bad Request\r\nContent-Type: application/json\r\nContent-Length: " + FLOOD + "\r\n\r\n");
      flood(out, sent, false);
    });

    final Answer answer = post("/activate", new JSONObject().put("subscriptionKey", "key"));

    assertThat(answer.status).isEqualTo(500);
    assertThat(new JSONObject(answer.body).getString("error", "")).startsWith("Activation failed").contains("KB");
    assertThat(gateway.floodSent("/api/activate")).isLessThan(FLOOD / 2);
  }

  @Test
  void aBufferedChatUnauthorizedWithANonJsonBodyKeepsTheTokenCode() throws Exception {
    startGateway((path, out, sent) -> {
      write(out, "HTTP/1.1 401 Unauthorized\r\nContent-Type: text/plain\r\nContent-Length: 4\r\n\r\nnope");
    });

    final Answer answer = post("/chat", chatRequest());

    assertThat(answer.status).isEqualTo(502);
    assertThat(new JSONObject(answer.body).getString("code", "")).isEqualTo("token_invalid");
  }

  @Test
  void anActivationRefusalEchoesOnlyTheStartOfTheGatewaysError() throws Exception {
    final byte[] json = new JSONObject().put("error", "e".repeat(CAP / 2)).toString().getBytes(StandardCharsets.UTF_8);
    startGateway((path, out, sent) -> {
      write(out, "HTTP/1.1 400 Bad Request\r\nContent-Type: application/json\r\nContent-Length: " + json.length + "\r\n\r\n");
      out.write(json);
      out.flush();
    });

    final Answer answer = post("/activate", new JSONObject().put("subscriptionKey", "key"));

    assertThat(answer.status).isEqualTo(400);
    assertThat(new JSONObject(answer.body).getString("error", "")).hasSizeLessThan(AiChatHandler.MAX_ERROR_EXCERPT_CHARS + 100);
  }

  /** An error body goes into a message or a log line only as its start. */
  @Test
  void anErrorBodyIsExcerptedForMessages() {
    assertThat(AiChatHandler.excerpt("short")).isEqualTo("short");
    assertThat(AiChatHandler.excerpt(null)).isEmpty();
    assertThat(AiChatHandler.excerpt("x".repeat(CAP))).hasSize(AiChatHandler.MAX_ERROR_EXCERPT_CHARS + 3).endsWith("...");
  }

  /** The parser's own message carries the whole input: a non-JSON answer is reported with its start only. */
  @Test
  void aNonJsonAnswerIsReportedWithItsStartOnly() {
    final String body = "x".repeat(CAP);
    assertThatThrownBy(() -> AiChatHandler.parseAnswer(body)).isInstanceOf(IllegalStateException.class)
        .satisfies(e -> assertThat(e.getMessage()).hasSizeLessThan(AiChatHandler.MAX_ERROR_EXCERPT_CHARS + 100));
    assertThat(AiChatHandler.refusalBody(body).length()).isZero();
    assertThat(AiChatHandler.parseAnswer("{\"response\":\"ok\"}").getString("response")).isEqualTo("ok");
  }

  /** The cap leaves an ordinary answer alone. */
  @Test
  void aBufferedChatAnswerUnderTheCapIsAnsweredAsBefore() throws Exception {
    final byte[] json = new JSONObject().put("response", "x".repeat(CAP / 2)).toString().getBytes(StandardCharsets.UTF_8);
    startGateway((path, out, sent) -> {
      write(out, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: " + json.length + "\r\n\r\n");
      out.write(json);
      out.flush();
    });

    final Answer answer = post("/chat", chatRequest());

    assertThat(answer.status).isEqualTo(200);
    assertThat(new JSONObject(answer.body).getString("response", "")).hasSize(CAP / 2);
  }

  // ------------------------------------------------------------------------------------------------------------
  // Fixtures
  // ------------------------------------------------------------------------------------------------------------

  @FunctionalInterface
  private interface Script {
    void answer(String path, OutputStream out, AtomicLong floodSent) throws Exception;
  }

  private record Answer(int status, String body) {
  }

  /** Sends {@link #FLOOD} bytes of 'a', as chunks of the chunked encoding or raw; stops when the reader closes the connection. */
  private static void flood(final OutputStream out, final AtomicLong sent, final boolean chunked) throws IOException {
    final byte[] chunk = new byte[64 * 1024];
    Arrays.fill(chunk, (byte) 'a');
    for (long left = FLOOD; left > 0; left -= chunk.length) {
      if (chunked)
        writeChunk(out, chunk);
      else
        out.write(chunk);
      sent.addAndGet(chunk.length);
    }
    out.flush();
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
    final HttpURLConnection conn = (HttpURLConnection) new URI(getServerHttpUrl(0, "/api/v1/ai" + path)).toURL().openConnection();
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

  private static void writeChunk(final OutputStream out, final byte[] bytes) throws IOException {
    out.write((Integer.toHexString(bytes.length) + "\r\n").getBytes(StandardCharsets.US_ASCII));
    out.write(bytes);
    out.write("\r\n".getBytes(StandardCharsets.US_ASCII));
    out.flush();
  }

  /** A gateway that answers each request with a script and records, per path, how much of its flood it delivered. */
  private static final class ScriptedGateway implements AutoCloseable {
    final int port;
    private final ServerSocket                    socket;
    private final List<Socket>                    accepted   = new CopyOnWriteArrayList<>();
    private final Map<String, AtomicLong>         floods     = new ConcurrentHashMap<>();
    private final Map<String, CountDownLatch>     floodsDone = new ConcurrentHashMap<>();
    private final Script                          script;

    ScriptedGateway(final Script script) throws IOException {
      this.script = script;
      this.socket = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
      this.port = socket.getLocalPort();
      final Thread acceptor = new Thread(this::acceptLoop, "issue9627-gateway");
      acceptor.setDaemon(true);
      acceptor.start();
    }

    long floodSent(final String path) throws InterruptedException {
      awaitFloodDone(path);
      final AtomicLong sent = floods.get(path);
      return sent == null ? -1 : sent.get();
    }

    void awaitFloodDone(final String path) throws InterruptedException {
      assertThat(latch(path).await(HANG_DETECT_MS, TimeUnit.MILLISECONDS)).as("the gateway's answer to " + path + " ended").isTrue();
    }

    private CountDownLatch latch(final String path) {
      return floodsDone.computeIfAbsent(path, p -> new CountDownLatch(1));
    }

    private void acceptLoop() {
      while (!socket.isClosed()) {
        try {
          final Socket connection = socket.accept();
          accepted.add(connection);
          final Thread handler = new Thread(() -> serve(connection), "issue9627-gateway-connection");
          handler.setDaemon(true);
          handler.start();
        } catch (final IOException e) {
          return;
        }
      }
    }

    private void serve(final Socket connection) {
      String path = "";
      try {
        final InputStream in = connection.getInputStream();
        path = readRequest(in);
        final AtomicLong sent = floods.computeIfAbsent(path, p -> new AtomicLong());
        script.answer(path, connection.getOutputStream(), sent);
      } catch (final Exception ignored) {
        // The reader closed the connection: that is what the tests assert on
      } finally {
        latch(path).countDown();
        try {
          connection.close();
        } catch (final IOException ignored) {
          // closing anyway
        }
      }
    }

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
