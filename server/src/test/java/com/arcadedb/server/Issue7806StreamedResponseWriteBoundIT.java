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
package com.arcadedb.server;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ai.AiConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7806: issue #7381 bounded the write side of ONE streamed response, the NDJSON answer of
 * {@code POST /api/v1/batch}. Every other streamed response - the NDJSON query encoding of {@code GET /query},
 * {@code POST /query} and {@code POST /command}, and the Server-Sent Events of the AI chat - was still written with
 * a plain blocking {@code write()}: a client that stopped reading left the worker thread blocked in it for as long as
 * the client kept the connection open.
 * <p>
 * Each test opens a raw socket with a tiny receive window, asks for a response far larger than anything the socket
 * buffers between the two ends can hold, and then reads NOTHING for several budgets. Only after that does it drain
 * whatever the connection still delivers. A bounded server has given up in the meantime and closed the connection,
 * so the stream ends without its terminal line. An unbounded server was merely blocked, and the drain releases it:
 * the stream then completes, terminal line included - which is what fails the assertion.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
@Tag("slow")
class Issue7806StreamedResponseWriteBoundIT extends BaseGraphServerTest {

  private static final String NDJSON          = "application/x-ndjson";
  /** The write-side budget under test. Short, so proving the bound costs seconds rather than minutes. */
  private static final int    WRITE_BUDGET_MS = 1_000;
  /**
   * How long the client reads nothing: many budgets, so the server has certainly produced enough to fill the buffers,
   * blocked, and - when bounded - given up. A wider window cannot turn a passing run red.
   */
  private static final long   NOT_READING_MS  = 10_000;
  /** A hang detector on the final drain, not a latency bound. */
  private static final int    HANG_DETECT_MS  = 120_000;
  /** Rows (or chat events) of the oversized response, and the payload of each: ~16 MB in all. */
  private static final int    ROWS            = 4_000;
  private static final int    ROW_PAYLOAD     = 4_000;

  private ScriptedGateway gateway;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_HTTP_STREAMING_WRITE_TIMEOUT.setValue(WRITE_BUDGET_MS);
  }

  @AfterEach
  void closeGateway() throws Exception {
    if (gateway != null)
      gateway.close();
    // The chats directory is shared by every test in this module (same clean-up as AiChatHandlerStreamingTest; the
    // directory is the SHA-256 of the user name, as ChatStorage names it).
    final File[] chats = new File("./target/chats/" + HexFormat.of().formatHex(
        MessageDigest.getInstance("SHA-256").digest("root".getBytes(StandardCharsets.UTF_8)))).listFiles();
    if (chats != null)
      for (final File f : chats)
        //noinspection ResultOfMethodCallIgnored
        f.delete();
  }

  @Test
  @Timeout(300)
  void aStreamedGetQueryNobodyReadsIsGivenUpOn() throws Exception {
    createBigRows();
    final String command = URLEncoder.encode("select from Big7806", StandardCharsets.UTF_8).replace("+", "%20");
    final String body = requestAndDrainLate("GET", "/api/v1/query/" + getDatabaseName() + "/sql/" + command, NDJSON,
        null);
    assertStreamCutShort(body, "{\"record\"", "{\"stats\"");
  }

  @Test
  @Timeout(300)
  void aStreamedPostQueryNobodyReadsIsGivenUpOn() throws Exception {
    createBigRows();
    final String body = requestAndDrainLate("POST", "/api/v1/query/" + getDatabaseName(), NDJSON,
        new JSONObject().put("language", "sql").put("command", "select from Big7806").toString());
    assertStreamCutShort(body, "{\"record\"", "{\"stats\"");
  }

  @Test
  @Timeout(300)
  void aStreamedCommandNobodyReadsIsGivenUpOn() throws Exception {
    createBigRows();
    final String body = requestAndDrainLate("POST", "/api/v1/command/" + getDatabaseName(), NDJSON,
        new JSONObject().put("language", "sql").put("command", "select from Big7806").toString());
    assertStreamCutShort(body, "{\"record\"", "{\"stats\"");
  }

  @Test
  @Timeout(300)
  void aStreamedAiChatNobodyReadsIsGivenUpOn() throws Exception {
    final String padding = "x".repeat(ROW_PAYLOAD);
    startGateway(out -> {
      write(out, "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n"
          + "Connection: close\r\n\r\n");
      writeEvent(out, new JSONObject().put("type", "session").put("sessionId", "s-7806"));
      for (int i = 0; i < ROWS; i++)
        writeEvent(out, new JSONObject().put("type", "text").put("n", i).put("text", padding));
      writeEvent(out, new JSONObject().put("type", "done").put("response", "the end of issue 7806"));
      write(out, "0\r\n\r\n");
    });

    final String body = requestAndDrainLate("POST", "/api/v1/ai/chat/stream", "text/event-stream",
        new JSONObject().put("database", getDatabaseName()).put("message", "What types exist?").toString());
    assertStreamCutShort(body, "\"type\":\"text\"", "the end of issue 7806");
  }

  // ---------------------------------------------------------------------------------------------------------------

  private void assertStreamCutShort(final String body, final String started, final String terminal) throws Exception {
    assertThat(body)
        .as("the response must have reached the streamed encoding, or this proves nothing about it")
        .startsWith("HTTP/1.1 200")
        .contains(started);
    assertThat(body)
        .as("the server must give up on a streamed response nobody reads and close the connection; a stream that "
            + "completed once the client finally read it means the server was still holding the worker thread, "
            + "blocked in write()")
        .doesNotContain(terminal);

    // The server is still answering: an ordinary request is served as usual.
    final HttpURLConnection conn = (HttpURLConnection) new URI("http://127.0.0.1:" + getServer(0).getHttpServer()
        .getPort() + "/api/v1/query/" + getDatabaseName() + "/sql/select%201%20as%20one").toURL().openConnection();
    try {
      conn.setRequestProperty("Authorization", "Basic " + Base64.getEncoder()
          .encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)));
      conn.setReadTimeout(HANG_DETECT_MS);
      assertThat(conn.getResponseCode()).isEqualTo(200);
    } finally {
      conn.disconnect();
    }
  }

  private void createBigRows() {
    final Database db = getServerDatabase(0, getDatabaseName());
    if (db.getSchema().existsType("Big7806"))
      return;
    db.getSchema().createDocumentType("Big7806");
    final String padding = "x".repeat(ROW_PAYLOAD);
    db.transaction(() -> {
      for (int i = 0; i < ROWS; i++)
        db.newDocument("Big7806").set("n", i).set("payload", padding).save();
    });
  }

  /**
   * Sends the request on a socket with a tiny receive window, reads nothing for {@link #NOT_READING_MS}, then drains
   * the connection to its end and returns the raw status line, headers and de-chunked body.
   */
  private String requestAndDrainLate(final String method, final String path, final String accept, final String body)
      throws Exception {
    final int port = getServer(0).getHttpServer().getPort();
    final byte[] payload = body != null ? body.getBytes(StandardCharsets.UTF_8) : new byte[0];
    try (final Socket socket = new Socket()) {
      // Set before connect, which is what pins the advertised receive window.
      socket.setReceiveBufferSize(4096);
      socket.connect(new InetSocketAddress("127.0.0.1", port), 30_000);
      socket.setSoTimeout(HANG_DETECT_MS);

      final String auth = Base64.getEncoder()
          .encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8));
      final StringBuilder head = new StringBuilder()
          .append(method).append(' ').append(path).append(" HTTP/1.1\r\n")
          .append("Host: 127.0.0.1:").append(port).append("\r\n")
          .append("Authorization: Basic ").append(auth).append("\r\n")
          .append("Accept: ").append(accept).append("\r\n")
          // Without it an unbounded server that completes the stream keeps the connection alive and the drain below
          // would wait for the hang detector instead of reaching the end of the stream.
          .append("Connection: close\r\n");
      if (body != null)
        head.append("Content-Type: application/json\r\n").append("Content-Length: ").append(payload.length)
            .append("\r\n");
      head.append("\r\n");

      final OutputStream out = socket.getOutputStream();
      out.write(head.toString().getBytes(StandardCharsets.UTF_8));
      out.write(payload);
      out.flush();

      Thread.sleep(NOT_READING_MS);

      return decode(socket.getInputStream());
    }
  }

  /** Status line and headers as they are, then the chunked body de-chunked, tolerating a stream cut short. */
  private static String decode(final InputStream in) throws IOException {
    final StringBuilder head = new StringBuilder();
    while (!head.toString().endsWith("\r\n\r\n")) {
      final int b = in.read();
      if (b < 0)
        return head.toString();
      head.append((char) b);
    }
    final ByteArrayOutputStream body = new ByteArrayOutputStream();
    if (!head.toString().toLowerCase(Locale.ROOT).contains("transfer-encoding: chunked")) {
      body.write(readRemaining(in));
      return head + body.toString(StandardCharsets.UTF_8);
    }
    try {
      while (true) {
        final String sizeLine = readLine(in);
        if (sizeLine == null || sizeLine.isBlank())
          break;
        final int size = Integer.parseInt(sizeLine.split(";")[0].trim(), 16);
        if (size == 0)
          break;
        for (int i = 0; i < size; i++) {
          final int b = in.read();
          if (b < 0)
            return head + body.toString(StandardCharsets.UTF_8);
          body.write(b);
        }
        readLine(in); // CRLF closing the chunk
      }
    } catch (final IOException e) {
      // A connection reset by the server that gave up is one way the stream ends: what arrived is the answer.
    }
    return head + body.toString(StandardCharsets.UTF_8);
  }

  private static byte[] readRemaining(final InputStream in) {
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    try {
      final byte[] buffer = new byte[8192];
      for (int n = in.read(buffer); n >= 0; n = in.read(buffer))
        out.write(buffer, 0, n);
    } catch (final IOException e) {
      // As above: a reset ends the stream.
    }
    return out.toByteArray();
  }

  private static String readLine(final InputStream in) throws IOException {
    final StringBuilder line = new StringBuilder();
    for (int b = in.read(); b >= 0; b = in.read()) {
      if (b == '\n')
        return line.toString().trim();
      line.append((char) b);
    }
    return line.isEmpty() ? null : line.toString().trim();
  }

  // ---------------------------------------------------------------------------------------------------------------
  // A scripted AI gateway (same shape as Issue8473AiGatewayStalledAnswerTest's)
  // ---------------------------------------------------------------------------------------------------------------

  @FunctionalInterface
  private interface Script {
    void answer(OutputStream out) throws Exception;
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

  private static void write(final OutputStream out, final String text) throws IOException {
    out.write(text.getBytes(StandardCharsets.UTF_8));
    out.flush();
  }

  private static void writeEvent(final OutputStream out, final JSONObject event) throws IOException {
    final byte[] bytes = ("data: " + event + "\n\n").getBytes(StandardCharsets.UTF_8);
    out.write((Integer.toHexString(bytes.length) + "\r\n").getBytes(StandardCharsets.UTF_8));
    out.write(bytes);
    write(out, "\r\n");
  }

  private static final class ScriptedGateway implements AutoCloseable {
    final int port;
    private final ServerSocket socket;
    private final List<Socket> accepted = new CopyOnWriteArrayList<>();
    private final Script       script;

    ScriptedGateway(final Script script) throws IOException {
      this.script = script;
      this.port = allocateFreePorts(1)[0];
      this.socket = new ServerSocket(port, 50, InetAddress.getLoopbackAddress());
      final Thread acceptor = new Thread(this::acceptLoop, "issue7806-gateway");
      acceptor.setDaemon(true);
      acceptor.start();
    }

    private void acceptLoop() {
      while (!socket.isClosed()) {
        try {
          final Socket connection = socket.accept();
          accepted.add(connection);
          final Thread handler = new Thread(() -> serve(connection), "issue7806-gateway-connection");
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
        readRequest(in);
        script.answer(connection.getOutputStream());
        while (in.read() >= 0) {
          // hold the connection until the client closes it
        }
      } catch (final Exception ignored) {
        // The test asserts on the client side
      }
    }

    private static void readRequest(final InputStream in) throws IOException {
      final StringBuilder headers = new StringBuilder();
      while (!headers.toString().endsWith("\r\n\r\n")) {
        final int b = in.read();
        if (b < 0)
          return;
        headers.append((char) b);
      }
      long length = 0;
      for (final String line : headers.toString().split("\r\n"))
        if (line.toLowerCase(Locale.ROOT).startsWith("content-length:"))
          length = Long.parseLong(line.substring("content-length:".length()).trim());
      for (long i = 0; i < length; i++)
        if (in.read() < 0)
          break;
    }

    @Override
    public void close() throws IOException {
      socket.close();
      for (final Socket s : new ArrayList<>(accepted))
        s.close();
    }
  }
}
