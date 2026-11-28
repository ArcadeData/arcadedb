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
package com.arcadedb.server.http;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * Issue #12606: an HTTP/2 connection is read for as long as it is open, so the read timeout it carries
 * ({@code arcadedb.network.socketTimeout}) kept running while one of its streams waited for an answer. A client waiting for
 * its answer has no frame to send, so the timer closed the connection one socket timeout into any statement longer than
 * that, and the client got "EOF reached while reading" where the same request on HTTP/1.1 got its answer. The read timeout
 * is lifted while a stream of the connection is being served and put back when the last one ends.
 * <p>
 * The socket timeout is lowered to one second here so a three-second statement crosses it.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
// A hang detector, not a latency bound
@Timeout(value = 5, unit = TimeUnit.MINUTES)
class Http2ReadTimeoutLiftTest extends BaseGraphServerTest {
  private static final int SOCKET_TIMEOUT_MS = 1_000;

  private static final int FRAME_DATA     = 0;
  private static final int FRAME_HEADERS  = 1;
  private static final int FLAG_END_STREAM = 0x1;

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.NETWORK_SOCKET_TIMEOUT, SOCKET_TIMEOUT_MS);
  }

  /** The reproduction of the issue: the JDK client at its default version, a GET that upgrades the connection, then the statement. */
  @Test
  void aStatementLongerThanTheSocketTimeoutIsAnsweredOnHttp2() throws Exception {
    final HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
    upgrade(client);

    final HttpResponse<String> response = client.send(command("SLEEP 3000; SELECT 1 AS answer"),
        HttpResponse.BodyHandlers.ofString());
    assertThat(response.version()).as("the statement travelled on HTTP/2").isEqualTo(HttpClient.Version.HTTP_2);
    assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
    assertThat(response.body()).contains("\"answer\":1");
  }

  /**
   * The read timeout belongs to the connection, which every stream shares: it stays lifted until the LAST stream ends. Were
   * it put back when the first one does, the second would lose the connection one socket timeout later.
   */
  @Test
  void theReadTimeoutStaysLiftedUntilTheLastStreamEnds() throws Exception {
    final HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
    upgrade(client);

    final CompletableFuture<HttpResponse<String>> longer = client.sendAsync(command("SLEEP 6000; SELECT 2 AS answer"),
        HttpResponse.BodyHandlers.ofString());
    final CompletableFuture<HttpResponse<String>> shorter = client.sendAsync(command("SLEEP 2000; SELECT 1 AS answer"),
        HttpResponse.BodyHandlers.ofString());

    final HttpResponse<String> first = shorter.get(60, TimeUnit.SECONDS);
    assertThat(first.version()).isEqualTo(HttpClient.Version.HTTP_2);
    assertThat(first.statusCode()).as(first.body()).isEqualTo(200);
    assertThat(first.body()).contains("\"answer\":1");

    // Four seconds, four socket timeouts, after the other stream ended
    final HttpResponse<String> second = longer.get(60, TimeUnit.SECONDS);
    assertThat(second.version()).isEqualTo(HttpClient.Version.HTTP_2);
    assertThat(second.statusCode()).as(second.body()).isEqualTo(200);
    assertThat(second.body()).contains("\"answer\":2");
  }

  /**
   * The read timeout is put back when the stream ends: a connection that goes idle after a long statement is still closed
   * by the server. Spoken by hand, because no HTTP client shows when the server closed its connection.
   */
  @Test
  void theReadTimeoutGuardsTheIdleConnectionAgainAfterTheStatement() throws Exception {
    try (final Socket socket = connect()) {
      final InputStream in = upgradeWith(socket, "SLEEP 3000; SELECT 1 AS answer");

      assertThat(readAnswerOfStreamOne(in)).as("the status the long statement was answered with").isEqualTo(200);

      socket.setSoTimeout(30_000);
      try {
        drainUntilClosed(in);
      } catch (final SocketTimeoutException e) {
        fail("the idle connection was never closed: the read timeout was not restored after the statement");
      }
    }
  }

  /** A connection no stream of which is being served keeps its read timeout: nothing was lifted for it. */
  @Test
  void anIdleHttp2ConnectionIsStillClosedAfterTheSocketTimeout() throws Exception {
    try (final Socket socket = connect()) {
      final InputStream in = upgradeWith(socket, "SELECT 1 AS answer");

      assertThat(readAnswerOfStreamOne(in)).isEqualTo(200);

      socket.setSoTimeout(30_000);
      try {
        drainUntilClosed(in);
      } catch (final SocketTimeoutException e) {
        fail("the idle connection was never closed by the read timeout");
      }
    }
  }

  /** The first request of the JDK client upgrades the plain connection to HTTP/2: every later one is a stream of it. */
  private void upgrade(final HttpClient client) throws Exception {
    client.send(HttpRequest.newBuilder(URI.create(getServerHttpUrl(0, "/api/v1/ready"))).GET().build(),
        HttpResponse.BodyHandlers.discarding());
    final HttpResponse<Void> ready = client.send(
        HttpRequest.newBuilder(URI.create(getServerHttpUrl(0, "/api/v1/ready"))).GET().build(),
        HttpResponse.BodyHandlers.discarding());
    assertThat(ready.version()).as("the connection was upgraded to HTTP/2").isEqualTo(HttpClient.Version.HTTP_2);
  }

  private HttpRequest command(final String script) {
    return HttpRequest.newBuilder(URI.create(getServerHttpUrl(0, "/api/v1/command/" + getDatabaseName())))
        .timeout(Duration.ofSeconds(60))
        .header("Authorization", basicAuth())
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(payload(script)))
        .build();
  }

  private static String payload(final String script) {
    return new JSONObject().put("language", "sqlscript").put("command", script).toString();
  }

  private Socket connect() throws IOException {
    final Socket socket = new Socket();
    socket.connect(new InetSocketAddress("localhost", getServerHttpPort(0)), 5_000);
    socket.setSoTimeout(60_000);
    return socket;
  }

  /**
   * Sends {@code script} as an HTTP/1.1 request that upgrades the connection to HTTP/2 (RFC 7540, section 3.2): the server
   * answers 101, then the request on stream 1 of the HTTP/2 connection. Answers the stream to read the frames from.
   */
  private InputStream upgradeWith(final Socket socket, final String script) throws IOException {
    final byte[] body = payload(script).getBytes(StandardCharsets.UTF_8);
    final OutputStream out = socket.getOutputStream();
    // HTTP2-Settings: one setting, SETTINGS_MAX_CONCURRENT_STREAMS = 100
    out.write(("POST /api/v1/command/" + getDatabaseName() + " HTTP/1.1\r\n" + "Host: localhost\r\n"
        + "Connection: Upgrade, HTTP2-Settings\r\n" + "Upgrade: h2c\r\n" + "HTTP2-Settings: AAMAAABk\r\n" + "Authorization: "
        + basicAuth() + "\r\n" + "Content-Type: application/json\r\n" + "Content-Length: " + body.length
        + "\r\n\r\n").getBytes(StandardCharsets.UTF_8));
    out.write(body);
    out.flush();

    final InputStream in = socket.getInputStream();
    assertThat(readHttp1Head(in)).as("the server switches protocols").startsWith("HTTP/1.1 101");

    // The client connection preface: the magic octets, then a SETTINGS frame, here an empty one
    out.write("PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n".getBytes(StandardCharsets.US_ASCII));
    out.write(new byte[] { 0, 0, 0, 4, 0, 0, 0, 0, 0 });
    out.flush();
    return in;
  }

  /**
   * Reads frames up to the end of stream 1 and answers the status of its response. The status is the first header field of
   * the block and Undertow sends the common ones as an index into the HPACK static table (RFC 7541, appendix A), which is
   * all this reads.
   */
  private static int readAnswerOfStreamOne(final InputStream in) throws IOException {
    final DataInputStream frames = new DataInputStream(in);
    int status = -1;
    while (true) {
      final int length;
      try {
        length = (frames.readUnsignedByte() << 16) | frames.readUnsignedShort();
      } catch (final EOFException e) {
        throw new IOException("The server closed the connection before it answered stream 1", e);
      }
      final int type = frames.readUnsignedByte();
      final int flags = frames.readUnsignedByte();
      final int streamId = frames.readInt() & 0x7FFFFFFF;
      final byte[] framePayload = frames.readNBytes(length);
      if (framePayload.length < length)
        throw new IOException("The server closed the connection in the middle of a frame");

      if (streamId != 1)
        continue;
      if (type == FRAME_HEADERS && status < 0 && length > 0)
        status = switch (framePayload[0] & 0xFF) {
          case 0x88 -> 200;
          case 0x89 -> 204;
          case 0x8C -> 400;
          case 0x8E -> 500;
          default -> 0;
        };
      if ((type == FRAME_HEADERS || type == FRAME_DATA) && (flags & FLAG_END_STREAM) != 0)
        return status;
    }
  }

  /** Reads until the server closes the connection. A reset is the server closing it too. */
  private static void drainUntilClosed(final InputStream in) throws SocketTimeoutException {
    final byte[] buffer = new byte[4096];
    try {
      while (in.read(buffer) >= 0) {
        // GOAWAY, or whatever the server still had to say
      }
    } catch (final SocketTimeoutException e) {
      throw e;
    } catch (final IOException e) {
      // Closed
    }
  }

  private static String readHttp1Head(final InputStream in) throws IOException {
    final ByteArrayOutputStream head = new ByteArrayOutputStream();
    int matched = 0;
    while (matched < 4) {
      final int b = in.read();
      if (b < 0)
        throw new IOException("Connection closed while reading the response headers: " + head);
      head.write(b);
      matched = (b == '\r' && (matched == 0 || matched == 2)) || (b == '\n' && (matched == 1 || matched == 3)) ? matched + 1 : 0;
    }
    return head.toString(StandardCharsets.UTF_8);
  }

  private static String basicAuth() {
    return "Basic " + Base64.getEncoder()
        .encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8));
  }
}
