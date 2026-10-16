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
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Base64;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for <a href="https://github.com/ArcadeData/arcadedb/issues/7772">issue #7772</a>.
 * <p>
 * {@code arcadedb.server.httpBodyContentMaxSize} used to be enforced ONLY against the length a request
 * declared. A request that declares none - {@code Transfer-Encoding: chunked}, or an HTTP/2 POST without
 * {@code content-length} - reports {@code -1} from {@code HttpServerExchange.getRequestContentLength()}, sailed
 * past that check and was then read whole into heap: {@code Undertow.MAX_ENTITY_SIZE} was pinned to
 * {@code Long.MAX_VALUE}, and Undertow's own {@code Receiver.maxBufferSize} only ever compares against the
 * DECLARED length too, so nothing capped the bytes actually read.
 * <p>
 * Every test below sends the SAME chunked body through a different body-reading entry point of the HTTP
 * server, with the cap set to 1 KB. Each one must be refused; before the fix each one buffered the whole
 * payload and answered as though the request were acceptable.
 * <p>
 * Each oversize case offers its body WITHOUT ever sending the terminating {@code 0\r\n\r\n} chunk, and that is
 * what makes the 413 mean something: it can only come from a server that decided on the bytes it had already
 * read. A server that waits for the end of the body - which is every one of these paths before the fix - never
 * answers at all, and the test fails on the read timeout instead.
 * <p>
 * What is deliberately NOT asserted is how many body bytes the client managed to write before the answer
 * arrived. That number measures the kernel socket buffers rather than the server: on a Linux loopback the whole
 * payload fits in them, so the client finishes writing long before the handler has read its first chunk, and an
 * {@code isLessThan(offered)} on it is red on CI and green on macOS for reasons that have nothing to do with the
 * cap. The heap bound the fix provides is not observable from the client side.
 */
class Issue7772ChunkedBodySizeLimitTest extends BaseGraphServerTest {

  private static final long SMALL_LIMIT_BYTES = 1024L;
  /** Well past the 1 KB cap, and small enough that the test stays fast even when the server accepts it all. */
  private static final long OVERSIZED_BODY_BYTES = 1024L * 1024L;

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    config.setValue(GlobalConfiguration.SERVER_HTTP_BODY_CONTENT_MAX_SIZE, SMALL_LIMIT_BYTES);
  }

  /**
   * {@code AbstractServerHttpHandler.parseRequestPayload} - the STRING body every JSON route reads, which is
   * the path the reporter demonstrated.
   */
  @Test
  void chunkedBodyOverLimitIsRefusedOnTheCommandEndpoint() throws Exception {
    final Response response = postChunked("/api/v1/command/" + getDatabaseName(), "application/json",
        OVERSIZED_BODY_BYTES);

    assertThat(response.statusCode).isEqualTo(413);
    assertThat(response.body).contains(GlobalConfiguration.SERVER_HTTP_BODY_CONTENT_MAX_SIZE.getKey());
  }

  /**
   * {@code AbstractBinaryHttpHandler.parseRequestPayload} - the BYTES body of the Prometheus remote-write
   * route. A second buffered reader with its own copy of the same {@code receiveFullBytes} call, so the fix
   * has to reach it too.
   */
  @Test
  void chunkedBodyOverLimitIsRefusedOnThePrometheusWriteEndpoint() throws Exception {
    final Response response = postChunked("/api/v1/ts/" + getDatabaseName() + "/prom/write",
        "application/x-protobuf", OVERSIZED_BODY_BYTES);

    assertThat(response.statusCode).isEqualTo(413);
    assertThat(response.body).contains(GlobalConfiguration.SERVER_HTTP_BODY_CONTENT_MAX_SIZE.getKey());
  }

  /**
   * {@code PostTimeSeriesWriteHandler.parseRequestPayload} - the InfluxDB line-protocol ingest route, a third
   * copy of the same buffered read.
   */
  @Test
  void chunkedBodyOverLimitIsRefusedOnTheTimeSeriesWriteEndpoint() throws Exception {
    final Response response = postChunked("/api/v1/ts/" + getDatabaseName() + "/write", "text/plain",
        OVERSIZED_BODY_BYTES);

    assertThat(response.statusCode).isEqualTo(413);
    assertThat(response.body).contains(GlobalConfiguration.SERVER_HTTP_BODY_CONTENT_MAX_SIZE.getKey());
  }

  /**
   * {@code PostBatchHandler} - the only route that does NOT buffer the body; it streams it from
   * {@code exchange.getInputStream()}. It is bounded by the same setting (see the note left on the removed
   * {@code arcadedb.ha.proxyMaxBodySize} in {@code GlobalConfiguration}), and the refusal must name the size
   * cap rather than be reported as a client that went away mid-upload.
   */
  @Test
  void chunkedBodyOverLimitIsRefusedOnTheStreamingBatchEndpoint() throws Exception {
    final Response response = postChunked("/api/v1/batch/" + getDatabaseName(), "application/x-ndjson",
        OVERSIZED_BODY_BYTES);

    assertThat(response.statusCode).isEqualTo(413);
    assertThat(response.body).contains(GlobalConfiguration.SERVER_HTTP_BODY_CONTENT_MAX_SIZE.getKey());
  }

  /**
   * The declared-length half of the cap, which {@code HttpServer.createBodySizeLimitHandler} answers before a
   * single body byte is read. Covered by {@code HttpBodySizeLimitTest} too, and repeated here because that class
   * addresses {@code 127.0.0.1:2480} literally: when anything else already holds that port the test server binds
   * another one and the assertion is made against a stranger. This one asks {@link #getServerHttpPort()} which
   * port the server under test actually bound, so the guarantee is checked against the right process.
   */
  @Test
  void declaredLengthOverLimitStillReturnsTheJson413() throws Exception {
    final Response response = postDeclaredLength("/api/v1/command/" + getDatabaseName(), "application/json",
        OVERSIZED_BODY_BYTES);

    assertThat(response.statusCode).isEqualTo(413);
    assertThat(response.body).contains(GlobalConfiguration.SERVER_HTTP_BODY_CONTENT_MAX_SIZE.getKey());
  }

  /**
   * The cap must refuse an oversized body, not chunked encoding itself: a chunked request that stays under the
   * limit is served exactly as a declared-length one.
   */
  @Test
  void chunkedBodyWithinLimitIsServedNormally() throws Exception {
    final Response response = postChunkedBody("/api/v1/query/" + getDatabaseName(), "application/json",
        "{\"language\":\"sql\",\"command\":\"SELECT 1\"}".getBytes(StandardCharsets.UTF_8));

    assertThat(response.statusCode).isEqualTo(200);
  }

  /**
   * Issue #8081: the other shape of undeclared body the issue names - an HTTP/2 POST with no {@code content-length} -
   * on cleartext HTTP/2 (h2c), which the server enables with {@code UndertowOptions.ENABLE_HTTP2}.
   * <p>
   * The JDK client negotiates h2c by an {@code Upgrade} on a bodiless request, so a GET opens the HTTP/2 connection and
   * the POST then travels on it as a stream of DATA frames: there is no chunked framing, and the body publisher has no
   * length, so no {@code content-length} header is sent either. Both versions are asserted, or the test could pass over
   * HTTP/1.1 having proved nothing about HTTP/2.
   * <p>
   * The body never ends. Undertow used to drain it after the 413 for as long as the client sent, and the JDK client,
   * which reports the answer only once it has sent the body, never got one. Now the server discards a bounded budget
   * and then resets the stream with {@code NO_ERROR}: the request ends, with the same JSON 413 as over HTTP/1.1 - or, when
   * Undertow answers DATA still in flight past the reset with {@code GOAWAY}, with the connection. The 413 itself is
   * asserted unconditionally by {@link #http2OversizedBodiesThatEndKeepTheConnection}, whose bodies end within the
   * budget.
   * <p>
   * The client is served afterwards, and that request must not stall: a reset that left the stream's bytes uncredited
   * in the connection's flow-control window stalled every later request with a body on that connection. Undertow may
   * instead have ended the connection, answering DATA that was in flight past the reset with {@code GOAWAY} (see
   * {@code Http2StreamReset}); a request that meets that is sent again, on the new connection the next bodiless request
   * opens - a stall is a timeout, which is never retried.
   */
  @Test
  void http2BodyWithoutContentLengthOverLimitIsRefused() throws Exception {
    final HttpClient client = http2Client();

    // Past the discard budget the stream is reset, and DATA the client still had in flight can reach Undertow after it,
    // which Undertow answers with GOAWAY (see Http2StreamReset). Which of the two the JDK client processes first decides
    // whether it reports the 413 or that the connection ended: both are the request ENDING, which is the guarantee. A
    // stall is a timeout, which fails the test.
    HttpResponse<String> response = null;
    try {
      response = client.send(HttpRequest.newBuilder(URI.create(getServerHttpUrl("/api/v1/command/" + getDatabaseName())))
          .timeout(Duration.ofSeconds(30))
          .header("Authorization", basicAuthorization())
          .header("Content-Type", "application/json")
          .POST(HttpRequest.BodyPublishers.ofInputStream(EndlessBody::new))
          .build(), HttpResponse.BodyHandlers.ofString());
    } catch (final HttpTimeoutException stalled) {
      throw stalled;
    } catch (final IOException connectionEnded) {
      // GOAWAY for DATA in flight past the reset: the refusal ended the request with the connection
    }

    if (response != null) {
      assertThat(response.version()).as("the POST must have travelled over HTTP/2").isEqualTo(HttpClient.Version.HTTP_2);
      assertThat(response.statusCode()).isEqualTo(413);
      assertThat(response.body()).contains(GlobalConfiguration.SERVER_HTTP_BODY_CONTENT_MAX_SIZE.getKey());
    }

    HttpResponse<String> next = null;
    for (int attempt = 0; next == null && attempt < 3; attempt++)
      try {
        assertServedOverHttp2(client);
        next = client.send(queryRequest(), HttpResponse.BodyHandlers.ofString());
      } catch (final HttpTimeoutException stalled) {
        throw stalled;
      } catch (final IOException connectionEnded) {
        // GOAWAY for DATA that was in flight past the reset: the next attempt opens a new connection
      }
    assertThat(next).as("the client must still be served after the refusal").isNotNull();
    assertThat(next.version()).isEqualTo(HttpClient.Version.HTTP_2);
    assertThat(next.statusCode()).as(next.body()).isEqualTo(200);
  }

  /**
   * Issue #8081: a body over the cap that does END - the common case, a batch somewhat larger than the limit - is read to
   * its end within the discard budget, so its stream finishes cleanly and the connection is reused: several refusals in a
   * row and a request after them, all on the one HTTP/2 connection. Refusing them by resetting the stream at once would
   * have the client's in-flight DATA answered with {@code GOAWAY}, failing whatever else it had on that connection.
   */
  @Test
  void http2OversizedBodiesThatEndKeepTheConnection() throws Exception {
    final HttpClient client = http2Client();
    final int bodyBytes = 2 * 1024 * 1024;

    for (int i = 0; i < 3; i++) {
      final HttpResponse<String> response = client.send(HttpRequest.newBuilder(
              URI.create(getServerHttpUrl("/api/v1/command/" + getDatabaseName())))
          .timeout(Duration.ofSeconds(30))
          .header("Authorization", basicAuthorization())
          .header("Content-Type", "application/json")
          .POST(HttpRequest.BodyPublishers.ofInputStream(() -> new FiniteBody(bodyBytes)))
          .build(), HttpResponse.BodyHandlers.ofString());

      assertThat(response.version()).as("refusal %d must have travelled over HTTP/2", i).isEqualTo(HttpClient.Version.HTTP_2);
      assertThat(response.statusCode()).isEqualTo(413);
    }

    final HttpResponse<String> next = client.send(queryRequest(), HttpResponse.BodyHandlers.ofString());
    assertThat(next.version()).isEqualTo(HttpClient.Version.HTTP_2);
    assertThat(next.statusCode()).as(next.body()).isEqualTo(200);
  }

  /**
   * Issue #8081 on HTTP/1.1: a kept-alive connection survives a refused body that ends within the discard budget. Before
   * the budget it was drained whole, however long; cutting it off at once instead would close a connection the client
   * is still writing to.
   */
  @Test
  void keepAliveChunkedBodyOverLimitThatEndsKeepsTheConnection() throws Exception {
    final String credentials = Base64.getEncoder()
        .encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8));
    final byte[] filler = new byte[64 * 1024];
    Arrays.fill(filler, (byte) 'x');

    try (final Socket socket = new Socket("127.0.0.1", getServerHttpPort())) {
      socket.setSoTimeout(30_000);
      final OutputStream out = socket.getOutputStream();
      final InputStream in = socket.getInputStream();

      out.write(("POST /api/v1/command/" + getDatabaseName() + " HTTP/1.1\r\n"
          + "Host: 127.0.0.1\r\n"
          + "Authorization: Basic " + credentials + "\r\n"
          + "Content-Type: application/json\r\n"
          + "Transfer-Encoding: chunked\r\n"
          + "\r\n").getBytes(StandardCharsets.US_ASCII));
      // 2 MB, then the terminating chunk: past the cap, within the discard budget
      for (int i = 0; i < 32; i++) {
        out.write((Integer.toHexString(filler.length) + "\r\n").getBytes(StandardCharsets.US_ASCII));
        out.write(filler);
        out.write("\r\n".getBytes(StandardCharsets.US_ASCII));
      }
      out.write("0\r\n\r\n".getBytes(StandardCharsets.US_ASCII));
      out.flush();

      final String refused = readResponse(in);
      assertThat(statusCodeOf(refused)).as(refused).isEqualTo(413);

      out.write(("GET /api/v1/ready HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n").getBytes(StandardCharsets.US_ASCII));
      out.flush();
      final String next = readResponse(in);
      assertThat(statusCodeOf(next)).as("the same connection serves the next request: " + next).isEqualTo(204);
    }
  }

  /**
   * Issue #8081, the HTTP/1.1 half of the defect the HTTP/2 test found: a refused body is read no further than the
   * discard budget. Every other test here sends {@code Connection: close}, which made Undertow close the request side
   * when the exchange ended. A client that keeps the connection alive used to have the rest of its body DRAINED instead
   * - read and discarded by a worker thread for as long as the client kept sending, which for a body that never ends is
   * forever. Past the budget the connection is closed.
   * <p>
   * Bounded by bytes rather than by time, so a JVM stall cannot fail it: a server that stops reading makes the writes
   * fail within the socket buffers, while one that drains accepts all of them.
   */
  @Test
  void keepAliveChunkedBodyOverLimitEndsTheConnectionInsteadOfDrainingIt() throws Exception {
    final String credentials = Base64.getEncoder()
        .encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8));
    final byte[] filler = new byte[64 * 1024];
    Arrays.fill(filler, (byte) 'x');
    final byte[] chunkHeader = (Integer.toHexString(filler.length) + "\r\n").getBytes(StandardCharsets.US_ASCII);
    final byte[] chunkTrailer = "\r\n".getBytes(StandardCharsets.US_ASCII);
    final long maxBytes = 256L * 1024L * 1024L;

    try (final Socket socket = new Socket("127.0.0.1", getServerHttpPort())) {
      socket.setSoTimeout(30_000);

      // Started before the first body byte, like exchange(): the answer arrives while the client is still writing, and
      // asserting it is what makes the refusal the 413 and not some other failure that happened to close the connection
      final StringBuilder collected = new StringBuilder();
      final Thread reader = new Thread(() -> {
        try (final InputStream in = socket.getInputStream()) {
          final byte[] buffer = new byte[8192];
          int read;
          while ((read = in.read(buffer)) > 0)
            synchronized (collected) {
              collected.append(new String(buffer, 0, read, StandardCharsets.UTF_8));
            }
        } catch (final IOException ignored) {
          // The server ends the connection after the answer; whatever arrived first is the answer.
        }
      }, "issue8081-keepalive-response-reader");
      reader.setDaemon(true);
      reader.start();

      final OutputStream out = socket.getOutputStream();
      out.write(("POST /api/v1/command/" + getDatabaseName() + " HTTP/1.1\r\n"
          + "Host: 127.0.0.1\r\n"
          + "Authorization: Basic " + credentials + "\r\n"
          + "Content-Type: application/json\r\n"
          + "Transfer-Encoding: chunked\r\n"
          + "\r\n").getBytes(StandardCharsets.US_ASCII));

      long sent = 0;
      boolean refused = false;
      while (sent < maxBytes)
        try {
          out.write(chunkHeader);
          out.write(filler);
          out.write(chunkTrailer);
          out.flush();
          sent += filler.length;
        } catch (final IOException e) {
          refused = true;
          break;
        }

      assertThat(refused).as("the server must stop reading a refused body, not drain the " + sent + " bytes offered")
          .isTrue();

      reader.join(30_000);
      final String raw;
      synchronized (collected) {
        raw = collected.toString();
      }
      assertThat(statusCodeOf(raw)).as("the connection ended because the body was refused: " + raw).isEqualTo(413);
      assertThat(bodyOf(raw)).contains(GlobalConfiguration.SERVER_HTTP_BODY_CONTENT_MAX_SIZE.getKey());
    }
  }

  /** The HTTP/2 counterpart of {@link #chunkedBodyWithinLimitIsServedNormally}: the cap refuses size, not HTTP/2. */
  @Test
  void http2BodyWithoutContentLengthWithinLimitIsServedNormally() throws Exception {
    final HttpClient client = http2Client();
    final byte[] body = "{\"language\":\"sql\",\"command\":\"SELECT 1\"}".getBytes(StandardCharsets.UTF_8);

    final HttpResponse<String> response = client.send(HttpRequest.newBuilder(
            URI.create(getServerHttpUrl("/api/v1/query/" + getDatabaseName())))
        .timeout(Duration.ofSeconds(30))
        .header("Authorization", basicAuthorization())
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.fromPublisher(HttpRequest.BodyPublishers.ofByteArray(body)))
        .build(), HttpResponse.BodyHandlers.ofString());

    assertThat(response.version()).isEqualTo(HttpClient.Version.HTTP_2);
    assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
  }

  /** A client that has already upgraded its connection to h2c, so every request it sends next travels on HTTP/2. */
  private HttpClient http2Client() throws Exception {
    final HttpClient client = HttpClient.newBuilder()
        .version(HttpClient.Version.HTTP_2)
        .connectTimeout(Duration.ofSeconds(30))
        .build();
    assertServedOverHttp2(client);
    return client;
  }

  /**
   * A bodiless request, which the JDK client sends on its pooled HTTP/2 connection or, when there is none, upgrades a
   * new one with - so the request with a body that follows travels on HTTP/2. The JDK client never upgrades a request
   * that carries a body, and it pools an upgraded connection a moment after handing back the upgrade's response, so
   * this is sent twice: the second one is on the pooled connection.
   */
  private void assertServedOverHttp2(final HttpClient client) throws Exception {
    for (int i = 0; i < 2; i++) {
      final HttpResponse<String> response = client.send(HttpRequest.newBuilder(URI.create(getServerHttpUrl("/api/v1/ready")))
          .timeout(Duration.ofSeconds(30))
          .GET()
          .build(), HttpResponse.BodyHandlers.ofString());
      assertThat(response.version()).as("the server must accept the h2c upgrade").isEqualTo(HttpClient.Version.HTTP_2);
    }
  }

  private HttpRequest queryRequest() {
    return HttpRequest.newBuilder(URI.create(getServerHttpUrl("/api/v1/query/" + getDatabaseName())))
        .timeout(Duration.ofSeconds(30))
        .header("Authorization", basicAuthorization())
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString("{\"language\":\"sql\",\"command\":\"SELECT 1\"}"))
        .build();
  }

  /** One HTTP/1.1 response off {@code in}: the headers, then exactly {@code Content-Length} bytes of body. */
  private static String readResponse(final InputStream in) throws IOException {
    final StringBuilder head = new StringBuilder();
    while (head.indexOf("\r\n\r\n") < 0) {
      final int b = in.read();
      if (b == -1)
        break;
      head.append((char) b);
    }
    int contentLength = 0;
    for (final String line : head.toString().split("\r\n"))
      if (line.toLowerCase().startsWith("content-length:"))
        contentLength = Integer.parseInt(line.substring("content-length:".length()).trim());
    final byte[] body = in.readNBytes(contentLength);
    return head + new String(body, StandardCharsets.UTF_8);
  }

  private static String basicAuthorization() {
    return "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8));
  }

  /** A request body of {@code size} bytes whose length the client cannot know in advance. */
  private static final class FiniteBody extends InputStream {
    private int remaining;

    private FiniteBody(final int size) {
      this.remaining = size;
    }

    @Override
    public int read() {
      if (remaining <= 0)
        return -1;
      --remaining;
      return 'x';
    }

    @Override
    public int read(final byte[] buffer, final int offset, final int length) {
      if (remaining <= 0)
        return -1;
      final int n = Math.min(length, remaining);
      Arrays.fill(buffer, offset, offset + n, (byte) 'x');
      remaining -= n;
      return n;
    }
  }

  /** A request body that never ends, so only a server that decides on the bytes it has read can answer it. */
  private static final class EndlessBody extends InputStream {
    @Override
    public int read() {
      return 'x';
    }

    @Override
    public int read(final byte[] buffer, final int offset, final int length) {
      Arrays.fill(buffer, offset, offset + length, (byte) 'x');
      return length;
    }
  }

  /**
   * Raising {@code arcadedb.server.httpBodyContentMaxSize} at runtime has to raise the whole enforcement, not
   * just the half of it that is re-read per request.
   * <p>
   * The cap is consulted on every request. An earlier revision of this fix also placed Undertow's own
   * entity-size ceiling just above it as a backstop, and that ceiling is frozen at the value read when the
   * server is built: a body inside the NEW cap but past the OLD ceiling was terminated inside the request
   * conduit, which closes the connection before any handler runs, so the caller saw a connection reset with no
   * status at all rather than either the documented 413 or the answer it asked for. Nothing can repair that from
   * a handler - {@code HttpServerExchange.setMaxEntitySize} calls {@code maxEntitySizeUpdated}, which is an empty
   * method for HTTP/1.1 - so the ceiling is off and the per-request readers are the enforcement. This test is
   * what keeps it off. Sized past where that ceiling used to sit, the old cap plus a 1 MB allowance.
   */
  @Test
  void raisingTheLimitAtRuntimeRaisesTheUndertowCeilingWithIt() throws Exception {
    getServer(0).getConfiguration().setValue(GlobalConfiguration.SERVER_HTTP_BODY_CONTENT_MAX_SIZE, 8L * 1024L * 1024L);
    try {
      final Response response = postChunkedBody("/api/v1/command/" + getDatabaseName(), "application/json",
          paddedCommand(3 * 1024 * 1024));
      assertThat(response.statusCode)
          .as("a body inside the raised cap must be answered, not cut off by a ceiling left at the old value")
          .isNotEqualTo(-1);
      assertThat(response.statusCode).as("and not refused as too large either").isNotEqualTo(413);
    } finally {
      getServer(0).getConfiguration().setValue(GlobalConfiguration.SERVER_HTTP_BODY_CONTENT_MAX_SIZE, SMALL_LIMIT_BYTES);
    }
  }

  /** A well-formed command whose payload is padded out to roughly {@code totalBytes}. */
  private static byte[] paddedCommand(final int totalBytes) {
    final StringBuilder padding = new StringBuilder(totalBytes);
    while (padding.length() < totalBytes)
      padding.append('x');
    return ("{\"language\":\"sql\",\"command\":\"SELECT '" + padding + "' AS pad\"}")
        .getBytes(StandardCharsets.UTF_8);
  }

  /**
   * Offers {@code bodyBytes} as a chunked body and never terminates it, so only a server that enforces the cap on
   * the bytes it has read can answer at all.
   */
  private Response postChunked(final String path, final String contentType, final long bodyBytes) throws Exception {
    final byte[] filler = new byte[8192];
    Arrays.fill(filler, (byte) 'x');
    return exchange(path, contentType, filler, bodyBytes, true, false);
  }

  private Response postChunkedBody(final String path, final String contentType, final byte[] body) throws Exception {
    return exchange(path, contentType, body, body.length, true, true);
  }

  private Response postDeclaredLength(final String path, final String contentType, final long bodyBytes)
      throws Exception {
    final byte[] filler = new byte[8192];
    Arrays.fill(filler, (byte) 'x');
    return exchange(path, contentType, filler, bodyBytes, false, true);
  }

  /**
   * Sends {@code totalBytes} of {@code chunk} with {@code Transfer-Encoding: chunked} over a raw socket, so the
   * encoding is exactly the one under test and never the JDK client's choice. With {@code endBody} false the
   * terminating zero-length chunk is never written, so the request stays deliberately unfinished.
   * <p>
   * The response is drained on a separate thread started BEFORE the first body byte goes out: a server that
   * refuses mid-upload answers and closes while the client is still writing, and a reader that only runs after
   * the write loop would lose the very answer being asserted on.
   */
  private Response exchange(final String path, final String contentType, final byte[] chunk, final long totalBytes,
      final boolean chunked, final boolean endBody) throws Exception {
    final String credentials = Base64.getEncoder()
        .encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8));

    try (final Socket socket = new Socket("127.0.0.1", getServerHttpPort())) {
      socket.setSoTimeout(30_000);
      socket.setTcpNoDelay(true);

      final StringBuilder collected = new StringBuilder();
      final Thread reader = new Thread(() -> {
        try (final InputStream in = socket.getInputStream()) {
          final byte[] buffer = new byte[8192];
          int read;
          while ((read = in.read(buffer)) > 0)
            synchronized (collected) {
              collected.append(new String(buffer, 0, read, StandardCharsets.UTF_8));
            }
        } catch (final IOException ignored) {
          // The server closes the connection after refusing the body; whatever arrived first is the answer.
        }
      }, "issue7772-response-reader");
      reader.setDaemon(true);
      reader.start();

      final OutputStream out = socket.getOutputStream();
      out.write(("POST " + path + " HTTP/1.1\r\n"
          + "Host: 127.0.0.1\r\n"
          + "Authorization: Basic " + credentials + "\r\n"
          + "Content-Type: " + contentType + "\r\n"
          + (chunked ? "Transfer-Encoding: chunked\r\n" : "Content-Length: " + totalBytes + "\r\n")
          + "Connection: close\r\n"
          + "\r\n").getBytes(StandardCharsets.US_ASCII));
      out.flush();

      final AtomicLong sent = new AtomicLong();
      try {
        while (sent.get() < totalBytes && !responseStarted(collected)) {
          final int size = (int) Math.min(chunk.length, totalBytes - sent.get());
          if (chunked)
            out.write((Integer.toHexString(size) + "\r\n").getBytes(StandardCharsets.US_ASCII));
          out.write(chunk, 0, size);
          if (chunked)
            out.write("\r\n".getBytes(StandardCharsets.US_ASCII));
          out.flush();
          sent.addAndGet(size);
        }
        if (chunked && endBody && !responseStarted(collected)) {
          out.write("0\r\n\r\n".getBytes(StandardCharsets.US_ASCII));
          out.flush();
        }
      } catch (final IOException expected) {
        // The server refused the body and stopped reading before the client finished writing it. That is the
        // point of the fix: the rest of the payload never arrives.
      }

      // Stop uploading as soon as an answer is on the wire, exactly as an HTTP client that watches for an early
      // response does. A server that refuses a body mid-upload answers and then closes WITHOUT draining the rest
      // - draining is the very cost the cap exists to avoid - and a peer still pushing megabytes into a socket
      // nobody is reading turns that close into an RST, which on BSD/macOS discards the answer the client had
      // already received. Half-closing here is what makes the 413 observable instead of racy.
      try {
        socket.shutdownOutput();
      } catch (final IOException ignored) {
        // Already reset by the server; whatever arrived before that is still the answer.
      }

      reader.join(30_000);

      final String raw;
      synchronized (collected) {
        raw = collected.toString();
      }
      return new Response(statusCodeOf(raw), bodyOf(raw));
    }
  }

  private static boolean responseStarted(final StringBuilder collected) {
    synchronized (collected) {
      return collected.indexOf("\r\n\r\n") >= 0;
    }
  }

  private static int statusCodeOf(final String raw) {
    final int firstSpace = raw.indexOf(' ');
    if (firstSpace < 0)
      return -1;
    final int secondSpace = raw.indexOf(' ', firstSpace + 1);
    if (secondSpace < 0)
      return -1;
    try {
      return Integer.parseInt(raw.substring(firstSpace + 1, secondSpace).trim());
    } catch (final NumberFormatException e) {
      return -1;
    }
  }

  private static String bodyOf(final String raw) {
    final int separator = raw.indexOf("\r\n\r\n");
    return separator < 0 ? "" : raw.substring(separator + 4);
  }

  private record Response(int statusCode, String body) {
  }
}
