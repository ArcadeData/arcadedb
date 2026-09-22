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
import java.nio.charset.StandardCharsets;
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
   * Offers {@code bodyBytes} as a chunked body and never terminates it, so only a server that enforces the cap on
   * the bytes it has read can answer at all.
   */
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
