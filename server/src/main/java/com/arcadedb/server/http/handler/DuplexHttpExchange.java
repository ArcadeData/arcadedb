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
package com.arcadedb.server.http.handler;

import com.arcadedb.log.LogManager;

import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpConnectTimeoutException;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;
import java.util.logging.Level;

/**
 * A full-duplex HTTP/1.1 exchange: the request body is sent on a thread of its own while the calling thread reads the
 * response, so the response is handed back as soon as its headers arrive - while the upload is still going (issue
 * #9216).
 * <p>
 * It exists for the streamed {@code /api/v1/batch} forward from a follower to the leader. The JDK {@link HttpClient}
 * hands back an HTTP/1.1 response only once the request body has been published whole (pinned by
 * {@code Issue8674StreamedRelayBodyCapTerminalLineTest.theJdkClientHandsBackTheResponseOnlyOnceTheUploadIsPublished}),
 * so a client loading through a follower saw none of the leader's progress lines until its whole upload had been
 * relayed, and a cap trip on the follower could only ever fail the send. HTTP/2 would deliver the response mid-upload,
 * but the JDK client reaches it only through ALPN on TLS: on the plain listener the {@code h2c} upgrade does not happen
 * on a request with a body.
 * <p>
 * Built from the {@link HttpRequest} the forward already assembles, so the request line and headers are those the JDK
 * client would have sent, and from the {@link HttpClient} the dial resolved, whose connect timeout and - on an HTTPS
 * dial (issue #7508) - TLS context it reuses. The {@code https} scheme is never sent in the clear: the socket is
 * wrapped in TLS with the client's context and parameters, and the leader's host name is verified as the JDK client
 * verifies it.
 * <p>
 * What it does on each failure, matched to the JDK client the forward used before so the caller's catch arms keep
 * their meaning:
 * <ul>
 *   <li>a connect that times out throws {@link HttpConnectTimeoutException}, one that is refused a plain
 *   {@link ConnectException};</li>
 *   <li>no response headers within {@code deadlineMs} of the last byte of the upload sent throws
 *   {@link HttpTimeoutException} and closes the connection (issue #8719's bound, unchanged);</li>
 *   <li>a request body that fails - including this node's own body cap (issue #8161) - aborts the connection, so the
 *   leader sees the upload cut rather than ended. Before the response headers that fails the send with an
 *   {@link IOException} whose cause is the body's failure; after them it surfaces as a failed read of the response
 *   body, where the relay's in-band 413 (issue #8674) answers it;</li>
 *   <li>a leader that stops reading the upload, or closes, only ends the upload: an answer it already sent - an early
 *   refusal - is still read.</li>
 * </ul>
 * The body of the response is not bounded here: the caller bounds each read of it, as it did with the JDK client's
 * {@code ofInputStream()} body (issue #7738).
 * <p>
 * Two choices that differ from the JDK client on purpose:
 * <ul>
 *   <li><b>no proxy.</b> The dial's clients ({@code LeaderDial.newConnectTimeoutBoundedClient} and the HA plugin's
 *   HTTPS client) are built without {@code HttpClient.Builder.proxy}, and the JDK client then connects directly too -
 *   it does not fall back to {@code ProxySelector.getDefault()}. Peer-to-peer cluster traffic dials the peer itself, so
 *   this socket does the same;</li>
 *   <li><b>one platform thread per exchange for the upload,</b> not a pool. It lives exactly as long as the forward
 *   whose Undertow worker is blocked waiting on it, so the count is already bounded by the worker pool, and it blocks
 *   on two sockets, which is the work a pooled compute thread must not be given. The JDK client did the same work on
 *   threads of its own executor.</li>
 * </ul>
 */
final class DuplexHttpExchange implements HttpResponse<InputStream>, AutoCloseable {

  /** The longest the wait for the response headers goes between two samples of the upload's progress counter. */
  private static final long   MAX_PROGRESS_SAMPLE_MS = 1_000L;
  private static final int    UPLOAD_BUFFER          = 8_192;
  /** Bounds a single head or chunk-size line, the number of headers and the whole head, against a broken peer. */
  private static final int    MAX_HEAD_LINE          = 16 * 1_024;
  private static final int    MAX_HEADERS            = 256;
  private static final int    MAX_HEAD_BYTES         = 64 * 1_024;
  private static final byte[] CRLF                   = { '\r', '\n' };
  private static final byte[] LAST_CHUNK             = "0\r\n\r\n".getBytes(StandardCharsets.US_ASCII);
  /** The JDK client's own switch for hostname verification, honoured so both transports verify alike. */
  private static final String DISABLE_HOSTNAME_VERIFICATION = "jdk.internal.httpclient.disableHostnameVerification";
  private static final AtomicBoolean HOSTNAME_VERIFICATION_OFF_WARNED = new AtomicBoolean(false);

  private final    HttpRequest   request;
  private final    InputStream   uploadBody;
  private final    long          contentLength;
  private final    long          joinUploadMs;
  /** The TCP socket, closed to abort the exchange: closing it releases both threads whatever they are blocked in. */
  private final    Socket        rawSocket;
  private final    Socket        socket;
  private final    InputStream   in;
  private final    OutputStream  out;
  private final    AtomicBoolean closed       = new AtomicBoolean(false);
  private final    Thread        uploader;
  /** Why the request body could not be read, or {@code null}: the failure that aborted the exchange. */
  private volatile Throwable     bodyFailure;
  /**
   * One byte, for the single-byte reads of the response body. Shared by {@code LengthBody} and {@code ChunkedBody}, of
   * which an exchange has exactly one, read by one thread.
   */
  private final    byte[]        oneByte      = new byte[1];
  /** The response head is written by send() before it returns, and read only by the thread it returned to. */
  private          int           headBytes;
  private          int           statusCode;
  private          HttpHeaders   headers;
  private          InputStream   body;

  private DuplexHttpExchange(final HttpRequest request, final InputStream uploadBody, final long contentLength,
      final long joinUploadMs, final Socket rawSocket, final Socket socket) throws IOException {
    this.request = request;
    this.uploadBody = uploadBody;
    this.contentLength = contentLength;
    this.joinUploadMs = joinUploadMs;
    this.rawSocket = rawSocket;
    this.socket = socket;
    this.in = new BufferedInputStream(socket.getInputStream(), UPLOAD_BUFFER);
    this.out = new BufferedOutputStream(socket.getOutputStream(), UPLOAD_BUFFER);
    this.uploader = new Thread(this::upload, "arcadedb-batch-forward-upload");
    this.uploader.setDaemon(true);
  }

  /**
   * Sends {@code request} with {@code body} as its body, and returns as soon as the response headers have arrived. The
   * upload carries on behind the returned response until it ends, fails, or the response is {@linkplain #close()
   * closed}; the caller must close it.
   *
   * @param client     whose connect timeout and, for an {@code https} request, TLS context and parameters are used
   * @param request    the request line and headers to send; its body publisher is not used, only its declared length
   * @param body       the request body, read once, on the upload thread
   * @param deadlineMs the longest to wait for the response headers since {@code progress} last changed
   * @param progress   a counter that moves whenever the upload does, safe to read from the calling thread
   */
  static DuplexHttpExchange send(final HttpClient client, final HttpRequest request, final InputStream body,
      final long deadlineMs, final LongSupplier progress) throws IOException, InterruptedException {
    final long deadline = Math.max(deadlineMs, LeaderDial.MIN_FORWARD_TIMEOUT_MS);
    final URI uri = request.uri();
    final boolean https = "https".equalsIgnoreCase(uri.getScheme());
    // URI.getHost() keeps the brackets of an IPv6 literal ("[::1]"), which neither resolves nor matches a certificate.
    final String uriHost = uri.getHost();
    final String host = uriHost != null && uriHost.startsWith("[") && uriHost.endsWith("]") ?
        uriHost.substring(1, uriHost.length() - 1) :
        uriHost;
    final int port = uri.getPort() >= 0 ? uri.getPort() : https ? 443 : 80;
    final long contentLength = request.bodyPublisher().map(HttpRequest.BodyPublisher::contentLength).orElse(-1L);

    final Socket raw = new Socket();
    Socket socket = raw;
    DuplexHttpExchange exchange = null;
    try {
      final long connectMs = client.connectTimeout().map(Duration::toMillis).orElse(0L);
      try {
        // The name is resolved here, before the connect timeout applies, as the JDK client resolves it too: cluster
        // peers are addressed by the names the cluster is configured with.
        raw.connect(new InetSocketAddress(host, port), (int) Math.min(connectMs, Integer.MAX_VALUE));
      } catch (final SocketTimeoutException e) {
        final HttpConnectTimeoutException timeout = new HttpConnectTimeoutException(
            "HTTP connect timed out after " + connectMs + " ms");
        timeout.initCause(e);
        throw timeout;
      }
      raw.setTcpNoDelay(true);

      if (https)
        socket = startTls(client, raw, host, port, deadline);

      exchange = new DuplexHttpExchange(request, body, contentLength, deadline, raw, socket);
      exchange.writeHead();
      exchange.uploader.start();
      exchange.awaitResponse(deadline, progress);
      return exchange;
    } catch (final IOException | InterruptedException | RuntimeException e) {
      if (exchange != null)
        // The upload may be parked in a read of the client's body, which the server drains once the caller has
        // answered: it is stopped here, as close() stops it on success, so no two threads ever read that body at once.
        exchange.close();
      else
        closeQuietly(raw);
      throw e;
    }
  }

  @Override
  public int statusCode() {
    return statusCode;
  }

  @Override
  public HttpHeaders headers() {
    return headers;
  }

  /** The response body. Closing it closes the connection without waiting, so it is safe from a timer thread. */
  @Override
  public InputStream body() {
    return body;
  }

  @Override
  public HttpRequest request() {
    return request;
  }

  @Override
  public Optional<HttpResponse<InputStream>> previousResponse() {
    return Optional.empty();
  }

  @Override
  public Optional<SSLSession> sslSession() {
    return socket instanceof SSLSocket ssl ? Optional.of(ssl.getSession()) : Optional.empty();
  }

  @Override
  public URI uri() {
    return request.uri();
  }

  @Override
  public HttpClient.Version version() {
    return HttpClient.Version.HTTP_1_1;
  }

  /**
   * Closes the connection and waits, at most the deadline, for the upload thread to stop. The upload reads the
   * client's request body, which the server goes on to drain once the handler returns, so it must not be left reading
   * it: with the connection closed it stops at its next write, or after its current read of the client returns.
   * <p>
   * Called by the handler thread that relayed the answer, never by an I/O thread: the silence timer closes the
   * {@linkplain #body() body} instead, which does not wait. A client that stopped sending holds that handler thread
   * here no longer than the deadline, and its own connection read timeout ends that read sooner.
   */
  @Override
  public void close() {
    abort();
    try {
      uploader.join(joinUploadMs);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    // A WARNING: the thread is not lost - it ends with its current read of the client - but it outlives the forward
    // that bounded it, and an operator seeing this repeatedly has clients that stall mid-upload.
    if (uploader.isAlive())
      LogManager.instance().log(this, Level.WARNING,
          "The upload relaying a batch to %s was still reading the client's body %,d ms after the forward ended",
          request.uri(), joinUploadMs);
  }

  /** Closes the connection without waiting for anything: releases both threads, and is safe from any thread. */
  private void abort() {
    if (closed.compareAndSet(false, true))
      closeQuietly(rawSocket);
  }

  private static Socket startTls(final HttpClient client, final Socket raw, final String host, final int port,
      final long deadlineMs) throws IOException {
    // Layered on the connected socket so the connect timeout above covers TLS dials as well. The host name passed here
    // is what SNI announces and what endpoint identification checks the certificate against.
    final SSLSocket ssl = (SSLSocket) client.sslContext().getSocketFactory().createSocket(raw, host, port, true);
    final SSLParameters parameters = client.sslParameters();
    if (!Boolean.getBoolean(DISABLE_HOSTNAME_VERIFICATION))
      parameters.setEndpointIdentificationAlgorithm("HTTPS");
    else if (HOSTNAME_VERIFICATION_OFF_WARNED.compareAndSet(false, true))
      LogManager.instance().log(DuplexHttpExchange.class, Level.WARNING,
          "'%s' is set, so the leader's TLS certificate is not checked against its host name when a streamed batch is "
              + "forwarded to it. This notice is logged only once.", DISABLE_HOSTNAME_VERIFICATION);
    ssl.setSSLParameters(parameters);
    ssl.setSoTimeout((int) Math.min(deadlineMs, Integer.MAX_VALUE));
    try {
      ssl.startHandshake();
    } catch (final SocketTimeoutException e) {
      final HttpTimeoutException timeout = new HttpTimeoutException("TLS handshake not completed within " + deadlineMs + " ms");
      timeout.initCause(e);
      throw timeout;
    }
    return ssl;
  }

  /** The request line and headers, as the JDK client builds them, plus the framing and {@code Connection: close}. */
  private void writeHead() throws IOException {
    final URI uri = request.uri();
    final String path = uri.getRawPath() == null || uri.getRawPath().isEmpty() ? "/" : uri.getRawPath();
    final StringBuilder head = new StringBuilder(512);
    head.append(request.method()).append(' ').append(path);
    if (uri.getRawQuery() != null)
      head.append('?').append(uri.getRawQuery());
    head.append(" HTTP/1.1\r\nHost: ").append(uri.getRawAuthority()).append("\r\n");
    // HttpRequest.Builder has already refused any name or value carrying a line break.
    for (final Map.Entry<String, List<String>> header : request.headers().map().entrySet())
      for (final String value : header.getValue())
        head.append(header.getKey()).append(": ").append(value).append("\r\n");
    if (contentLength >= 0)
      head.append("Content-Length: ").append(contentLength).append("\r\n");
    else
      head.append("Transfer-Encoding: chunked\r\n");
    // One exchange per connection: the response may end before the upload does, so the connection cannot be reused.
    head.append("Connection: close\r\n\r\n");
    out.write(head.toString().getBytes(StandardCharsets.ISO_8859_1));
    out.flush();
  }

  /**
   * The upload thread. Each piece read from the body is sent at once, so the leader can acknowledge it while the next
   * one is still on its way. A body that fails aborts the exchange; a write that fails only ends the upload, because
   * the leader may have answered and closed, and that answer still has to be read.
   */
  private void upload() {
    final byte[] buffer = new byte[UPLOAD_BUFFER];
    long sent = 0;
    try {
      while (contentLength < 0 || sent < contentLength) {
        final int max = contentLength < 0 ? buffer.length : (int) Math.min(buffer.length, contentLength - sent);
        final int read;
        try {
          read = uploadBody.read(buffer, 0, max);
        } catch (final IOException | RuntimeException e) {
          failBody(e);
          return;
        }
        if (read < 0)
          break;
        if (read == 0) {
          // A blocking stream asked for at least one byte must not answer none: retrying it would spin.
          failBody(new IOException("The request body returned no bytes from a blocking read"));
          return;
        }
        if (contentLength < 0) {
          out.write(Integer.toHexString(read).getBytes(StandardCharsets.US_ASCII));
          out.write(CRLF);
          out.write(buffer, 0, read);
          out.write(CRLF);
        } else
          out.write(buffer, 0, read);
        out.flush();
        sent += read;
      }

      if (contentLength >= 0 && sent < contentLength) {
        // The JDK client fails a body shorter than it declared too: the leader must not read it as one that ended.
        failBody(new IOException("The request body ended after " + sent + " of the " + contentLength
            + " bytes it declared"));
        return;
      }
      if (contentLength < 0) {
        out.write(LAST_CHUNK);
        out.flush();
      }
    } catch (final IOException e) {
      // The leader closed, or the exchange was aborted: either way nothing more can be sent, and the response - if
      // there is one - is the reader's.
      LogManager.instance().log(this, Level.FINE, "The upload relaying a batch to %s ended after %,d bytes: %s",
          request.uri(), sent, e.getMessage());
    }
  }

  private void failBody(final Throwable failure) {
    bodyFailure = failure;
    abort();
  }

  /**
   * Reads the status line and headers, skipping interim {@code 1xx} answers. The read is sampled rather than blocked
   * on, so the deadline counts from the last byte of the upload sent (issue #8719) and an interrupt is noticed.
   */
  private void awaitResponse(final long deadlineMs, final LongSupplier progress) throws IOException, InterruptedException {
    final long deadlineNanos = TimeUnit.MILLISECONDS.toNanos(deadlineMs);
    final int sampleMs = (int) Math.max(1L, Math.min(deadlineMs / 4, MAX_PROGRESS_SAMPLE_MS));
    final long[] still = { progress.getAsLong(), System.nanoTime() };
    final ByteArrayOutputStream line = new ByteArrayOutputStream(128);

    socket.setSoTimeout(sampleMs);
    try {
      do {
        final String status = readHeadLine(line, deadlineNanos, progress, still);
        statusCode = parseStatus(status);
        final Map<String, List<String>> fields = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (int count = 0; ; count++) {
          final String field = readHeadLine(line, deadlineNanos, progress, still);
          if (field.isEmpty())
            break;
          if (count >= MAX_HEADERS)
            throw new IOException("The response from " + request.uri() + " carries more than " + MAX_HEADERS + " headers");
          final int colon = field.indexOf(':');
          if (colon <= 0)
            throw new IOException("Malformed response header from " + request.uri() + ": " + field);
          fields.computeIfAbsent(field.substring(0, colon).trim(), k -> new ArrayList<>(1))
              .add(field.substring(colon + 1).trim());
        }
        headers = HttpHeaders.of(fields, (name, value) -> true);
        // 101 would hand the connection over to another protocol, which this exchange never asked for.
        if (statusCode == 101)
          throw new IOException("Unexpected 101 Switching Protocols from " + request.uri());
      } while (statusCode >= 100 && statusCode < 200);
      // From here on the caller bounds each read of the body itself.
      socket.setSoTimeout(0);
    } catch (final IOException e) {
      throw withBodyFailure(e);
    }

    body = new ResponseBody(responseBodyFraming());
  }

  /**
   * One line of the response head, without its line terminator. A read that times out is a sample: the wait goes on
   * while the upload moves, and gives up once it has stood still for the whole deadline.
   */
  private String readHeadLine(final ByteArrayOutputStream line, final long deadlineNanos, final LongSupplier progress,
      final long[] still) throws IOException, InterruptedException {
    line.reset();
    while (true) {
      final int b;
      try {
        b = in.read();
      } catch (final SocketTimeoutException e) {
        if (Thread.interrupted()) {
          abort();
          throw new InterruptedException("Interrupted while waiting for " + request.uri() + " to answer");
        }
        failIfStill(deadlineNanos, progress, still);
        continue;
      }
      // Also after a byte that arrived: only the upload counts as progress, so a peer dripping its head a byte at a
      // time is given up on like one that sends nothing (it would otherwise reset the socket timeout forever).
      failIfStill(deadlineNanos, progress, still);
      if (b < 0)
        throw new IOException("The connection to " + request.uri() + " closed before a complete response head");
      if (++headBytes > MAX_HEAD_BYTES)
        throw new IOException("The response head from " + request.uri() + " is longer than " + MAX_HEAD_BYTES + " bytes");
      if (b == '\n') {
        final byte[] bytes = line.toByteArray();
        final int length = bytes.length > 0 && bytes[bytes.length - 1] == '\r' ? bytes.length - 1 : bytes.length;
        return new String(bytes, 0, length, StandardCharsets.ISO_8859_1);
      }
      if (line.size() >= MAX_HEAD_LINE)
        throw new IOException("A response head line from " + request.uri() + " is longer than " + MAX_HEAD_LINE + " bytes");
      line.write(b);
    }
  }

  /** Gives up once the upload has not moved for the whole deadline; a move restarts the window. */
  private void failIfStill(final long deadlineNanos, final LongSupplier progress, final long[] still)
      throws HttpTimeoutException {
    final long now = progress.getAsLong();
    if (now != still[0]) {
      still[0] = now;
      still[1] = System.nanoTime();
    } else if (System.nanoTime() - still[1] >= deadlineNanos) {
      abort();
      throw new HttpTimeoutException("No complete response from " + request.uri() + " within "
          + TimeUnit.NANOSECONDS.toMillis(deadlineNanos) + " ms of the last byte of the upload sent");
    }
  }

  private int parseStatus(final String status) throws IOException {
    // "HTTP/1.1 200 OK": the reason phrase is optional.
    if (status.startsWith("HTTP/1.")) {
      final int space = status.indexOf(' ');
      if (space > 0 && status.length() >= space + 4)
        try {
          return Integer.parseInt(status.substring(space + 1, space + 4));
        } catch (final NumberFormatException ignored) {
          // reported below
        }
    }
    throw new IOException("Malformed status line from " + request.uri() + ": " + status);
  }

  private InputStream responseBodyFraming() throws IOException {
    final String transferEncoding = headers.firstValue("Transfer-Encoding").orElse("");
    final List<String> lengths = headers.allValues("Content-Length");
    // Ambiguous framing is refused rather than guessed at (RFC 9112 6.3): the relay must never pass off a body cut at
    // the wrong place as the leader's whole answer.
    if (!transferEncoding.isEmpty() && !lengths.isEmpty())
      throw new IOException("The response from " + request.uri() + " carries both Transfer-Encoding and Content-Length");
    if (lengths.size() > 1 && lengths.stream().map(String::trim).distinct().count() > 1)
      throw new IOException("The response from " + request.uri() + " carries conflicting Content-Length values " + lengths);
    if (!transferEncoding.isEmpty()) {
      // Only chunked is decoded; any other coding (gzip, ...) would be relayed as if it were the leader's lines.
      if (!transferEncoding.trim().toLowerCase(Locale.ROOT).equals("chunked"))
        throw new IOException("Unsupported Transfer-Encoding '" + transferEncoding + "' in the response from " + request.uri());
      return new ChunkedBody();
    }
    final Optional<String> length = lengths.isEmpty() ? Optional.empty() : Optional.of(lengths.getFirst());
    if (length.isPresent())
      try {
        return new LengthBody(Long.parseLong(length.get().trim()));
      } catch (final NumberFormatException e) {
        throw new IOException("Malformed Content-Length from " + request.uri() + ": " + length.get());
      }
    if (statusCode == 204 || statusCode == 304)
      return InputStream.nullInputStream();
    // Neither: the body runs until the leader closes the connection, which Connection: close allows.
    return in;
  }

  /** A failure reading the response, with the request body's own failure attached when that is what aborted it. */
  private IOException withBodyFailure(final IOException e) {
    final Throwable failure = bodyFailure;
    if (failure == null || e instanceof HttpTimeoutException)
      return e;
    final IOException wrapped = new IOException("The request body failed while relaying it to " + request.uri() + ": "
        + failure.getMessage(), failure);
    wrapped.addSuppressed(e);
    return wrapped;
  }

  private static void closeQuietly(final Socket socket) {
    try {
      socket.close();
    } catch (final IOException ignored) {
      // the connection is being abandoned either way
    }
  }

  /** The response body as the caller reads it: a read past the end of a framed body is an error, not an end. */
  private final class ResponseBody extends InputStream {
    private final InputStream framed;

    private ResponseBody(final InputStream framed) {
      this.framed = framed;
    }

    @Override
    public int read() throws IOException {
      try {
        return framed.read();
      } catch (final IOException e) {
        throw withBodyFailure(e);
      }
    }

    @Override
    public int read(final byte[] b, final int off, final int len) throws IOException {
      try {
        return framed.read(b, off, len);
      } catch (final IOException e) {
        throw withBodyFailure(e);
      }
    }

    @Override
    public int available() throws IOException {
      if (closed.get())
        return 0;
      try {
        return framed.available();
      } catch (final IOException e) {
        // Closed by the silence timer between the check and the probe: nothing is available, which is not a failure.
        if (closed.get())
          return 0;
        throw e;
      }
    }

    /** Never blocks: the relay's silence timer closes the body from the server's I/O thread. */
    @Override
    public void close() {
      abort();
    }
  }

  /** A {@code Content-Length} body. */
  private final class LengthBody extends InputStream {
    private long left;

    private LengthBody(final long length) {
      this.left = length;
    }

    @Override
    public int read() throws IOException {
      return read(oneByte, 0, 1) < 0 ? -1 : oneByte[0] & 0xFF;
    }

    @Override
    public int read(final byte[] b, final int off, final int len) throws IOException {
      if (left <= 0)
        return -1;
      final int n = in.read(b, off, (int) Math.min(len, left));
      if (n < 0)
        throw new IOException("The response from " + request.uri() + " ended " + left + " bytes before its Content-Length");
      left -= n;
      return n;
    }

    @Override
    public int available() throws IOException {
      return (int) Math.min(in.available(), left);
    }
  }

  /** A {@code Transfer-Encoding: chunked} body; a connection that ends mid-chunk is an error, not the end. */
  private final class ChunkedBody extends InputStream {
    private final StringBuilder chunkLine = new StringBuilder(16);
    private long    chunkLeft;
    private boolean ended;

    @Override
    public int read() throws IOException {
      return read(oneByte, 0, 1) < 0 ? -1 : oneByte[0] & 0xFF;
    }

    @Override
    public int read(final byte[] b, final int off, final int len) throws IOException {
      if (ended)
        return -1;
      if (len == 0)
        return 0;
      if (chunkLeft == 0) {
        chunkLeft = nextChunkSize();
        if (chunkLeft == 0) {
          // the last chunk: skip the trailers up to the empty line that ends the body
          while (!readBodyLine().isEmpty()) {
            // trailers are not relayed
          }
          ended = true;
          return -1;
        }
      }
      final int n = in.read(b, off, (int) Math.min(len, chunkLeft));
      if (n < 0)
        throw new IOException("The response from " + request.uri() + " ended in the middle of a chunk");
      chunkLeft -= n;
      if (chunkLeft == 0 && !readBodyLine().isEmpty())
        throw new IOException("Malformed chunk terminator in the response from " + request.uri());
      return n;
    }

    @Override
    public int available() throws IOException {
      return ended ? 0 : (int) Math.min(in.available(), chunkLeft);
    }

    private long nextChunkSize() throws IOException {
      final String line = readBodyLine();
      final int extension = line.indexOf(';');
      final String size = (extension >= 0 ? line.substring(0, extension) : line).trim();
      // Hex digits only, which Long.parseLong alone does not enforce (it takes a sign), and few enough to fit a long.
      boolean valid = !size.isEmpty() && size.length() <= 15;
      for (int i = 0; valid && i < size.length(); i++)
        valid = Character.digit(size.charAt(i), 16) >= 0;
      if (!valid)
        throw new IOException("Malformed chunk size in the response from " + request.uri() + ": " + line);
      return Long.parseLong(size, 16);
    }

    private String readBodyLine() throws IOException {
      final StringBuilder line = chunkLine;
      line.setLength(0);
      while (true) {
        final int c = in.read();
        if (c < 0)
          throw new IOException("The response from " + request.uri() + " ended in the middle of its chunk framing");
        if (c == '\n') {
          final int length = line.length();
          return length > 0 && line.charAt(length - 1) == '\r' ? line.substring(0, length - 1) : line.toString();
        }
        if (line.length() >= MAX_HEAD_LINE)
          throw new IOException("A chunk header in the response from " + request.uri() + " is too long");
        line.append((char) c);
      }
    }
  }
}
