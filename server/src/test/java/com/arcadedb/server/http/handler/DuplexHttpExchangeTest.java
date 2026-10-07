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

import com.arcadedb.server.http.FakeLeader;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The HTTP/1.1 framing of {@link DuplexHttpExchange} (issue #9216), the transport of the streamed batch forward: how it
 * reads each shape of answer a leader - or something answering in its place - can send, and how it refuses the ones
 * it cannot read safely. The forward's behaviour end to end is {@code Issue9216StreamedForwardIncrementalAcksTest}.
 */
class DuplexHttpExchangeTest {

  private static final long   DEADLINE_MS = 10_000L;
  private static final byte[] PAYLOAD     = "{\"@type\":\"vertex\",\"type\":\"V\"}\n".getBytes(StandardCharsets.UTF_8);

  private final HttpClient client = HttpClient.newHttpClient();

  @AfterEach
  void closeClient() {
    client.shutdownNow();
  }

  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  void aChunkedAnswerWithTrailersIsReadWhole() throws Exception {
    try (final FakeLeader leader = FakeLeader.scripted(out -> {
      write(out, "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nContent-Type: application/x-ndjson\r\n\r\n"
          + "6;ext=1\r\nhello \r\n5\r\nworld\r\n0\r\nX-Trailer: ignored\r\n\r\n");
    })) {
      try (final DuplexHttpExchange response = send(leader, PAYLOAD.length)) {
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.headers().firstValue("content-type")).hasValue("application/x-ndjson");
        assertThat(readAll(response.body())).isEqualTo("hello world");
      }
    }
  }

  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  void aChunkedAnswerCutMidChunkIsAFailureNotAnEnd() throws Exception {
    try (final FakeLeader leader = FakeLeader.scripted(out -> {
      write(out, "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\na\r\nabc");
      out.close();
    })) {
      try (final DuplexHttpExchange response = sendNothing(leader)) {
        assertThatThrownBy(() -> readAll(response.body())).isInstanceOf(IOException.class)
            .hasMessageContaining("middle of a chunk");
      }
    }
  }

  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  void aContentLengthAnswerCutShortIsAFailure() throws Exception {
    try (final FakeLeader leader = FakeLeader.scripted(out -> {
      write(out, "HTTP/1.1 400 Bad Request\r\nContent-Length: 50\r\n\r\n{\"error\":");
      out.close();
    })) {
      try (final DuplexHttpExchange response = sendNothing(leader)) {
        assertThat(response.statusCode()).isEqualTo(400);
        assertThatThrownBy(() -> readAll(response.body())).isInstanceOf(IOException.class)
            .hasMessageContaining("before its Content-Length");
      }
    }
  }

  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  void anAnswerWithNoFramingRunsUntilTheLeaderCloses() throws Exception {
    try (final FakeLeader leader = FakeLeader.scripted(out -> {
      write(out, "HTTP/1.1 503 Service Unavailable\r\nConnection: close\r\n\r\n{\"error\":\"down\"}");
      out.close();
    })) {
      try (final DuplexHttpExchange response = sendNothing(leader)) {
        assertThat(response.statusCode()).isEqualTo(503);
        assertThat(readAll(response.body())).isEqualTo("{\"error\":\"down\"}");
      }
    }
  }

  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  void anInterim100ContinueIsSkipped() throws Exception {
    try (final FakeLeader leader = FakeLeader.scripted(out -> {
      write(out, "HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok");
    })) {
      try (final DuplexHttpExchange response = send(leader, PAYLOAD.length)) {
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(readAll(response.body())).isEqualTo("ok");
      }
    }
  }

  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  void heads101AndMalformedStatusLinesAreRefused() throws Exception {
    for (final String head : new String[] { "HTTP/1.1 101 Switching Protocols\r\nUpgrade: h2c\r\n\r\n",
        "SSH-2.0-OpenSSH\r\n\r\n", "HTTP/1.1 2xx OK\r\n\r\n", "HTTP/1.1 200 OK\r\nno-colon-here\r\n\r\n" })
      try (final FakeLeader leader = FakeLeader.scripted(out -> write(out, head))) {
        assertThatThrownBy(() -> send(leader, PAYLOAD.length).close()).as(head).isInstanceOf(IOException.class);
      }
  }

  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  void ambiguousFramingIsRefused() throws Exception {
    for (final String head : new String[] { "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nContent-Length: 5\r\n\r\n",
        "HTTP/1.1 200 OK\r\nContent-Length: 5\r\nContent-Length: 6\r\n\r\n" })
      try (final FakeLeader leader = FakeLeader.scripted(out -> write(out, head))) {
        assertThatThrownBy(() -> send(leader, PAYLOAD.length).close()).as(head).isInstanceOf(IOException.class)
            .hasMessageContaining("Content-Length");
      }
  }

  /** A head dripped a byte at a time keeps the socket busy but is not an answer: the deadline still applies. */
  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  void aHeadDrippedAByteAtATimeIsGivenUpOn() throws Exception {
    try (final FakeLeader leader = FakeLeader.scripted(out -> {
      write(out, "HTTP/1.1 200 OK\r\nX-Drip: ");
      for (int i = 0; i < 150; i++) {
        write(out, "a");
        try {
          Thread.sleep(200);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        }
      }
    })) {
      final HttpRequest request = PostBatchHandler.buildForwardRequest("http://" + leader.address() + "/api/v1/batch/mydb",
          "application/x-ndjson", "test-token", "root", PAYLOAD.length, new ByteArrayInputStream(PAYLOAD),
          NdJsonResultStream.CONTENT_TYPE, null, null);

      final StallAwareStopwatch watch = StallAwareStopwatch.start();
      assertThatThrownBy(() -> DuplexHttpExchange.send(client, request, new ByteArrayInputStream(PAYLOAD), 1_000L,
          () -> 0L).close()).isInstanceOf(HttpTimeoutException.class);
      watch.assertGaveUpWithin(15_000L, "a 1s bound on a dripped head from a wait the drip renews forever (30s)");
    }
  }

  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  void aSignedChunkSizeIsRefused() throws Exception {
    try (final FakeLeader leader = FakeLeader.scripted(out -> write(out,
        "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n+5\r\nhello\r\n0\r\n\r\n"))) {
      try (final DuplexHttpExchange response = send(leader, PAYLOAD.length)) {
        assertThatThrownBy(() -> readAll(response.body())).isInstanceOf(IOException.class)
            .hasMessageContaining("Malformed chunk size");
      }
    }
  }

  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  void aHeadLargerThanTheCapIsRefused() throws Exception {
    final StringBuilder head = new StringBuilder("HTTP/1.1 200 OK\r\n");
    for (int i = 0; i < 100; i++)
      head.append("X-Big-").append(i).append(": ").append("v".repeat(1_000)).append("\r\n");
    head.append("\r\n");
    try (final FakeLeader leader = FakeLeader.scripted(out -> write(out, head.toString()))) {
      assertThatThrownBy(() -> send(leader, PAYLOAD.length).close()).isInstanceOf(IOException.class)
          .hasMessageContaining("response head");
    }
  }

  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  void a204HasNoBody() throws Exception {
    try (final FakeLeader leader = FakeLeader.scripted(out -> write(out, "HTTP/1.1 204 No Content\r\n\r\n"))) {
      try (final DuplexHttpExchange response = send(leader, PAYLOAD.length)) {
        assertThat(response.statusCode()).isEqualTo(204);
        assertThat(readAll(response.body())).isEmpty();
      }
    }
  }

  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  void aTransferEncodingOtherThanChunkedIsRefused() throws Exception {
    try (final FakeLeader leader = FakeLeader.scripted(out -> write(out, "HTTP/1.1 200 OK\r\nTransfer-Encoding: gzip\r\n\r\n"))) {
      assertThatThrownBy(() -> send(leader, PAYLOAD.length).close()).isInstanceOf(IOException.class)
          .hasMessageContaining("Unsupported Transfer-Encoding");
    }
  }

  /**
   * A send that fails stops its upload before it returns, as a successful one does on close: the upload reads the
   * client's body, which the server drains once the handler has answered, and two threads must never read it at once.
   */
  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  void aFailedSendStopsReadingTheClientsBodyBeforeItReturns() throws Exception {
    final CountDownLatch inFirstRead = new CountDownLatch(1);
    final AtomicBoolean inRead = new AtomicBoolean();
    final AtomicInteger reads = new AtomicInteger();
    final InputStream clientBody = new InputStream() {
      @Override
      public int read() throws IOException {
        throw new UnsupportedOperationException();
      }

      @Override
      public int read(final byte[] b, final int off, final int len) {
        inRead.set(true);
        reads.incrementAndGet();
        try {
          inFirstRead.countDown();
          // A client that is slow to send: the read returns after a while, with a byte.
          Thread.sleep(1_000);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        } finally {
          inRead.set(false);
        }
        b[off] = '\n';
        return 1;
      }
    };

    try (final FakeLeader leader = FakeLeader.scripted(out -> {
      try {
        inFirstRead.await(DEADLINE_MS, TimeUnit.MILLISECONDS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      write(out, "SSH-2.0-not-http\r\n\r\n");
    })) {
      final HttpRequest request = PostBatchHandler.buildForwardRequest("http://" + leader.address() + "/api/v1/batch/mydb",
          "application/x-ndjson", "test-token", "root", -1, clientBody, NdJsonResultStream.CONTENT_TYPE, null, null);

      assertThatThrownBy(() -> DuplexHttpExchange.send(client, request, clientBody, DEADLINE_MS, reads::get))
          .isInstanceOf(IOException.class).hasMessageContaining("Malformed status line");

      assertThat(inRead.get()).as("no read of the client's body is left running").isFalse();
      final int readsWhenSendFailed = reads.get();
      Thread.sleep(1_500);
      assertThat(reads.get()).as("and none starts afterwards").isEqualTo(readsWhenSendFailed);
    }
  }

  /** The JDK client fails a body shorter than it declared, and so does this: the leader must not read it as ended. */
  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  void aBodyShorterThanItsDeclaredLengthFailsTheSend() throws Exception {
    try (final FakeLeader leader = FakeLeader.draining()) {
      assertThatThrownBy(() -> send(leader, PAYLOAD.length * 10L).close()).isInstanceOf(IOException.class)
          .hasMessageContaining("request body failed")
          .hasRootCauseMessage("The request body ended after " + PAYLOAD.length + " of the " + PAYLOAD.length * 10L
              + " bytes it declared");
      assertThat(leader.awaitConnectionClosedByClient(DEADLINE_MS, TimeUnit.MILLISECONDS))
          .as("the connection is aborted, so the leader sees the upload cut").isTrue();
    }
  }

  /** An upload declaring {@code Content-Length: 0} used to make {@code buildForwardRequest} throw outside the forward's catch arms. */
  @Test
  void anEmptyDeclaredBodyIsForwardedWithContentLengthZero() {
    final HttpRequest request = PostBatchHandler.buildForwardRequest("http://127.0.0.1:1/api/v1/batch/mydb",
        "application/x-ndjson", "test-token", "root", 0, InputStream.nullInputStream());
    assertThat(request.bodyPublisher().orElseThrow().contentLength()).isZero();
  }

  // ------------------------------------------------------------------------------------------------------------

  private DuplexHttpExchange send(final FakeLeader leader, final long declaredLength) throws Exception {
    final HttpRequest request = PostBatchHandler.buildForwardRequest("http://" + leader.address() + "/api/v1/batch/mydb",
        "application/x-ndjson", "test-token", "root", declaredLength, new ByteArrayInputStream(PAYLOAD),
        NdJsonResultStream.CONTENT_TYPE, null, null);
    return DuplexHttpExchange.send(client, request, new ByteArrayInputStream(PAYLOAD), DEADLINE_MS, () -> 0L);
  }

  /**
   * An exchange with an empty body, for the leaders that close the connection: a leader closing with upload bytes
   * still unread in its receive buffer resets the connection, which would discard the very answer under test.
   */
  private DuplexHttpExchange sendNothing(final FakeLeader leader) throws Exception {
    final HttpRequest request = PostBatchHandler.buildForwardRequest("http://" + leader.address() + "/api/v1/batch/mydb",
        "application/x-ndjson", "test-token", "root", 0, InputStream.nullInputStream(), NdJsonResultStream.CONTENT_TYPE,
        null, null);
    return DuplexHttpExchange.send(client, request, InputStream.nullInputStream(), DEADLINE_MS, () -> 0L);
  }

  private static String readAll(final InputStream in) throws IOException {
    return new String(in.readAllBytes(), StandardCharsets.UTF_8);
  }

  private static void write(final OutputStream out, final String text) throws IOException {
    out.write(text.getBytes(StandardCharsets.ISO_8859_1));
    out.flush();
  }
}
