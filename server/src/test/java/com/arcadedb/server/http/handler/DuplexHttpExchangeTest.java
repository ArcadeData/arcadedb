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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

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
