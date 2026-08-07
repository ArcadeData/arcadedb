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

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Flow;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A batch load that lands on a follower is relayed to the leader, and that hop must not be able to deliver less than
 * the client sent without anyone noticing (issue #5618).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PostBatchHandlerForwardRequestTest {

  private static final String PAYLOAD = "{\"@type\":\"vertex\",\"@class\":\"V1\",\"@id\":\"a\"}\n";

  /**
   * The relayed request has to announce the same length the client did. Without it the JDK client sends the body
   * chunked, and a chunked body announces nothing - which disables the leader's own truncation check, so a relayed
   * upload that ended early came back as a successful load with a partial count.
   */
  @Test
  void aRelayedPayloadDeclaresTheLengthTheClientAnnounced() {
    final byte[] body = PAYLOAD.getBytes(StandardCharsets.UTF_8);

    final HttpRequest request = PostBatchHandler.buildForwardRequest("http://leader:2480/api/v1/batch/db",
        "application/x-ndjson", "token", "root", body.length, new ByteArrayInputStream(body));

    assertThat(request.bodyPublisher()).isPresent();
    assertThat(request.bodyPublisher().get().contentLength()).isEqualTo(body.length);
  }

  /**
   * A client that uploaded chunked announced nothing to relay, and inventing a length would truncate the payload at
   * that many bytes. The hop stays chunked, exactly as the client's own was.
   */
  @Test
  void aChunkedUploadIsRelayedWithoutALength() {
    final HttpRequest request = PostBatchHandler.buildForwardRequest("http://leader:2480/api/v1/batch/db",
        "application/x-ndjson", "token", "root", -1,
        new ByteArrayInputStream(PAYLOAD.getBytes(StandardCharsets.UTF_8)));

    assertThat(request.bodyPublisher().get().contentLength()).isEqualTo(-1);
  }

  /**
   * HTTP/2 on a plaintext connection means an h2c upgrade, whose failure mode is re-sending a request whose body
   * cannot be rewound. The hop is pinned to HTTP/1.1 so the question never arises.
   */
  @Test
  void theRelayIsPinnedToHttp11() {
    final HttpRequest request = PostBatchHandler.buildForwardRequest("http://leader:2480/api/v1/batch/db",
        "application/x-ndjson", "token", "root", PAYLOAD.length(),
        new ByteArrayInputStream(PAYLOAD.getBytes(StandardCharsets.UTF_8)));

    assertThat(request.version()).contains(HttpClient.Version.HTTP_1_1);
  }

  /**
   * The heart of it. {@code BodyPublishers.ofInputStream} takes a supplier because the JDK may subscribe more than
   * once and expects a FRESH stream every time; there is only one request body here, so handing the same one back a
   * second time would relay a payload starting in the middle of the file - a load missing its beginning, reported as
   * a success, with an "unknown temporary ID" for a vertex that is right there in the payload. It refuses instead.
   */
  @Test
  void theBodyCanOnlyBeHandedOverOnce() {
    final InputStream body = new ByteArrayInputStream(PAYLOAD.getBytes(StandardCharsets.UTF_8));
    final HttpRequest request = PostBatchHandler.buildForwardRequest("http://leader:2480/api/v1/batch/db",
        "application/x-ndjson", "token", "root", PAYLOAD.length(), body);

    // First subscription gets the body, as the single real send does.
    final HttpRequest.BodyPublisher publisher = request.bodyPublisher().get();
    publisher.subscribe(new DiscardingSubscriber());

    assertThatThrownBy(() -> publisher.subscribe(new DiscardingSubscriber()))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("can only be read once");
  }

  /**
   * The headers the leader authenticates the hop with must be on the relayed request, or the load is rejected before
   * a single line of it is parsed.
   */
  @Test
  void theRelayCarriesTheClusterTokenAndTheForwardedUser() {
    final HttpRequest request = PostBatchHandler.buildForwardRequest("http://leader:2480/api/v1/batch/db",
        "text/csv", "the-token", "importer", -1,
        new ByteArrayInputStream(PAYLOAD.getBytes(StandardCharsets.UTF_8)));

    assertThat(request.headers().firstValue("X-ArcadeDB-Cluster-Token")).contains("the-token");
    assertThat(request.headers().firstValue("X-ArcadeDB-Forwarded-User")).contains("importer");
    assertThat(request.headers().firstValue("Content-Type")).contains("text/csv");
  }

  /**
   * A subscriber that cancels at once: enough to make the publisher hand out its single stream without reading it.
   */
  private static class DiscardingSubscriber implements Flow.Subscriber<ByteBuffer> {
    @Override
    public void onSubscribe(final Flow.Subscription subscription) {
      subscription.cancel();
    }

    @Override
    public void onNext(final ByteBuffer item) {
    }

    @Override
    public void onError(final Throwable throwable) {
    }

    @Override
    public void onComplete() {
    }
  }
}
