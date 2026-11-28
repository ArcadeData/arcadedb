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
package com.arcadedb.bench.workload;

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.test.support.ServerWrapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;

import static com.arcadedb.bench.server.ServerUnderTest.PASSWORD;

/**
 * Posts InfluxDB line protocol bodies to {@code /api/v1/ts/<db>/write?precision=ms}. The bench does the posting itself, instead of
 * going through the load tests' wrapper, because the wrapper times its batches into a global Micrometer timer and the workload
 * needs one latency sample per batch in its own recorder.
 */
public final class LineProtocolClient implements AutoCloseable {
  static final int MAX_ATTEMPTS = 10;

  private static final Logger   LOGGER         = LoggerFactory.getLogger(LineProtocolClient.class);
  private static final long     RETRY_PAUSE_MS = 500L;
  private static final Duration REQUEST_TIMEOUT = Duration.ofMinutes(5);

  private final HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
  private final URI        uri;
  private final String     authorization;

  public LineProtocolClient(final ServerWrapper server, final String database) {
    this.uri = URI.create("http://" + server.host() + ":" + server.httpPort() + "/api/v1/ts/" + database + "/write?precision=ms");
    this.authorization = "Basic " + Base64.getEncoder().encodeToString(("root:" + PASSWORD).getBytes(StandardCharsets.UTF_8));
  }

  /** Releases the connections of the HTTP client. */
  @Override
  public void close() {
    client.close();
  }

  /**
   * Writes the body, one point per line. A 503 is the server's retryable answer (a conflict it cannot resolve itself, such as a
   * compaction race or a transaction prepared before a schema change, #10970), so it is retried up to {@value #MAX_ATTEMPTS}
   * times with a growing pause; the retries are part of the time of the call. Any other non-2xx answer, a 400 partial write
   * included, or a 503 on the last attempt, throws.
   */
  public void write(final String body) {
    final HttpRequest request = HttpRequest.newBuilder(uri).timeout(REQUEST_TIMEOUT).header("Authorization", authorization)
        .header("Content-Type", "text/plain").POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8)).build();
    for (int attempt = 1;; attempt++) {
      final HttpResponse<String> response;
      try {
        response = client.send(request, HttpResponse.BodyHandlers.ofString());
      } catch (final IOException e) {
        throw new IllegalStateException("Line protocol write failed: " + e, e);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("Line protocol write interrupted", e);
      }
      final int status = response.statusCode();
      if (status >= 200 && status < 300)
        return;
      if (!shouldRetry(status, attempt)) {
        throw new IllegalStateException("Line protocol write answered HTTP " + status
            + (status == 503 ? " (the retryable answer, still given after " + attempt + " attempts)" : "") + describe(response.body()));
      }
      LOGGER.info("Line protocol write answered 503 (attempt {}/{}), retrying", attempt, MAX_ATTEMPTS);
      try {
        Thread.sleep(RETRY_PAUSE_MS * attempt);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("Line protocol write interrupted", e);
      }
    }
  }

  /** True for the retryable 503 while attempts are left. */
  static boolean shouldRetry(final int status, final int attempt) {
    return status == 503 && attempt < MAX_ATTEMPTS;
  }

  private static String describe(final String body) {
    if (body == null || body.isBlank())
      return "";
    try {
      return ": " + new JSONObject(body);
    } catch (final RuntimeException e) {
      return ": " + body;
    }
  }
}
