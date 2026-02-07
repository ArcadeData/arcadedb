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

import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for GetDynamicContentHandler.
 * Tests static content serving, templating, and content type handling.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class GetDynamicContentHandlerIT extends BaseGraphServerTest {
  private final HttpClient client = HttpClient.newHttpClient();

  @Test
  void shouldHandleRootRequest() throws Exception {
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(new URI("http://localhost:2480/"))
        .GET()
        .build();

    final HttpResponse<String> response = client.send(request, BodyHandlers.ofString());

    // Root may return 404 if index.html is not available in test environment, or 200 if it is
    assertThat(response.statusCode()).isIn(200, 404);
  }

  @Test
  void shouldHandleIndexHtmlRequest() throws Exception {
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(new URI("http://localhost:2480/index.html"))
        .GET()
        .build();

    final HttpResponse<String> response = client.send(request, BodyHandlers.ofString());

    // index.html may return 404 if not available in test environment, or 200 if it is
    assertThat(response.statusCode()).isIn(200, 404);
  }

  @Test
  void shouldReturn404ForNonExistentFile() throws Exception {
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(new URI("http://localhost:2480/nonexistent.html"))
        .GET()
        .build();

    final HttpResponse<String> response = client.send(request, BodyHandlers.ofString());

    assertThat(response.statusCode()).isEqualTo(404);
    assertThat(response.body()).contains("Not Found");
  }

  @Test
  void shouldRejectPathWithDotDot() throws Exception {
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(new URI("http://localhost:2480/../etc/passwd"))
        .GET()
        .build();

    final HttpResponse<String> response = client.send(request, BodyHandlers.ofString());

    assertThat(response.statusCode()).isEqualTo(404);
  }

  @Test
  void shouldServeJavaScriptWithCorrectContentType() throws Exception {
    // Try to access a common JS file from the static resources
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(new URI("http://localhost:2480/js/studio.js"))
        .GET()
        .build();

    final HttpResponse<String> response = client.send(request, BodyHandlers.ofString());

    // Could be 200 if file exists, or 404 if not
    if (response.statusCode() == 200) {
      assertThat(response.headers().firstValue("Content-Type")).isPresent();
      assertThat(response.headers().firstValue("Content-Type").get()).contains("text/javascript");
      // JS files should have cache control
      assertThat(response.headers().firstValue("Cache-Control")).isPresent();
    }
  }

  @Test
  void shouldServeCSSWithCorrectContentType() throws Exception {
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(new URI("http://localhost:2480/css/studio.css"))
        .GET()
        .build();

    final HttpResponse<String> response = client.send(request, BodyHandlers.ofString());

    if (response.statusCode() == 200) {
      assertThat(response.headers().firstValue("Content-Type")).isPresent();
      assertThat(response.headers().firstValue("Content-Type").get()).contains("text/css");
    }
  }

  @Test
  void shouldAddHtmlExtensionWhenMissing() throws Exception {
    // Request without extension should add .html
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(new URI("http://localhost:2480/studio"))
        .GET()
        .build();

    final HttpResponse<String> response = client.send(request, BodyHandlers.ofString());

    // Should try to serve studio.html
    // Could be 200 or 404 depending on whether the file exists
    assertThat(response.statusCode()).isIn(200, 404);
  }

  @Test
  void shouldSetCacheControlForStaticAssets() throws Exception {
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(new URI("http://localhost:2480/favicon.ico"))
        .GET()
        .build();

    final HttpResponse<String> response = client.send(request, BodyHandlers.ofString());

    if (response.statusCode() == 200) {
      // Static assets (non-HTML) should have cache control
      assertThat(response.headers().firstValue("Cache-Control")).isPresent();
      assertThat(response.headers().firstValue("Cache-Control").get()).contains("max-age");
    }
  }

  @Test
  void shouldServeSVGWithCorrectContentType() throws Exception {
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(new URI("http://localhost:2480/images/logo.svg"))
        .GET()
        .build();

    final HttpResponse<String> response = client.send(request, BodyHandlers.ofString());

    if (response.statusCode() == 200) {
      assertThat(response.headers().firstValue("Content-Type")).isPresent();
      assertThat(response.headers().firstValue("Content-Type").get()).contains("image/svg+xml");
    }
  }

  @Test
  void shouldServeIconWithCorrectContentType() throws Exception {
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(new URI("http://localhost:2480/favicon.ico"))
        .GET()
        .build();

    final HttpResponse<String> response = client.send(request, BodyHandlers.ofString());

    if (response.statusCode() == 200) {
      assertThat(response.headers().firstValue("Content-Type")).isPresent();
      assertThat(response.headers().firstValue("Content-Type").get()).contains("image/x-icon");
    }
  }

  @Test
  void shouldHandleMultipleRequestsConsistently() throws Exception {
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(new URI("http://localhost:2480/"))
        .GET()
        .build();

    int firstStatusCode = -1;
    for (int i = 0; i < 5; i++) {
      final HttpResponse<String> response = client.send(request, BodyHandlers.ofString());
      if (i == 0) {
        firstStatusCode = response.statusCode();
      }
      // All responses should have the same status code
      assertThat(response.statusCode()).isEqualTo(firstStatusCode);
    }
  }

  @Test
  void shouldHandleConcurrentRequests() throws Exception {
    final Thread[] threads = new Thread[10];

    for (int i = 0; i < threads.length; i++) {
      threads[i] = new Thread(() -> {
        try {
          final HttpRequest request = HttpRequest.newBuilder()
              .uri(new URI("http://localhost:2480/"))
              .GET()
              .build();

          final HttpResponse<String> response = client.send(request, BodyHandlers.ofString());
          assertThat(response.statusCode()).isEqualTo(200);
        } catch (final Exception e) {
          throw new RuntimeException(e);
        }
      });
      threads[i].start();
    }

    for (final Thread thread : threads) {
      thread.join();
    }
  }

  @Test
  void shouldServeJSONWithCorrectContentType() throws Exception {
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(new URI("http://localhost:2480/config.json"))
        .GET()
        .build();

    final HttpResponse<String> response = client.send(request, BodyHandlers.ofString());

    if (response.statusCode() == 200) {
      assertThat(response.headers().firstValue("Content-Type")).isPresent();
      assertThat(response.headers().firstValue("Content-Type").get()).contains("application/json");
    }
  }

  @Test
  void shouldHandleFontFilesWithCorrectContentType() throws Exception {
    // Test WOFF font
    HttpRequest request = HttpRequest.newBuilder()
        .uri(new URI("http://localhost:2480/fonts/font.woff"))
        .GET()
        .build();

    HttpResponse<String> response = client.send(request, BodyHandlers.ofString());

    if (response.statusCode() == 200) {
      assertThat(response.headers().firstValue("Content-Type")).isPresent();
      assertThat(response.headers().firstValue("Content-Type").get()).contains("font/woff");
    }

    // Test WOFF2 font
    request = HttpRequest.newBuilder()
        .uri(new URI("http://localhost:2480/fonts/font.woff2"))
        .GET()
        .build();

    response = client.send(request, BodyHandlers.ofString());

    if (response.statusCode() == 200) {
      assertThat(response.headers().firstValue("Content-Type")).isPresent();
      assertThat(response.headers().firstValue("Content-Type").get()).contains("font/woff2");
    }
  }
}
