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
package com.arcadedb.utility;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for GHSA-4w2m-77c8-83mw, the incomplete fix of CVE-2026-54077.
 *
 * <p>The 26.6.1 remediation validated the import URL's hostname and then handed the raw URL string to
 * {@code URL.openConnection()}. {@link java.net.HttpURLConnection} follows same-protocol 3xx redirects by default and
 * the redirect target was never revalidated, so an attacker-controlled public host answering
 * {@code 302 Location: http://169.254.169.254/latest/meta-data/...} walked straight past the block-list. That was the
 * reporter's "easiest PoC" and it is what these tests reproduce, against the shared fetcher that now backs both
 * {@code IMPORT DATABASE} and Cypher {@code LOAD CSV}.</p>
 *
 * <p>The origin server here plays the role of the attacker's public host: the block-predicate used in each test lets
 * loopback through and blocks only the link-local range, so the first hop is permitted exactly as a real public host
 * would be, and the assertion is about what happens to the <em>redirect target</em>. No request is ever sent to
 * 169.254.169.254 - it is refused before connecting.</p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SafeHttpFetcherTest {

  private static final String SECRET = "4w2m-internal-sentinel";

  /** Mirrors a real block-list closely enough for the test while letting the loopback origin server through. */
  private static final Predicate<InetAddress> BLOCK_LINK_LOCAL = InetAddress::isLinkLocalAddress;

  private HttpServer server;
  private String     baseUrl;

  @BeforeEach
  void startServer() throws IOException {
    server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
    baseUrl = "http://127.0.0.1:" + server.getAddress().getPort();

    server.createContext("/content", exchange -> {
      final byte[] body = SECRET.getBytes(StandardCharsets.UTF_8);
      exchange.sendResponseHeaders(200, body.length);
      exchange.getResponseBody().write(body);
      exchange.close();
    });

    // The bypass: a public host that 302s to the cloud metadata endpoint.
    server.createContext("/redirect-to-metadata", exchange -> {
      exchange.getResponseHeaders().add("Location", "http://169.254.169.254/latest/meta-data/iam/security-credentials/");
      exchange.sendResponseHeaders(302, -1);
      exchange.close();
    });

    // A redirect that tries to turn a remote fetch into a local file read.
    server.createContext("/redirect-to-file", exchange -> {
      exchange.getResponseHeaders().add("Location", "file:///etc/passwd");
      exchange.sendResponseHeaders(302, -1);
      exchange.close();
    });

    // A legitimate relative redirect, to prove redirects still work.
    server.createContext("/redirect-relative", exchange -> {
      exchange.getResponseHeaders().add("Location", "/content");
      exchange.sendResponseHeaders(302, -1);
      exchange.close();
    });

    // An endless redirect loop.
    server.createContext("/loop", exchange -> {
      exchange.getResponseHeaders().add("Location", baseUrl + "/loop");
      exchange.sendResponseHeaders(302, -1);
      exchange.close();
    });

    server.start();
  }

  @AfterEach
  void stopServer() {
    if (server != null)
      server.stop(0);
  }

  @Test
  void refusesRedirectToBlockedAddress() {
    // THE regression: the first hop is allowed (public host), the redirect target is link-local and must be refused.
    // On the vulnerable build the JDK followed this redirect silently and returned the metadata response.
    assertThatThrownBy(() -> SafeHttpFetcher.open(baseUrl + "/redirect-to-metadata", BLOCK_LINK_LOCAL, "IMPORT DATABASE"))
        .isInstanceOf(SecurityException.class)
        .hasMessageContaining("169.254.169.254")
        .hasMessageContaining("IMPORT DATABASE");
  }

  @Test
  void refusesRedirectToNonHttpScheme() {
    // A redirect must not be able to escalate a remote fetch into a local file read.
    assertThatThrownBy(() -> SafeHttpFetcher.open(baseUrl + "/redirect-to-file", BLOCK_LINK_LOCAL, "IMPORT DATABASE"))
        .isInstanceOf(SecurityException.class)
        .hasMessageContaining("file");
  }

  @Test
  void refusesBlockedFirstHop() {
    // The original block-list behaviour must be preserved: a directly-requested blocked address is still refused,
    // before any connection is attempted.
    assertThatThrownBy(() -> SafeHttpFetcher.open("http://169.254.169.254/latest/meta-data/", BLOCK_LINK_LOCAL,
        "IMPORT DATABASE"))
        .isInstanceOf(SecurityException.class)
        .hasMessageContaining("169.254.169.254");
  }

  @Test
  void refusesRedirectLoop() {
    assertThatThrownBy(() -> SafeHttpFetcher.open(baseUrl + "/loop", BLOCK_LINK_LOCAL, "IMPORT DATABASE"))
        .isInstanceOf(SecurityException.class)
        .hasMessageContaining("maximum number of redirects");
  }

  @Test
  void followsAllowedRedirectAndReturnsContent() throws IOException {
    // Positive control: legitimate redirects are still followed, including relative Location headers, so the fix does
    // not break imports from hosts that redirect (CDNs, release URLs, ...).
    final var connection = SafeHttpFetcher.open(baseUrl + "/redirect-relative", BLOCK_LINK_LOCAL, "IMPORT DATABASE");
    try (final var in = connection.getInputStream()) {
      assertThat(new String(in.readAllBytes(), StandardCharsets.UTF_8)).isEqualTo(SECRET);
    } finally {
      connection.disconnect();
    }
  }

  @Test
  void fetchesDirectContent() throws IOException {
    final var connection = SafeHttpFetcher.open(baseUrl + "/content", BLOCK_LINK_LOCAL, "LOAD CSV");
    try (final var in = connection.getInputStream()) {
      assertThat(new String(in.readAllBytes(), StandardCharsets.UTF_8)).isEqualTo(SECRET);
    } finally {
      connection.disconnect();
    }
  }

  @Test
  void reportsHttpErrorStatus() {
    assertThatThrownBy(() -> SafeHttpFetcher.open(baseUrl + "/does-not-exist", BLOCK_LINK_LOCAL, "IMPORT DATABASE"))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("404");
  }

  @Test
  void pinningIsInertWithoutAProviderAndDoesNotLeak() throws IOException {
    // arcadedb-engine deliberately ships no resolver provider (installing one is a JVM-global, single-slot decision
    // that must not be taken on an embedder's behalf), so binding must be a harmless no-op here and fetches must keep
    // working through the documented fallback.
    assertThat(PinnedDnsResolution.isPinningAvailable())
        .as("the engine module must not register a resolver provider").isFalse();

    final var connection = SafeHttpFetcher.open(baseUrl + "/content", BLOCK_LINK_LOCAL, "LOAD CSV");
    try (final var in = connection.getInputStream()) {
      assertThat(new String(in.readAllBytes(), StandardCharsets.UTF_8)).isEqualTo(SECRET);
    } finally {
      connection.disconnect();
    }

    // The fetch must never leave a binding behind: a leaked pin on a pooled worker thread would constrain that
    // thread's resolution of the hostname for every later, unrelated request it serves.
    assertThat(PinnedDnsResolution.lookup("127.0.0.1")).as("no binding may survive the fetch").isNull();
  }
}
