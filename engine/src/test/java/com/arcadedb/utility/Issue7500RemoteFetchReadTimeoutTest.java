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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7500: a remote source that stops sending mid-stream and never closes the socket.
 * <p>
 * Before #7494 that ended the content sniff with a WRONG answer: {@code Parser.isAvailable()} asked
 * {@code reader.ready() || is.available() > 0} - "can I read a byte right now without blocking" - and a quiet socket
 * answered false, which every caller read as end-of-input. #7494 fixed the guess by peeking a real character, which
 * BLOCKS. What that exposed is the other half: nothing must WAIT FOREVER either.
 * <p>
 * The deadline belongs on the connection, which is the one thing that can tell a stalled socket from a slow parser,
 * and {@link SafeHttpFetcher} is where every caller-supplied URL is opened. Two things are asserted here: the wait is
 * BOUNDED by {@link GlobalConfiguration#NETWORK_REMOTE_FETCH_READ_TIMEOUT}, and the failure NAMES that setting -
 * a bare {@code "Read timed out"} arriving inside {@code "Error on parsing source ..."} tells an operator neither
 * what expired nor what to change, which is the same class of unhelpful error #7346 and #7461 were about.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7500RemoteFetchReadTimeoutTest {

  /** Long enough to be unmistakably a deliberate stall, short enough that the test costs a fraction of a second. */
  private static final int TIMEOUT_MS = 300;

  /** Mirrors a real block-list while letting the loopback origin server through. */
  private static final Predicate<InetAddress> BLOCK_LINK_LOCAL = InetAddress::isLinkLocalAddress;

  private HttpServer     server;
  private String         baseUrl;
  private CountDownLatch release;
  private Object         previousReadTimeout;

  @BeforeEach
  void startServer() throws IOException {
    previousReadTimeout = GlobalConfiguration.NETWORK_REMOTE_FETCH_READ_TIMEOUT.getValue();
    GlobalConfiguration.NETWORK_REMOTE_FETCH_READ_TIMEOUT.setValue(TIMEOUT_MS);

    release = new CountDownLatch(1);

    server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
    baseUrl = "http://127.0.0.1:" + server.getAddress().getPort();

    // A hung origin: the response headers and the first bytes arrive, then the handler goes quiet holding the socket
    // open - a proxy that keeps the connection after the origin dies, as the issue puts it. The declared length is
    // longer than what is written, so the reader legitimately waits for more.
    server.createContext("/stall", exchange -> {
      final byte[] head = "id,name\n".getBytes(StandardCharsets.UTF_8);
      exchange.sendResponseHeaders(200, head.length + 4096);
      exchange.getResponseBody().write(head);
      exchange.getResponseBody().flush();
      try {
        release.await(30, TimeUnit.SECONDS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      exchange.close();
    });

    // An origin that accepts the connection and sends NOTHING - not even a status line.
    server.createContext("/silent", exchange -> {
      try {
        release.await(30, TimeUnit.SECONDS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      exchange.close();
    });

    server.createContext("/content", exchange -> {
      final byte[] body = "id,name\n1,Jay\n".getBytes(StandardCharsets.UTF_8);
      exchange.sendResponseHeaders(200, body.length);
      exchange.getResponseBody().write(body);
      exchange.close();
    });

    server.start();
  }

  @AfterEach
  void stopServer() {
    release.countDown();
    if (server != null)
      server.stop(0);
    GlobalConfiguration.NETWORK_REMOTE_FETCH_READ_TIMEOUT.setValue(previousReadTimeout);
  }

  /**
   * The wait is bounded. {@code @Timeout} is the hang detector - it fails the run rather than letting it hang - and
   * the {@link StallAwareStopwatch} bound is the assertion: a tripwire between a bounded read and the unbounded one
   * this issue is about, sized far enough above the configured timeout that a JVM hiccup cannot turn it red.
   */
  @Test
  @Timeout(60)
  void aStalledSourceFailsInsteadOfWaitingForever() throws IOException {
    final HttpURLConnection connection = SafeHttpFetcher.open(baseUrl + "/stall", BLOCK_LINK_LOCAL, "IMPORT DATABASE");
    assertThat(connection.getReadTimeout()).as("the configured timeout reached the connection").isEqualTo(TIMEOUT_MS);

    final InputStream in = SafeHttpFetcher.body(connection, "IMPORT DATABASE");
    assertThat(in.read()).as("what the origin did send still arrives").isEqualTo('i');

    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    assertThatThrownBy(() -> in.readAllBytes())
        .isInstanceOf(SocketTimeoutException.class)
        .as("the message names the wait, the source and the setting that relaxes it")
        .hasMessageContaining("IMPORT DATABASE")
        .hasMessageContaining("/stall")
        .hasMessageContaining(String.valueOf(TIMEOUT_MS))
        .hasMessageContaining(GlobalConfiguration.NETWORK_REMOTE_FETCH_READ_TIMEOUT.getKey());
    stopwatch.assertGaveUpWithin(30_000, "a read bounded by the configured timeout from one that waits forever");

    connection.disconnect();
  }

  /**
   * The cause is kept, so a caller that already looks for a {@link SocketTimeoutException} - or logs the chain - is
   * not left with only the rewritten message.
   */
  @Test
  @Timeout(60)
  void theOriginalTimeoutIsTheCause() throws IOException {
    final HttpURLConnection connection = SafeHttpFetcher.open(baseUrl + "/stall", BLOCK_LINK_LOCAL, "IMPORT DATABASE");
    final InputStream in = SafeHttpFetcher.body(connection, "IMPORT DATABASE");

    assertThatThrownBy(() -> in.readAllBytes())
        .hasCauseInstanceOf(SocketTimeoutException.class)
        .cause()
        .hasMessageContaining("Read timed out");

    connection.disconnect();
  }

  /**
   * A source that behaves reads exactly as it did: the wrapper is transparent when nothing times out.
   */
  @Test
  @Timeout(60)
  void aHealthySourceIsUnaffected() throws IOException {
    final HttpURLConnection connection = SafeHttpFetcher.open(baseUrl + "/content", BLOCK_LINK_LOCAL, "IMPORT DATABASE");
    try (final InputStream in = SafeHttpFetcher.body(connection, "IMPORT DATABASE")) {
      assertThat(new String(in.readAllBytes(), StandardCharsets.UTF_8)).isEqualTo("id,name\n1,Jay\n");
    }
    connection.disconnect();
  }

  /**
   * The OTHER place the wait can expire: while {@link SafeHttpFetcher#open} is reading the response HEADERS, before
   * there is a body for {@link SafeHttpFetcher#body} to wrap. An origin that accepts the connection and then sends
   * nothing at all - or that stalls part way down a redirect chain - times out there, and used to reach the caller
   * as the JDK's bare {@code "Read timed out"} while a stall one byte later got the full diagnostic (PR #7755
   * review).
   */
  @Test
  @Timeout(60)
  void anOriginThatNeverSendsAHeaderTimesOutWithTheSameDiagnostic() {
    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();

    assertThatThrownBy(() -> SafeHttpFetcher.open(baseUrl + "/silent", BLOCK_LINK_LOCAL, "IMPORT DATABASE"))
        .isInstanceOf(SocketTimeoutException.class)
        .as("the message names the wait, the source and the settings that relax it")
        .hasMessageContaining("IMPORT DATABASE")
        .hasMessageContaining("/silent")
        .hasMessageContaining(String.valueOf(TIMEOUT_MS))
        .hasMessageContaining(GlobalConfiguration.NETWORK_REMOTE_FETCH_READ_TIMEOUT.getKey())
        .as("and the JDK's own timeout is kept as the cause")
        .hasCauseInstanceOf(SocketTimeoutException.class);

    stopwatch.assertGaveUpWithin(30_000, "a bounded header read from one that waits for as long as the socket lives");
  }

  /**
   * The setting is read per fetch, so a change takes effect on the next fetch rather than at the next restart, and a
   * negative value - which the JDK answers with an {@link IllegalArgumentException} from the middle of a fetch -
   * falls back to the default instead of failing the import.
   */
  @Test
  void theTimeoutComesFromTheConfigurationAndANegativeValueFallsBack() {
    assertThat(SafeHttpFetcher.configuredReadTimeoutMs(null)).isEqualTo(TIMEOUT_MS);

    GlobalConfiguration.NETWORK_REMOTE_FETCH_READ_TIMEOUT.setValue(-1);
    assertThat(SafeHttpFetcher.configuredReadTimeoutMs(null)).isEqualTo(SafeHttpFetcher.DEFAULT_READ_TIMEOUT_MS);

    GlobalConfiguration.NETWORK_REMOTE_FETCH_READ_TIMEOUT.setValue(0);
    assertThat(SafeHttpFetcher.configuredReadTimeoutMs(null)).as("0 is the JDK's 'no timeout', and is kept").isZero();

    final Object previousConnect = GlobalConfiguration.NETWORK_REMOTE_FETCH_CONNECT_TIMEOUT.getValue();
    try {
      GlobalConfiguration.NETWORK_REMOTE_FETCH_CONNECT_TIMEOUT.setValue(1234);
      assertThat(SafeHttpFetcher.configuredConnectTimeoutMs(null)).isEqualTo(1234);
    } finally {
      GlobalConfiguration.NETWORK_REMOTE_FETCH_CONNECT_TIMEOUT.setValue(previousConnect);
    }
  }

  /**
   * The channel an operator actually uses, and the reason the timeouts take a {@link ContextConfiguration} at all.
   * <p>
   * Both settings are {@code SCOPE.SERVER}, and a {@code ContextConfiguration} is an overlay that NEVER writes
   * through to the {@link GlobalConfiguration} enum - a server configuration file, {@code ALTER SERVER SETTING} and
   * {@code SET SERVER SETTING} all store there. So a fetch reading the enum alone goes on using the default while
   * the operator's value sits in the overlay applied to nothing, which is what this pins (PR #7755 review).
   */
  @Test
  @Timeout(60)
  void aServerConfiguredTimeoutIsTheOneTheFetchUses() throws IOException {
    final ContextConfiguration serverConfiguration = new ContextConfiguration();
    serverConfiguration.setValue(GlobalConfiguration.NETWORK_REMOTE_FETCH_READ_TIMEOUT, 4321);
    serverConfiguration.setValue(GlobalConfiguration.NETWORK_REMOTE_FETCH_CONNECT_TIMEOUT, 8765);

    assertThat(GlobalConfiguration.NETWORK_REMOTE_FETCH_READ_TIMEOUT.getValueAsInteger())
        .as("the overlay did not write through, which is the whole point")
        .isEqualTo(TIMEOUT_MS);

    assertThat(SafeHttpFetcher.configuredReadTimeoutMs(serverConfiguration)).isEqualTo(4321);
    assertThat(SafeHttpFetcher.configuredConnectTimeoutMs(serverConfiguration)).isEqualTo(8765);

    // And it reaches the connection, not just the accessor.
    final HttpURLConnection connection = SafeHttpFetcher.open(baseUrl + "/content", BLOCK_LINK_LOCAL,
        "IMPORT DATABASE", serverConfiguration);
    assertThat(connection.getReadTimeout()).isEqualTo(4321);
    assertThat(connection.getConnectTimeout()).isEqualTo(8765);
    connection.disconnect();

    // A caller with no overlay - the CLI importer, an embedded restore - still reads the enum.
    assertThat(SafeHttpFetcher.configuredReadTimeoutMs(null)).isEqualTo(TIMEOUT_MS);
  }

}
