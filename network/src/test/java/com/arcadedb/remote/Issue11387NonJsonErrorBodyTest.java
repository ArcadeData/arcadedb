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
package com.arcadedb.remote;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.remote.timeseries.TimeSeriesPoint;
import com.arcadedb.serializer.json.JSONException;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11387: an error answer whose body is not JSON - the HTML or plain-text page an ingress, a load balancer or a sidecar
 * sends with a 502, 503 or 504 - used to come back as the parser's bare {@code JSONException}, with the status code lost, the
 * 503 never retried, and a log line saying it was retrying. It is now mapped on the status code exactly as the same status
 * with an empty JSON body is, and the exception names the status and a bounded excerpt of the body.
 */
class Issue11387NonJsonErrorBodyTest {
  private static final String HTML_503 = "<html><body><h1>503 Service Temporarily Unavailable</h1></body></html>";

  private ProxyNode           proxy;
  private RemoteDatabase      database;
  private RemoteHttpComponent component;

  @AfterEach
  void tearDown() {
    if (database != null)
      database.close();
    if (component != null)
      component.close();
    if (proxy != null)
      proxy.stop();
  }

  /** The mapping alone, on a component whose constructor's cluster probe is answered by a loopback node. */
  private RemoteHttpComponent mapper() throws IOException {
    proxy = new ProxyNode(404, "");
    component = new RemoteHttpComponent("127.0.0.1", proxy.port(), "root", "test", new ContextConfiguration());
    return component;
  }

  @Test
  void aNonJsonBodyMapsOnTheStatusCodeLikeAnEmptyJsonBody() throws IOException {
    final RemoteHttpComponent component = mapper();
    for (final int status : new int[] { 400, 401, 403, 404, 500, 502, 503, 504 }) {
      final Exception fromJson = component.manageException(status, "{}", "test");
      for (final String body : List.of("<html><body><h1>" + status + " Proxy Error</h1></body></html>", "upstream request timeout")) {
        final Exception fromPage = component.manageException(status, body, "test");

        assertThat(fromPage).as("status %d, body '%s'", status, body).isNotInstanceOf(JSONException.class);
        // the same type, or for a 503 its subtype that says the page did not come from the server
        assertThat(fromPage).as("status %d, body '%s'", status, body).isInstanceOf(fromJson.getClass());
        assertThat(fromPage.getMessage()).as("status %d, body '%s'", status, body)
            .contains("httpErrorCode=" + status)
            .contains(body);
      }
    }
  }

  @Test
  void aProxys503PageIsRetryable() throws IOException {
    final RemoteHttpComponent component = mapper();
    assertThat(component.manageException(503, HTML_503, "test")).isExactlyInstanceOf(RemoteIntermediaryRefusalException.class);
    // the server's own 503 keeps the plain type, which carries its "refused before running" guarantee
    assertThat(component.manageException(503, "{\"error\":\"Server temporarily unavailable\"}", "test"))
        .isExactlyInstanceOf(NeedRetryException.class);
  }

  @Test
  void theBodyInTheMessageIsBoundedAndOnOneLine() throws IOException {
    final RemoteHttpComponent component = mapper();
    final String page = "<html>\n  <body>\n" + "x".repeat(10_000) + "\n  </body>\n</html>";

    final String message = component.manageException(502, page, "test").getMessage();

    assertThat(message).doesNotContain("\n").contains("<html> <body> xxx");
    assertThat(message.length()).isLessThan(RemoteHttpComponent.NON_JSON_BODY_EXCERPT_MAX + 200);
  }

  @Test
  void bodyExcerptCollapsesWhitespaceAndTruncates() {
    assertThat(RemoteHttpComponent.bodyExcerpt(null)).isEmpty();
    assertThat(RemoteHttpComponent.bodyExcerpt("  a\r\n\tb  ")).isEqualTo("a b");
    final String excerpt = RemoteHttpComponent.bodyExcerpt("y".repeat(1_000));
    assertThat(excerpt).hasSize(RemoteHttpComponent.NON_JSON_BODY_EXCERPT_MAX + 3).endsWith("...");
  }

  /** The issue's harness: a 503 page in front of the query route is retried up to the election budget, as the JSON 503 is. */
  @Test
  void aQueryAnsweredWithAProxys503PageIsRetriedUpToTheElectionBudget() throws IOException {
    proxy = new ProxyNode(503, HTML_503);
    database = connect(proxy);

    assertThatThrownBy(() -> database.query("sql", "select 1")).isExactlyInstanceOf(RemoteIntermediaryRefusalException.class)
        .hasMessageContaining("httpErrorCode=503");

    assertThat(proxy.requestsTo("/api/v1/query/db")).as("the first attempt and the 3 election retries").isEqualTo(4);
  }

  @Test
  void aQueryAnsweredWithAProxys502PageNamesTheStatusAndIsNotRetried() throws IOException {
    proxy = new ProxyNode(502, "<html><body><h1>502 Bad Gateway</h1></body></html>");
    database = connect(proxy);

    assertThatThrownBy(() -> database.query("sql", "select 1")).isExactlyInstanceOf(RemoteException.class)
        .hasMessageContaining("httpErrorCode=502").hasMessageContaining("502 Bad Gateway");

    assertThat(proxy.requestsTo("/api/v1/query/db")).isEqualTo(1);
  }

  /** begin() tests the mapped type for NeedRetryException: a proxy's 503 page must reach transaction()'s retry loop as one. */
  @Test
  void aTransactionWhoseBeginMeetsAProxys503PageIsRetried() throws IOException {
    proxy = new ProxyNode(503, HTML_503);
    database = connect(proxy);

    assertThatThrownBy(() -> database.transaction(() -> {
    }, false, 3)).isExactlyInstanceOf(RemoteIntermediaryRefusalException.class);

    assertThat(proxy.requestsTo("/api/v1/begin/db")).isEqualTo(3);
  }

  /**
   * A hop may answer 503 after it forwarded the request (the upstream reset before its answer arrived), so the page does not
   * prove the write did not run: a command that cannot be replayed safely is not sent again on it.
   */
  @Test
  void aWriteCommandAnsweredWithAProxys503PageIsNotReplayed() throws IOException {
    proxy = new ProxyNode(503, HTML_503);
    database = connect(proxy);

    assertThatThrownBy(() -> database.command("sql", "insert into V set a = 1")).isExactlyInstanceOf(RemoteException.class)
        .hasMessageContaining("may already have applied it")
        .cause().isExactlyInstanceOf(RemoteIntermediaryRefusalException.class);

    assertThat(proxy.requestsTo("/api/v1/command/db")).isEqualTo(1);
  }

  /**
   * The request id lets the server answer a replay from its cache, but only the server that holds the entry: the write is
   * resent on a proxy's 503 page only on a strategy that never fails over, the rule a transport failure after sending follows.
   */
  @Test
  void aReplayProtectedWriteIsResentOnAProxys503PageOnlyOnTheFixedStrategy() throws IOException {
    proxy = new ProxyNode(503, HTML_503);
    proxy.advertiseReplayProtection();
    database = connect(proxy);
    // the first answer tells the client which server process holds the replay cache
    assertThatThrownBy(() -> database.query("sql", "select 1")).isInstanceOf(NeedRetryException.class);

    assertThatThrownBy(() -> database.command("sql", "insert into V set a = 1")).isExactlyInstanceOf(RemoteException.class)
        .hasMessageContaining("may already have applied it");
    assertThat(proxy.requestsTo("/api/v1/command/db")).as("a strategy that may fail over: not resent").isEqualTo(1);

    database.setConnectionStrategy(RemoteHttpComponent.CONNECTION_STRATEGY.FIXED);
    assertThatThrownBy(() -> database.command("sql", "insert into V set a = 1"))
        .isExactlyInstanceOf(RemoteIntermediaryRefusalException.class);
    assertThat(proxy.requestsTo("/api/v1/command/db")).as("FIXED: resent with its id, up to the election budget").isEqualTo(1 + 4);
    // the four attempts of the FIXED call are one logical request, and the resends name the process that holds its entry
    final List<String[]> fixedCall = proxy.commandHeaders().subList(1, 5);
    assertThat(fixedCall.getFirst()[0]).isNotEmpty();
    for (final String[] attempt : fixedCall)
      assertThat(attempt[0]).isEqualTo(fixedCall.getFirst()[0]);
    for (final String[] attempt : fixedCall.subList(1, 4))
      assertThat(attempt[1]).isEqualTo("instance-1");
  }

  /** A streamed write answered with a proxy's 503 page is not handed to transaction()'s retry loop as retryable. */
  @Test
  void aStreamedWriteInATransactionAnsweredWithAProxys503PageIsNotReplayed() throws IOException {
    proxy = new ProxyNode(503, HTML_503);
    proxy.beginOpensASession();
    database = connect(proxy);

    final AtomicInteger executions = new AtomicInteger();
    assertThatThrownBy(() -> database.transaction(() -> {
      executions.incrementAndGet();
      database.commandStream("sql", "update V set a = 2 batch 10", Map.of());
    }, false, 3)).isExactlyInstanceOf(RemoteException.class).hasMessageContaining("may already have applied it");

    assertThat(executions.get()).isEqualTo(1);
    assertThat(proxy.requestsTo("/api/v1/command/db")).isEqualTo(1);
  }

  /** A time-series write answered with a proxy's 503 page may have stored its samples: transaction() must not write them again. */
  @Test
  void aTimeSeriesWriteInATransactionAnsweredWithAProxys503PageIsNotReplayed() throws IOException {
    proxy = new ProxyNode(503, HTML_503);
    proxy.beginOpensASession();
    database = connect(proxy);

    final List<TimeSeriesPoint> points = List.of(new TimeSeriesPoint("Sensor", 1L, Map.of(), Map.of("value", 1.0)));
    final AtomicInteger executions = new AtomicInteger();
    assertThatThrownBy(() -> database.transaction(() -> {
      executions.incrementAndGet();
      database.timeSeriesWrite(points);
    }, false, 3)).isExactlyInstanceOf(RemoteException.class).hasMessageContaining("may already have applied it");

    assertThat(executions.get()).isEqualTo(1);
    assertThat(proxy.requestsTo("/api/v1/ts/db/write")).isEqualTo(1);
  }

  /** A streamed read is safe to repeat: it keeps the retryable type. */
  @Test
  void aStreamedReadAnsweredWithAProxys503PageStaysRetryable() throws IOException {
    proxy = new ProxyNode(503, HTML_503);
    database = connect(proxy);

    assertThatThrownBy(() -> database.queryStream("sql", "select 1", Map.of()))
        .isExactlyInstanceOf(RemoteIntermediaryRefusalException.class);
  }

  /** A commit answered with a proxy's 503 page may have landed: transaction() must not run the block a second time. */
  @Test
  void aCommitAnsweredWithAProxys503PageIsNotRetried() throws IOException {
    proxy = new ProxyNode(503, HTML_503);
    proxy.beginOpensASession();
    database = connect(proxy);

    final AtomicInteger executions = new AtomicInteger();
    assertThatThrownBy(() -> database.transaction(executions::incrementAndGet, false, 3)).isExactlyInstanceOf(TransactionException.class)
        .hasMessageContaining("may have been applied")
        .cause().isExactlyInstanceOf(RemoteIntermediaryRefusalException.class);

    assertThat(executions.get()).isEqualTo(1);
    assertThat(proxy.requestsTo("/api/v1/commit/db")).isEqualTo(1);
    assertThat(proxy.requestsTo("/api/v1/rollback/db")).as("an outcome that may have landed is not rolled back").isZero();
  }

  /** A begin refused with a page that is not retryable: one TransactionException, whose cause names the status. */
  @Test
  void aBeginAnsweredWithAProxys502PageIsWrappedOnceAndKeepsTheStatus() throws IOException {
    proxy = new ProxyNode(502, "<html><body><h1>502 Bad Gateway</h1></body></html>");
    database = connect(proxy);

    assertThatThrownBy(() -> database.begin()).isExactlyInstanceOf(TransactionException.class)
        .hasMessage("Error on transaction begin")
        .cause().isExactlyInstanceOf(RemoteException.class).hasMessageContaining("httpErrorCode=502");

    assertThat(proxy.requestsTo("/api/v1/begin/db")).isEqualTo(1);
  }

  /** The time-series write parses a 400 body itself before it reaches the mapping: a proxy's page must not stop it there. */
  @Test
  void aTimeSeriesWriteAnsweredWithAProxys400PageNamesTheStatus() throws IOException {
    proxy = new ProxyNode(400, "<html><body><h1>400 Bad Request</h1></body></html>");
    database = connect(proxy);

    final List<TimeSeriesPoint> points = List.of(new TimeSeriesPoint("Sensor", 1L, Map.of(), Map.of("value", 1.0)));
    assertThatThrownBy(() -> database.timeSeriesWrite(points)).isExactlyInstanceOf(RemoteException.class)
        .hasMessageContaining("httpErrorCode=400").hasMessageContaining("400 Bad Request");
  }

  /** A streamed batch import whose answer is a proxy's page: mapped on the status, not reported as an old server. */
  @Test
  void aStreamedBatchImportAnsweredWithAProxys503PageKeepsTheStatus() throws IOException {
    proxy = new ProxyNode(503, HTML_503);
    database = connect(proxy);

    assertThatThrownBy(() -> database.sendBatch("{\"@type\":\"vertex\"}", Map.of(), progress -> {
    })).isExactlyInstanceOf(DatabaseOperationException.class)
        .hasMessageContaining("HTTP 503")
        .cause().isExactlyInstanceOf(RemoteIntermediaryRefusalException.class).hasMessageContaining("503 Service Temporarily Unavailable");
  }

  private static RemoteDatabase connect(final ProxyNode node) {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.HA_CLIENT_ELECTION_RETRY_DELAY_MS, 1L);
    configuration.setValue(GlobalConfiguration.HA_CLIENT_ELECTION_RETRY_COUNT, 3);
    return new RemoteDatabase("127.0.0.1", node.port(), "db", "root", "test", configuration);
  }

  /** A loopback hop: no cluster configuration (404), and every other route answered with its status and a non-JSON page. */
  private static final class ProxyNode {
    private final HttpServer   http;
    private final List<String>   requests       = new CopyOnWriteArrayList<>();
    // X-Request-Id and X-ArcadeDB-Replay-Instance of each command request, "" when absent
    private final List<String[]> commandHeaders = new CopyOnWriteArrayList<>();
    private final int          status;
    private final byte[]       page;
    private volatile boolean   beginOpensASession;
    private volatile boolean   advertiseReplayProtection;

    private ProxyNode(final int status, final String page) throws IOException {
      this.status = status;
      this.page = page.getBytes(StandardCharsets.UTF_8);
      // THE ADDRESS THE CLIENTS DIAL: getLoopbackAddress() MAY BE ::1 ON A JVM THAT PREFERS IPv6
      http = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
      http.createContext("/api/v1/", this::handle);
      http.start();
    }

    private void handle(final HttpExchange exchange) throws IOException {
      final String path = exchange.getRequestURI().getPath();
      requests.add(path);
      if (path.startsWith("/api/v1/command/"))
        commandHeaders.add(new String[] { header(exchange, RemoteHttpComponent.HEADER_REQUEST_ID),
            header(exchange, RemoteHttpComponent.HEADER_REPLAY_INSTANCE) });
      if (advertiseReplayProtection)
        exchange.getResponseHeaders().add(RemoteHttpComponent.HEADER_REPLAY_PROTECTION, "instance-1");
      if ("/api/v1/server".equals(path)) {
        exchange.sendResponseHeaders(404, -1);
        exchange.close();
        return;
      }
      if (beginOpensASession && path.startsWith("/api/v1/begin/")) {
        exchange.getResponseHeaders().add(RemoteDatabase.ARCADEDB_SESSION_ID, "AS-1");
        exchange.sendResponseHeaders(204, -1);
        exchange.close();
        return;
      }
      exchange.getResponseHeaders().add("Content-Type", "text/html");
      exchange.sendResponseHeaders(status, page.length);
      exchange.getResponseBody().write(page);
      exchange.close();
    }

    /** Names a server process with a replay cache on every answer, as a server with replay protection does. */
    private void advertiseReplayProtection() {
      advertiseReplayProtection = true;
    }

    /** Answers begin with 204 and a session, so the scripted status reaches the commit. */
    private void beginOpensASession() {
      beginOpensASession = true;
    }

    private static String header(final HttpExchange exchange, final String name) {
      final String value = exchange.getRequestHeaders().getFirst(name);
      return value != null ? value : "";
    }

    private List<String[]> commandHeaders() {
      return commandHeaders;
    }

    private int requestsTo(final String route) {
      return (int) requests.stream().filter(route::equals).count();
    }

    private int port() {
      return http.getAddress().getPort();
    }

    private void stop() {
      http.stop(0);
    }
  }
}
