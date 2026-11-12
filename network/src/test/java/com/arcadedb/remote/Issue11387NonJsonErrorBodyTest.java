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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

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
        assertThat(fromPage.getClass()).as("status %d, body '%s'", status, body).isEqualTo(fromJson.getClass());
        assertThat(fromPage.getMessage()).as("status %d, body '%s'", status, body)
            .contains("httpErrorCode=" + status)
            .contains(body);
      }
    }
  }

  @Test
  void aProxys503PageIsRetryable() throws IOException {
    final RemoteHttpComponent component = mapper();
    assertThat(component.manageException(503, HTML_503, "test")).isExactlyInstanceOf(NeedRetryException.class);
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

    assertThatThrownBy(() -> database.query("sql", "select 1")).isExactlyInstanceOf(NeedRetryException.class)
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
    }, false, 3)).isExactlyInstanceOf(NeedRetryException.class);

    assertThat(proxy.requestsTo("/api/v1/begin/db")).isEqualTo(3);
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
        .cause().isExactlyInstanceOf(NeedRetryException.class).hasMessageContaining("503 Service Temporarily Unavailable");
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
    private final List<String> requests = new CopyOnWriteArrayList<>();
    private final int          status;
    private final byte[]       page;

    private ProxyNode(final int status, final String page) throws IOException {
      this.status = status;
      this.page = page.getBytes(StandardCharsets.UTF_8);
      http = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
      http.createContext("/api/v1/", this::handle);
      http.start();
    }

    private void handle(final HttpExchange exchange) throws IOException {
      final String path = exchange.getRequestURI().getPath();
      requests.add(path);
      if ("/api/v1/server".equals(path)) {
        exchange.sendResponseHeaders(404, -1);
        exchange.close();
        return;
      }
      exchange.getResponseHeaders().add("Content-Type", "text/html");
      exchange.sendResponseHeaders(status, page.length);
      exchange.getResponseBody().write(page);
      exchange.close();
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
