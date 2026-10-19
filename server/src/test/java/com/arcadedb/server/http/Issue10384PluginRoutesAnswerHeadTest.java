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
package com.arcadedb.server.http;

import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.handlers.PathHandler;
import io.undertow.util.Headers;
import io.undertow.util.Methods;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.atomic.AtomicInteger;

import static com.arcadedb.server.StaticBaseServerTest.allocateFreePorts;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10384: a route a plugin declares as {@code GET} through {@link PluginRoutes} answered {@code HEAD} with 405, and the
 * {@code Allow} header it sent named {@code GET}: self-contradictory (RFC 9110 requires a server that supports GET on a
 * resource to support HEAD on it), and a regression, since before #10157 a plugin route matched on the path alone and
 * Undertow discarded the body of the HEAD answer. A container healthcheck such as {@code wget --spider} issues HEAD, so the
 * Prometheus scrape endpoint failed the probe the core routes were fixed to pass (#8133). The method-keyed dispatcher
 * answers HEAD from the GET entry, once, for every plugin.
 * <p>
 * Proven over a real Undertow listener wired the way {@code HttpServer.setupRoutes()} wires its {@code PathHandler}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10384PluginRoutesAnswerHeadTest {

  private final AtomicInteger scrapeRuns = new AtomicInteger();
  private final AtomicInteger headRuns   = new AtomicInteger();

  private Undertow   undertow;
  private String     baseUrl;
  private HttpClient client;

  @BeforeEach
  void startListener() {
    final PathHandler paths = new PathHandler();
    new PluginRoutes(paths)//
        .get("/scrape", exchange -> {
          scrapeRuns.incrementAndGet();
          exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");
          exchange.getResponseSender().send("PLUGIN scrape");
        })//
        .post("/rpc", exchange -> exchange.getResponseSender().send("rpc ran"))//
        .get("/custom", exchange -> exchange.getResponseSender().send("GET body"))//
        .exact(Methods.HEAD, "/custom", exchange -> {
          headRuns.incrementAndGet();
          exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/x-head");
        });

    final int port = allocateFreePorts(1)[0];
    undertow = Undertow.builder().addHttpListener(port, "127.0.0.1").setHandler(paths).build();
    undertow.start();
    baseUrl = "http://127.0.0.1:" + port;
    client = HttpClient.newHttpClient();
  }

  @AfterEach
  void stopListener() {
    if (undertow != null)
      undertow.stop();
    if (client != null)
      client.close();
  }

  @Test
  void aGetRouteAnswersHeadWithTheStatusAndHeadersOfGetAndNoBody() throws Exception {
    final HttpResponse<String> get = send("GET", "/scrape");
    final HttpResponse<String> head = send("HEAD", "/scrape");

    assertThat(get.statusCode()).isEqualTo(200);
    assertThat(get.body()).isEqualTo("PLUGIN scrape");

    assertThat(head.statusCode()).isEqualTo(200);
    assertThat(head.body()).isEmpty();
    assertThat(head.headers().firstValue("Content-Type")).isEqualTo(get.headers().firstValue("Content-Type"));
    assertThat(head.headers().firstValue("Allow")).as("a successful answer carries no Allow header").isEmpty();
    assertThat(scrapeRuns.get()).as("the one GET handler served both").isEqualTo(2);
  }

  @Test
  void headOnAPostOnlyRouteIsStill405AndAllowDoesNotPromiseIt() throws Exception {
    final HttpResponse<String> head = send("HEAD", "/rpc");

    assertThat(head.statusCode()).isEqualTo(405);
    assertThat(head.headers().firstValue("Allow")).hasValue("POST");
  }

  @Test
  void theAllowHeaderOfAGetRouteNamesHeadToo() throws Exception {
    final HttpResponse<String> post = send("POST", "/scrape");

    assertThat(post.statusCode()).isEqualTo(405);
    assertThat(post.headers().firstValue("Allow")).as("GET implies HEAD, so a 405 must not leave it out").hasValue("GET, HEAD");
    assertThat(scrapeRuns.get()).isZero();
  }

  @Test
  void aPluginThatDeclaresHeadItselfKeepsItsOwnHandler() throws Exception {
    final HttpResponse<String> head = send("HEAD", "/custom");

    assertThat(head.statusCode()).isEqualTo(200);
    assertThat(head.headers().firstValue("Content-Type")).hasValue("text/x-head");
    assertThat(headRuns.get()).isEqualTo(1);

    // HEAD was declared, so it is not repeated in the Allow header the 405 for another method carries
    assertThat(send("DELETE", "/custom").headers().firstValue("Allow")).hasValue("GET, HEAD");
  }

  @Test
  void theDerivedHeadIsAnsweredNotDeclared() {
    final RecordingPathHandler routes = new RecordingPathHandler();
    final HttpHandler handler = exchange -> {
    };
    new PluginRoutes(routes).get("/a", handler);

    // The spec-drift checks compare what a plugin DECLARED with what its OpenAPI paths say, and HEAD is derived, not declared
    assertThat(routes.getRegisteredOperations()).containsExactly("GET /a");
    assertThat(routes.getRegisteredHandlers().get("/a")).isSameAs(handler);
    assertThat(routes.getGetRoutesNotAnsweringHead()).isEmpty();
  }

  private HttpResponse<String> send(final String method, final String path) throws Exception {
    return client.send(HttpRequest.newBuilder(URI.create(baseUrl + path)).method(method, HttpRequest.BodyPublishers.noBody()).build(),
        HttpResponse.BodyHandlers.ofString());
  }
}
