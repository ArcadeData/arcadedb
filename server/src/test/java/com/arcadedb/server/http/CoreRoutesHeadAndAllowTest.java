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

import com.arcadedb.server.BaseGraphServerTest;
import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.handlers.PathHandler;
import io.undertow.util.HttpString;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static com.arcadedb.server.StaticBaseServerTest.allocateFreePorts;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The core HTTP routes answer HEAD from their GET entry and a 405 carries an {@code Allow} header naming the methods
 * the route answers, pinned on the probe routes, on the recording router, and on the live server's whole route inventory.
 */
class CoreRoutesHeadAndAllowTest {
  // Regression: #10384 the probe routes answer HEAD with their GET handler
  /**
   * Issue #10384: the core dispatcher learned "answer HEAD from the GET entry" by hand, route by route (issue #8133), and the
   * plugin dispatcher learned it once, in the registrar. This pins the core half so the pairs can never be dropped: the two
   * probe routes container healthchecks target answer HEAD with the very handler that answers their GET.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ProbeRoutesAnswerHead extends BaseGraphServerTest {

    @Test
    void theProbeRoutesRegisterHeadNextToGetWithTheSameHandler() {
      final Map<RouteRecordingRoutingHandler.RouteDescriptor, HttpHandler> handlers = getServer(0).getHttpServer()
          .getRegisteredRouteHandlers();

      for (final String path : new String[] { "/api/v1/ready", "/api/v1/health" }) {
        final HttpHandler get = handlers.get(new RouteRecordingRoutingHandler.RouteDescriptor("GET", path));
        final HttpHandler head = handlers.get(new RouteRecordingRoutingHandler.RouteDescriptor("HEAD", path));

        assertThat(get).as("GET " + path).isNotNull();
        assertThat(head).as("HEAD " + path + " must stay registered next to GET (issue #8133)").isSameAs(get);
      }
    }
  }

  // Regression: #10531 core routes answered HEAD with 405 and sent no Allow header
  /**
   * Issue #10531: the core dispatcher learned "answer HEAD from the GET entry" route by route, with a second {@code .add("HEAD", ...)}
   * line, and only the two probe routes had one: 30 of the 32 GET routes answered HEAD with 405, and no core 405 carried the
   * {@code Allow} header that RFC 9110 section 15.5.6 requires, while the plugin registrar derives both. The recording router
   * derives them now, once, for every route it carries: a HEAD that has no entry of its own is answered by the GET entry of the same
   * template, and a 405 names the methods the template answers.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class RouterHeadAndAllow {
    private static final HttpHandler UNUSED = exchange -> {
    };

    private final RouteRecordingRoutingHandler router = new RouteRecordingRoutingHandler();
    private       Undertow                     undertow;
    private       HttpClient                   client;
    private       String                       baseUrl;

    @AfterEach
    void stopListener() {
      if (undertow != null)
        undertow.stop();
      if (client != null)
        client.close();
    }

    @Test
    void aHeadWithoutAnEntryOfItsOwnIsAnsweredByTheGetEntryWithItsPathParameters() throws Exception {
      router.get("/databases", say("databases")).get("/exists/{database}", exchange -> {
        exchange.getResponseHeaders().put(new HttpString("X-Database"), exchange.getQueryParameters().get("database").getFirst());
        exchange.getResponseSender().send("exists");
      });
      startListener();

      final HttpResponse<String> head = send("HEAD", "/api/v1/databases");
      assertThat(head.statusCode()).as("HEAD is GET without the body").isEqualTo(200);
      assertThat(head.body()).isEmpty();

      final HttpResponse<String> get = send("GET", "/api/v1/databases");
      assertThat(get.body()).isEqualTo("databases");
      assertThat(head.headers().firstValue("Content-Length")).as("same headers as the GET").isEqualTo(get.headers().firstValue("Content-Length"));

      final HttpResponse<String> parameterized = send("HEAD", "/api/v1/exists/mydb");
      assertThat(parameterized.statusCode()).isEqualTo(200);
      assertThat(parameterized.headers().firstValue("X-Database")).as("the path parameter reaches the handler").hasValue("mydb");
    }

    @Test
    void anEntryOfItsOwnWinsOverTheDerivedOneWhicheverIsRegisteredFirst() throws Exception {
      router.get("/before", signed("GET before")).add("HEAD", "/before", signed("HEAD before"));
      router.add("HEAD", "/after", signed("HEAD after")).get("/after", signed("GET after"));
      startListener();

      // a HEAD response carries no body: the headers say which handler answered
      assertThat(send("HEAD", "/api/v1/before").headers().firstValue("X-Answered-By")).hasValue("HEAD before");
      assertThat(send("HEAD", "/api/v1/after").headers().firstValue("X-Answered-By")).hasValue("HEAD after");
      assertThat(send("GET", "/api/v1/before").headers().firstValue("X-Answered-By")).hasValue("GET before");
      assertThat(send("GET", "/api/v1/after").headers().firstValue("X-Answered-By")).hasValue("GET after");
    }

    @Test
    void theDerivedHeadIsNotAnotherRegisteredRoute() {
      router.get("/databases", say("databases")).post("/query/{database}", say("query"));

      // the registered routes are what the OpenAPI document and the dispatch census compare against: a HEAD nobody declared is not one
      assertThat(router.getRegisteredRoutes()).containsExactly(new RouteRecordingRoutingHandler.RouteDescriptor("GET", "/databases"),
          new RouteRecordingRoutingHandler.RouteDescriptor("POST", "/query/{database}"));
    }

    @Test
    void aMethodNotAllowedNamesTheMethodsTheRouteAnswers() throws Exception {
      router.get("/databases", say("databases"))
          .post("/query/{database}", say("query"))
          .get("/server", say("server")).post("/server", say("server")).delete("/server", say("server"))
          .get("/ready", say("ready")).add("HEAD", "/ready", say("ready"));
      startListener();

      final HttpResponse<String> put = send("PUT", "/api/v1/databases");
      assertThat(put.statusCode()).isEqualTo(405);
      assertThat(put.headers().firstValue("Allow")).as("GET implies HEAD").hasValue("GET, HEAD");
      assertThat(put.body()).contains("Method Not Allowed").contains("GET, HEAD");

      assertThat(send("GET", "/api/v1/query/mydb").headers().firstValue("Allow")).as("a template with a parameter").hasValue("POST");
      assertThat(send("PATCH", "/api/v1/server").headers().firstValue("Allow")).as("sorted, whatever the registration order")
          .hasValue("DELETE, GET, HEAD, POST");
      assertThat(send("PUT", "/api/v1/ready").headers().firstValue("Allow")).as("a HEAD registered by hand is named once")
          .hasValue("GET, HEAD");
    }

    @Test
    void aPathNoRouteAnswersIsStillNotFoundNotMethodNotAllowed() throws Exception {
      router.get("/databases", say("databases"));
      startListener();

      assertThat(send("GET", "/api/v1/nothing").statusCode()).isEqualTo(404);
      assertThat(send("PUT", "/api/v1/nothing").statusCode()).isEqualTo(404);
    }

    private static HttpHandler say(final String body) {
      return exchange -> exchange.getResponseSender().send(body);
    }

    /** A handler that names itself in a header, which a HEAD response keeps and its body does not. */
    private static HttpHandler signed(final String name) {
      return exchange -> {
        exchange.getResponseHeaders().put(new HttpString("X-Answered-By"), name);
        exchange.getResponseSender().send(name);
      };
    }

    private void startListener() {
      final PathHandler paths = new PathHandler();
      paths.addPrefixPath("/api/v1", router);
      final int port = allocateFreePorts(1)[0];
      undertow = Undertow.builder().addHttpListener(port, "127.0.0.1").setHandler(paths).build();
      undertow.start();
      baseUrl = "http://127.0.0.1:" + port;
      client = HttpClient.newHttpClient();
    }

    private HttpResponse<String> send(final String method, final String path) throws Exception {
      return client.send(HttpRequest.newBuilder(URI.create(baseUrl + path)).method(method, HttpRequest.BodyPublishers.noBody()).build(),
          HttpResponse.BodyHandlers.ofString());
    }
  }

  // Regression: #10531 every live core route answers HEAD and a 405 carries Allow
  /**
   * Issue #10531, the census half: {@code ProbeRoutesAnswerHead} pins the two probe routes, which cannot fail for the
   * other 30. This sweeps the live server's own inventory of core routes, so a route added tomorrow is held to the same two rules the
   * day it is registered: every template with a GET answers HEAD (never 405, and with the status its GET answers), and a method the
   * template does not answer gets a 405 whose {@code Allow} names exactly the methods it does.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class LiveRoutesCensus extends BaseGraphServerTest {

    @Test
    void everyCoreGetRouteAnswersHeadAndEveryCore405NamesWhatTheRouteAnswers() throws Exception {
      final Map<String, TreeSet<String>> methodsByPath = new TreeMap<>();
      for (final RouteRecordingRoutingHandler.RouteDescriptor route : getServer(0).getHttpServer().getRegisteredRoutes())
        methodsByPath.computeIfAbsent(route.path(), path -> new TreeSet<>()).add(route.method());
      assertThat(methodsByPath).as("the sweep found the core routes").hasSizeGreaterThan(30);

      final List<String> failures = new ArrayList<>();
      int heads = 0;
      try (final HttpClient client = HttpClient.newHttpClient()) {
        for (final Map.Entry<String, TreeSet<String>> route : methodsByPath.entrySet()) {
          final String url = getServerHttpUrl(0, route.getKey().replaceAll("\\{[^}]*}", "probe"));
          final TreeSet<String> methods = route.getValue();

          if (methods.contains("GET")) {
            final int get = send(client, "GET", url).statusCode();
            final int head = send(client, "HEAD", url).statusCode();
            heads++;
            if (head == 405 || head != get)
              failures.add("HEAD " + route.getKey() + " answered " + head + " where GET answered " + get);
          }

          final TreeSet<String> answered = new TreeSet<>(methods);
          if (answered.contains("GET"))
            answered.add("HEAD");
          final HttpResponse<String> patch = send(client, "PATCH", url);
          final String allow = patch.headers().firstValue("Allow").orElse("");
          if (patch.statusCode() != 405 || !allow.equals(String.join(", ", answered)))
            failures.add("PATCH " + route.getKey() + " answered " + patch.statusCode() + " with Allow '" + allow + "', expected 405 with '"
                + String.join(", ", answered) + "'");
        }
      }

      assertThat(heads).as("the sweep sent a HEAD for the GET routes").isGreaterThan(25);
      assertThat(failures).as("every core GET route answers HEAD, and a core 405 carries Allow (RFC 9110 sections 9.3.2 and 15.5.6)")
          .isEmpty();
    }

    private HttpResponse<String> send(final HttpClient client, final String method, final String url) throws Exception {
      return client.send(HttpRequest.newBuilder(URI.create(url))
          .header("Authorization",
              "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)))
          .method(method, HttpRequest.BodyPublishers.noBody()).build(), HttpResponse.BodyHandlers.ofString());
    }
  }
}
