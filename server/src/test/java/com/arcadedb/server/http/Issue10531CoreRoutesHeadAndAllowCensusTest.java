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
import org.junit.jupiter.api.Test;

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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10531, the census half: {@code Issue10384CoreProbeRoutesAnswerHeadTest} pins the two probe routes, which cannot fail for the
 * other 30. This sweeps the live server's own inventory of core routes, so a route added tomorrow is held to the same two rules the
 * day it is registered: every template with a GET answers HEAD (never 405, and with the status its GET answers), and a method the
 * template does not answer gets a 405 whose {@code Allow} names exactly the methods it does.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10531CoreRoutesHeadAndAllowCensusTest extends BaseGraphServerTest {

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
