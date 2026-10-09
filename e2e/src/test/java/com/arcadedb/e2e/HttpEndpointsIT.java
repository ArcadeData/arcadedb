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
package com.arcadedb.e2e;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/** The HTTP surface beyond the Java client: Studio, scripting languages over the command API, the Prometheus scrape. */
class HttpEndpointsIT extends ArcadeContainerTemplate {
  private static final String     AUTH   = "Basic " + Base64.getEncoder()
      .encodeToString("root:playwithdata".getBytes(StandardCharsets.UTF_8));
  private final        HttpClient client = HttpClient.newHttpClient();

  @Test
  void studioIsServed() throws Exception {
    final HttpResponse<String> response = get("/");
    assertThat(response.statusCode()).isEqualTo(200);
    assertThat(response.body()).containsIgnoringCase("arcadedb");
  }

  @Test
  void javascriptCommand() throws Exception {
    // GraalJS is embedded in both images
    final HttpResponse<String> response = command("{\"language\":\"js\",\"command\":\"40 + 2\"}");
    assertThat(response.statusCode()).isEqualTo(200);
    assertThat(response.body()).contains("42");
  }

  @Test
  void cypherCommand() throws Exception {
    final HttpResponse<String> response = command(
        "{\"language\":\"cypher\",\"command\":\"MATCH (b:Beer) RETURN count(b) AS n\"}");
    assertThat(response.statusCode()).isEqualTo(200);
    assertThat(response.body()).contains("\"n\"");
  }

  @Test
  void prometheusScrape() throws Exception {
    command("{\"language\":\"sql\",\"command\":\"SELECT FROM Beer LIMIT 1\"}");
    final HttpResponse<String> response = get("/prometheus");
    assertThat(response.statusCode()).isEqualTo(200);
    assertThat(response.body()).contains("arcadedb_");
  }

  private HttpResponse<String> get(final String path) throws IOException, InterruptedException {
    return client.send(HttpRequest.newBuilder(URI.create("http://" + host + ":" + httpPort + path)).header("Authorization", AUTH)
        .GET().build(), HttpResponse.BodyHandlers.ofString());
  }

  private HttpResponse<String> command(final String json) throws IOException, InterruptedException {
    return client.send(HttpRequest.newBuilder(URI.create("http://" + host + ":" + httpPort + "/api/v1/command/beer"))
        .header("Authorization", AUTH).header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(json)).build(), HttpResponse.BodyHandlers.ofString());
  }
}
