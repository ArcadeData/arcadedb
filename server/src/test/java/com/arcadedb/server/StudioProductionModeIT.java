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
package com.arcadedb.server;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that the Studio web tool (static content) is disabled by default in production mode,
 * and can be force-enabled with {@link GlobalConfiguration#STUDIO_ENABLED}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class StudioProductionModeIT extends StaticBaseServerTest {
  private final HttpClient client = HttpClient.newHttpClient();
  private       ArcadeDBServer server;

  @BeforeEach
  public void beginTest() {
    super.beginTest();
  }

  @AfterEach
  public void endTest() {
    if (server != null && server.isStarted())
      server.stop();
    server = null;
    super.endTest();
  }

  @Test
  void studioDisabledInProductionByDefault() throws Exception {
    GlobalConfiguration.SERVER_MODE.setValue("production");

    startServer();

    final HttpResponse<String> response = get("/");
    // The static content route is not registered, so the root path is not served
    assertThat(response.statusCode()).isEqualTo(404);
  }

  @Test
  void studioEnabledInProductionWhenForced() throws Exception {
    GlobalConfiguration.SERVER_MODE.setValue("production");
    GlobalConfiguration.STUDIO_ENABLED.setValue(true);

    startServer();

    final HttpResponse<String> response = get("/");
    // index.html is available on the test classpath, so the root path is served
    assertThat(response.statusCode()).isEqualTo(200);
  }

  private void startServer() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_ROOT_PATH, "./target");
    config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, DEFAULT_PASSWORD_FOR_TESTS);
    config.setValue(GlobalConfiguration.SERVER_HTTP_IO_THREADS, 2);
    config.setValue(GlobalConfiguration.TYPE_DEFAULT_BUCKETS, 2);

    server = new ArcadeDBServer(config);
    server.start();
  }

  private HttpResponse<String> get(final String path) throws Exception {
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(new URI("http://localhost:2480" + path))
        .GET()
        .build();
    return client.send(request, BodyHandlers.ofString());
  }
}
