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
import com.arcadedb.server.StaticBaseServerTest;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12407: {@code GET /api/v1/exists/{database}} with an empty database name answered 200 {@code {"result":false}},
 * while the HTTP API reference lists a 400 for a request that names no database, and the sibling route
 * {@code GET /api/v1/progress/{database}} already answers 400 for the same empty name. The name is bound from the
 * path, so the request arrived with an empty name rather than with no name at all, and the handler's own check for a
 * missing name never saw it.
 */
class Issue12407ExistsWithoutADatabaseNameIs400Test extends BaseGraphServerTest {

  @Test
  void aRequestThatNamesNoDatabaseIsRefused() throws Exception {
    final Response response = send("GET", "/api/v1/exists/");

    assertThat(response.status).as(response.body).isEqualTo(400);
    assertThat(response.body).as("the answer names the cause").contains("Database parameter is null");
  }

  @Test
  void aRequestWithABlankDatabaseNameIsRefused() throws Exception {
    final Response response = send("GET", "/api/v1/exists/%20");

    assertThat(response.status).as(response.body).isEqualTo(400);
    assertThat(response.body).as("the answer names the cause").contains("Database parameter is null");
  }

  @Test
  void aDatabaseThatExistsStillAnswersTrue() throws Exception {
    final Response response = send("GET", "/api/v1/exists/" + getDatabaseName());

    assertThat(response.status).as(response.body).isEqualTo(200);
    assertThat(response.body).as("the database exists").contains("\"result\":true");
  }

  @Test
  void aDatabaseThatDoesNotExistStillAnswersFalse() throws Exception {
    final Response response = send("GET", "/api/v1/exists/nosuchdatabase12407");

    assertThat(response.status).as(response.body).isEqualTo(200);
    assertThat(response.body).as("the database does not exist").contains("\"result\":false");
  }

  private Response send(final String method, final String path) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(getServerHttpUrl(path)).toURL().openConnection();
    connection.setRequestMethod(method);
    final String credentials = Base64.getEncoder().encodeToString(("root:" + StaticBaseServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8));
    connection.setRequestProperty("Authorization", "Basic " + credentials);
    try {
      connection.connect();
      final int status = connection.getResponseCode();
      final InputStream in = status >= 400 ? connection.getErrorStream() : connection.getInputStream();
      final String body = in == null ? "" : new String(in.readAllBytes(), StandardCharsets.UTF_8);
      return new Response(status, body);
    } finally {
      connection.disconnect();
    }
  }

  private record Response(int status, String body) {
  }
}
