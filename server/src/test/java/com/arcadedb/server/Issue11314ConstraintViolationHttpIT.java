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

import com.arcadedb.exception.UnknownFunctionException;
import com.arcadedb.exception.ValidationException;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11314 over HTTP: a value a declared property constraint refuses is the caller's mistake, answered 400 with the
 * {@link ValidationException} and the constraint it violates in {@code exceptionArgs}, and the Java client rebuilds the exception
 * with its {@link ValidationException.Kind}, as it rebuilds a taken name with its kind (#10153). A call of an unknown function is
 * a 400 too, not the 500 of a server fault.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11314ConstraintViolationHttpIT extends BaseGraphServerTest {

  @Test
  void aViolatedConstraintAnswers400WithItsKind() throws Exception {
    assertThat(command("sql", "CREATE DOCUMENT TYPE H11314").statusCode()).isEqualTo(200);
    assertThat(command("sql", "CREATE PROPERTY H11314.nn STRING (notnull true)").statusCode()).isEqualTo(200);
    assertThat(command("sql", "CREATE PROPERTY H11314.mx STRING (max 3)").statusCode()).isEqualTo(200);

    assertRefused("INSERT INTO H11314 SET nn = null", "NOT_NULL");
    assertRefused("INSERT INTO H11314 SET nn = 'a', mx = 'abcdef'", "MAX_LENGTH");
  }

  @Test
  void anUnknownFunctionAnswers400() throws Exception {
    final HttpResponse<String> response = command("sql", "SELECT foo(1)");
    assertThat(response.statusCode()).as(response.body()).isEqualTo(400);
    assertThat(new JSONObject(response.body()).getString("exception")).isEqualTo(UnknownFunctionException.class.getName());
  }

  @Test
  void theJavaClientRebuildsTheViolationWithItsKind() {
    try (final RemoteDatabase database = new RemoteDatabase("127.0.0.1", getServerHttpPort(0), getDatabaseName(), "root",
        DEFAULT_PASSWORD_FOR_TESTS)) {
      database.command("sql", "CREATE DOCUMENT TYPE R11314");
      database.command("sql", "CREATE PROPERTY R11314.i INTEGER (max 10)");

      assertThatThrownBy(() -> database.command("sql", "INSERT INTO R11314 SET i = 11"))
          .isInstanceOf(ValidationException.class)
          .hasMessageContaining("greater than")
          .satisfies(e -> assertThat(((ValidationException) e).getKind()).isEqualTo(ValidationException.Kind.MAX));
    }
  }

  private void assertRefused(final String statement, final String kind) throws Exception {
    final HttpResponse<String> response = command("sql", statement);
    assertThat(response.statusCode()).as("%s: %s", statement, response.body()).isEqualTo(400);
    final JSONObject body = new JSONObject(response.body());
    assertThat(body.getString("exception")).isEqualTo(ValidationException.class.getName());
    assertThat(body.getString("exceptionArgs", null)).isEqualTo(kind);
  }

  private HttpResponse<String> command(final String language, final String command) throws Exception {
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(getServerHttpUrl(0, "/api/v1/command/" + getDatabaseName())))
        .header("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(new JSONObject().put("language", language).put("command", command).toString()))
        .build();
    try (final HttpClient client = HttpClient.newHttpClient()) {
      return client.send(request, HttpResponse.BodyHandlers.ofString());
    }
  }
}
