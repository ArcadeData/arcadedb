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

import com.arcadedb.exception.AlreadyExistsException;
import com.arcadedb.exception.SchemaException;
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
 * Issue #10153: creating a type, property, index, bucket or materialized view under a taken name answered HTTP 500 - "Cannot
 * execute command", or "Internal error" for the SchemaException ones - and logged a SEVERE stack trace, while a taken database,
 * user or token name answered 409 (#10073, #10074). A retry policy keyed on 5xx re-sent a statement that can only be refused
 * again. Every one of them is a 409 now, with a label naming the kind (it survives production mode, which conceals 'detail'),
 * and the Java client rebuilds the {@link AlreadyExistsException}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10153TakenSchemaNameAnswers409IT extends BaseGraphServerTest {

  @Test
  void everyTakenSchemaObjectNameAnswers409() throws Exception {
    assertConflict("sql", "create vertex type T10153", "Type already exists", "TYPE");
    // the name is taken by a vertex type
    assertThat(command("sql", "create edge type T10153").statusCode()).isEqualTo(409);
    assertConflict("sql", "create property T10153.p1 string", "Property already exists", "PROPERTY");
    assertConflict("sql", "create index on T10153 (p1) unique", "Index already exists", "INDEX");
    assertConflict("sql", "create bucket b10153", "Bucket already exists", "BUCKET");
    assertConflict("sql", "create materialized view mv10153 as select from T10153", "Materialized view already exists", "MATERIALIZED_VIEW");
    assertConflict("opencypher", "CREATE CONSTRAINT c10153 FOR (n:P) REQUIRE n.name IS UNIQUE", "Constraint already exists",
        "CONSTRAINT");

    // a rename onto a taken name was a 400, the status of a malformed request
    assertThat(command("sql", "create document type D10153").statusCode()).isEqualTo(200);
    final HttpResponse<String> rename = command("sql", "alter type D10153 name T10153");
    assertThat(rename.statusCode()).isEqualTo(409);
    assertThat(new JSONObject(rename.body()).getString("exception")).isEqualTo(AlreadyExistsException.class.getName());
  }

  @Test
  void theJavaClientRebuildsTheConflictWithItsKind() {
    try (final RemoteDatabase database = new RemoteDatabase("127.0.0.1", getServerHttpPort(0), getDatabaseName(), "root",
        DEFAULT_PASSWORD_FOR_TESTS)) {
      database.command("sql", "create vertex type Remote10153");

      assertThatThrownBy(() -> database.command("sql", "create vertex type Remote10153"))
          .isInstanceOf(AlreadyExistsException.class)
          .isInstanceOf(SchemaException.class)
          .hasMessageContaining("already exists")
          .satisfies(e -> assertThat(((AlreadyExistsException) e).getKind()).isEqualTo(AlreadyExistsException.Kind.TYPE));

      assertThatThrownBy(() -> database.getSchema().createDocumentType("Remote10153"))
          .isInstanceOf(AlreadyExistsException.class);
    }
  }

  /** Not every refusal became a conflict: a statement naming a type that does not exist keeps its answer. */
  @Test
  void aMissingTypeIsNotAConflict() throws Exception {
    final HttpResponse<String> missing = command("sql", "create property NoSuchType10153.p string");
    assertThat(missing.statusCode()).isNotEqualTo(409);
  }

  private void assertConflict(final String language, final String statement, final String label, final String kind)
      throws Exception {
    assertThat(command(language, statement).statusCode()).as("first %s", statement).isEqualTo(200);

    final HttpResponse<String> second = command(language, statement);
    assertThat(second.statusCode()).as("second %s: %s", statement, second.body()).isEqualTo(409);
    final JSONObject body = new JSONObject(second.body());
    assertThat(body.getString("error")).isEqualTo(label);
    assertThat(body.getString("exception")).isEqualTo(AlreadyExistsException.class.getName());
    assertThat(body.getString("exceptionArgs", null)).isEqualTo(kind);
    assertThat(body.getString("detail", "")).contains("already exists");
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
