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
package com.arcadedb.server.http.handler;

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for GHSA-qwgr-2c45-63xx: the "create database" / "drop database" server commands
 * built an on-disk path from the caller-supplied database name without any sanitization, so a name
 * containing {@code ../} sequences (or an absolute path) let an authenticated root account write and
 * later recursively delete files anywhere the server process could reach, entirely outside the
 * configured {@code arcadedb.server.databaseDirectory}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PostServerCommandPathTraversalIT extends BaseGraphServerTest {
  private final HttpClient client = HttpClient.newHttpClient();

  @ParameterizedTest
  @ValueSource(strings = {
      "../../../../../../tmp/arcadedb-traversal-poc",
      "../arcadedb-traversal-sibling",
      "sub/../../arcadedb-traversal-escape",
      "foo/bar",
      "foo\\bar"
  })
  void createDatabaseRejectsPathTraversal(final String maliciousName) throws Exception {
    // The escaped directory that the traversal payload would resolve to, relative to the server working dir.
    final File escapeTarget = new File("./target/databases", maliciousName).getCanonicalFile();

    final HttpResponse<String> response = executeServerCommand("create database " + maliciousName);

    // The request must be rejected...
    assertThat(response.statusCode())
        .as("create database with traversal payload '%s' must be rejected, body=%s", maliciousName, response.body())
        .isIn(400, 403, 500);

    // ...and no database files may have been written outside the configured database directory.
    assertThat(escapeTarget.exists())
        .as("no directory must be created outside the database directory at %s", escapeTarget.getAbsolutePath())
        .isFalse();

    // The malicious name must not appear in the registry either.
    final HttpResponse<String> list = executeServerCommand("list databases");
    assertThat(list.body())
        .as("traversal name must not be registered")
        .doesNotContain(maliciousName);
  }

  @Test
  void dropDatabaseRejectsPathTraversal() throws Exception {
    final HttpResponse<String> response = executeServerCommand("drop database ../../../../../../tmp/arcadedb-traversal-poc");
    assertThat(response.statusCode())
        .as("drop database with traversal payload must be rejected, body=%s", response.body())
        .isIn(400, 403, 500);
    // The error must be a validation/not-found error, never a successful recursive delete outside the sandbox.
    assertThat(response.body()).doesNotContain("\"result\":\"ok\"");
  }

  /**
   * Sanity check that a plain, legitimate database name still works after the validation is in place.
   */
  @Test
  void createDatabaseAllowsLegitimateName() throws Exception {
    executeServerCommand("drop database traversal_ok_db");

    HttpResponse<String> response = executeServerCommand("create database traversal_ok_db");
    assertThat(response.statusCode()).isEqualTo(200);

    final HttpResponse<String> list = executeServerCommand("list databases");
    assertThat(list.body()).contains("traversal_ok_db");

    response = executeServerCommand("drop database traversal_ok_db");
    assertThat(response.statusCode()).isEqualTo(200);
  }

  private HttpResponse<String> executeServerCommand(final String command) throws Exception {
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(new URI("http://localhost:2480/api/v1/server"))
        .POST(HttpRequest.BodyPublishers.ofString(new JSONObject()
            .put("command", command)
            .toString()))
        .setHeader("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()))
        .build();

    return client.send(request, BodyHandlers.ofString());
  }
}
