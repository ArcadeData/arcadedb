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

import com.arcadedb.database.Database;
import com.arcadedb.exception.DeletedEntityException;
import com.arcadedb.exception.ExternalResourceException;
import com.arcadedb.exception.ObjectInUseException;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.Base64;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11974: three openCypher user errors answered HTTP 500 (a {@code CommandExecutionException} no arm classified). They
 * are the caller's: a DELETE of a node that still has relationships is 409, a use of a node the statement deleted is 404, and a
 * {@code LOAD CSV} URL that cannot be opened is 400.
 */
class Issue11974CypherUserErrorStatusTest extends BaseGraphServerTest {

  @BeforeEach
  void createGraph() {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.transaction(() -> database.command("opencypher", "CREATE (:Issue11974A {x: 1})-[:Issue11974R]->(:Issue11974B), (:Issue11974C {x: 1})"));
  }

  @Test
  void deletingANodeThatStillHasRelationshipsIs409() throws Exception {
    assertAnswer("MATCH (n:Issue11974A) DELETE n", 409, ObjectInUseException.class, "DeleteConnectedNode");
  }

  @Test
  void usingANodeDeletedEarlierInTheStatementIs404() throws Exception {
    assertAnswer("MATCH (n:Issue11974C) DELETE n RETURN n.x AS x", 404, DeletedEntityException.class, "DeletedEntityAccess");
    assertAnswer("MATCH (n:Issue11974C) DELETE n CREATE (n)-[:Issue11974R]->(:Issue11974B)", 404, DeletedEntityException.class,
        "DeletedEntityAccess");
    assertThat(getServerDatabase(0, getDatabaseName()).countType("Issue11974C", false)).isEqualTo(1);
  }

  @Test
  void aLoadCsvFileThatDoesNotExistIs400() throws Exception {
    assertAnswer("LOAD CSV FROM 'file:///Issue11974-no-such-file.csv' AS l RETURN l", 400, ExternalResourceException.class,
        "Error opening CSV file");
  }

  private void assertAnswer(final String statement, final int status, final Class<? extends Throwable> exception, final String detail)
      throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(
        getServerHttpUrl(0, "/api/v1/command/" + getDatabaseName())).toURL().openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    formatPayload(connection, "opencypher", statement, null, Map.of());
    connection.connect();
    try {
      assertThat(connection.getResponseCode()).as(statement).isEqualTo(status);
      final String body;
      try (final InputStream in = connection.getErrorStream()) {
        body = FileUtils.readStreamAsString(in, "UTF-8");
      }
      final JSONObject answer = new JSONObject(body);
      assertThat(answer.getString("exception", "")).as(statement).isEqualTo(exception.getName());
      assertThat(answer.getString("detail", "")).as(statement).contains(detail);
    } finally {
      connection.disconnect();
    }
  }
}
