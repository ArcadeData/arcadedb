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
import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.exception.ObjectInUseException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.exception.TypeMismatchException;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.Base64;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11803: SQL statements that fail on something the caller can fix answered HTTP 500 "Internal error", because the engine
 * raised a plain {@code CommandExecutionException} for them. Each one is answered as the caller's mistake now: 409 for a statement
 * refused for what the database holds (a type that still has records, an indexed property), 400 for the others.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11803SqlUserErrorStatusTest extends BaseGraphServerTest {
  private static final String V = "Issue11803V";
  private static final String E = "Issue11803E";

  @BeforeEach
  void createSchema() {
    final Database database = getServerDatabase(0, getDatabaseName());
    for (final String statement : new String[] { "CREATE VERTEX TYPE " + V, "CREATE PROPERTY " + V + ".id INTEGER",
        "CREATE PROPERTY " + V + ".s STRING", "CREATE INDEX ON " + V + " (id) UNIQUE", "CREATE EDGE TYPE " + E })
      database.command("sql", statement);
    database.transaction(() -> {
      database.command("sql", "INSERT INTO " + V + " SET id = 1");
      database.command("sql", "INSERT INTO " + V + " SET id = 2");
      database.command("sql", "CREATE EDGE " + E + " FROM (SELECT FROM " + V + " WHERE id = 1) TO (SELECT FROM " + V + " WHERE id = 2)");
    });
  }

  @Test
  void aStatementRefusedForWhatTheDatabaseHoldsIs409() throws Exception {
    assertAnswer("TRUNCATE TYPE " + V, 409, ObjectInUseException.class, "UNSAFE");
    assertAnswer("TRUNCATE TYPE " + E, 409, ObjectInUseException.class, "UNSAFE");
    for (final Bucket bucket : getServerDatabase(0, getDatabaseName()).getSchema().getType(V).getBuckets(false))
      if (bucket.count() > 0)
        assertAnswer("TRUNCATE BUCKET " + bucket.getName(), 409, ObjectInUseException.class, "UNSAFE");
    assertAnswer("DROP TYPE " + V, 409, ObjectInUseException.class, "contains Vertices");
    assertAnswer("DROP TYPE " + E, 409, ObjectInUseException.class, "contains Edges");
    assertAnswer("DROP PROPERTY " + V + ".id", 409, ObjectInUseException.class, "FORCE");

    // nothing was changed by any of them
    final Database database = getServerDatabase(0, getDatabaseName());
    assertThat(database.countType(V, false)).isEqualTo(2);
    assertThat(database.countType(E, false)).isEqualTo(1);
    assertThat(database.getSchema().getType(V).existsProperty("id")).isTrue();
  }

  @Test
  void alterPropertyTypeIsRefusedWithoutAdviceThatAlsoFails() throws Exception {
    // the bare word used to be refused for its value, with the advice to quote it, and the quoted form then for the setting
    final JSONObject bare = assertAnswer("ALTER PROPERTY " + V + ".s TYPE LONG", 400, CommandSQLParsingException.class,
        "cannot be changed");
    assertThat(bare.getString("detail")).doesNotContain("Quote it");
    final JSONObject quoted = assertAnswer("ALTER PROPERTY " + V + ".s TYPE 'LONG'", 400, CommandSQLParsingException.class,
        "cannot be changed");
    assertThat(quoted.getString("detail")).contains("UPDATE " + V);

    // any other setting ALTER PROPERTY does not have names the ones it does
    assertAnswer("ALTER PROPERTY " + V + ".s NOSUCH 1", 400, CommandSQLParsingException.class, "MANDATORY");
    assertThat(getServerDatabase(0, getDatabaseName()).getSchema().getType(V).getProperty("s").getType().name()).isEqualTo("STRING");
  }

  @Test
  void aTargetOfTheWrongKindIs400WithAWellFormedName() throws Exception {
    final JSONObject edge = assertAnswer(
        "CREATE EDGE " + V + " FROM (SELECT FROM " + V + " WHERE id = 1) TO (SELECT FROM " + V + " WHERE id = 2)", 400,
        SchemaException.class, "Type '" + V + "' is not an edge type");
    assertThat(edge.getString("detail")).doesNotContain("' '");
    assertAnswer("CREATE VERTEX " + E, 400, SchemaException.class, "Type '" + E + "' is not a vertex type");
  }

  @Test
  void aLimitOrSkipThatIsNoNumberIs400() throws Exception {
    assertAnswer("SELECT FROM " + V + " LIMIT 'a'", 400, TypeMismatchException.class, "Invalid value for LIMIT: a");
    assertAnswer("SELECT FROM " + V + " SKIP 'a'", 400, TypeMismatchException.class, "Invalid value for SKIP: a");
  }

  @Test
  void rebuildingAnIndexThatDoesNotExistIs400() throws Exception {
    assertAnswer("REBUILD INDEX Issue11803Nope", 400, SchemaException.class, "Issue11803Nope");
  }

  @Test
  void importingASourceThatDoesNotExistIs400() throws Exception {
    final File missing = new File(System.getProperty("java.io.tmpdir"), "issue11803-" + System.nanoTime() + ".jsonl");
    assertThat(missing).doesNotExist();
    assertAnswer("IMPORT DATABASE file://" + missing.getAbsolutePath(), 400, IllegalArgumentException.class, "was not found");
  }

  @Test
  void theNeighboursKeepTheirAnswer() throws Exception {
    assertAnswer("DROP TYPE Issue11803Nope", 400, SchemaException.class, "Issue11803Nope");
    assertAnswer("TRUNCATE TYPE Issue11803Nope", 400, SchemaException.class, "Issue11803Nope");
    assertAnswer("DROP PROPERTY " + V + ".nope", 400, SchemaException.class, "nope");
    // and UNSAFE / FORCE still do what the refusals advise
    assertThat(post("DROP PROPERTY " + V + ".id FORCE").getResponseCode()).isEqualTo(200);
    assertThat(post("TRUNCATE TYPE " + E + " UNSAFE").getResponseCode()).isEqualTo(200);
  }

  private JSONObject assertAnswer(final String statement, final int status, final Class<? extends Throwable> exception,
      final String detail) throws Exception {
    final HttpURLConnection connection = post(statement);
    try {
      assertThat(connection.getResponseCode()).as(statement).isEqualTo(status);
      final String body;
      try (final InputStream in = connection.getErrorStream()) {
        body = FileUtils.readStreamAsString(in, "UTF-8");
      }
      final JSONObject answer = new JSONObject(body);
      assertThat(answer.getString("exception", "")).as(statement).isEqualTo(exception.getName());
      assertThat(answer.getString("detail", "")).as(statement).contains(detail);
      return answer;
    } finally {
      connection.disconnect();
    }
  }

  private HttpURLConnection post(final String statement) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(
        getServerHttpUrl(0, "/api/v1/command/" + getDatabaseName())).toURL().openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    formatPayload(connection, "sql", statement, null, Map.of());
    connection.connect();
    return connection;
  }
}
