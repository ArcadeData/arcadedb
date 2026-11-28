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
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.exception.QueryNotIdempotentException;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.ServerControlPlane;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12904: four caller mistakes answered HTTP 500 - a polyglot statement sent to {@code /query}, {@code connect} /
 * {@code disconnect cluster} on a server without HA, a function returning no record used as a FROM target, and a write on a
 * type whose trigger body did not parse, which CREATE TRIGGER had accepted. Each is the caller's, so each answers 400, and the
 * trigger is refused where it is created rather than on every later write.
 */
class Issue12904CallerMistakesStatusTest extends BaseGraphServerTest {

  @BeforeEach
  void createType() {
    final Database database = getServerDatabase(0, getDatabaseName());
    if (!database.getSchema().existsType("Issue12904P"))
      database.command("sql", "CREATE DOCUMENT TYPE Issue12904P");
    if (!database.getSchema().existsType("Issue12904Log"))
      database.command("sql", "CREATE DOCUMENT TYPE Issue12904Log");
  }

  @Test
  void aPolyglotStatementSentToQueryIs400() throws Exception {
    final JSONObject answer = post("/api/v1/query/" + getDatabaseName(),
        new JSONObject().put("language", "js").put("command", "1 + 1"), 400);
    assertThat(answer.getString("exception", "")).isEqualTo(QueryNotIdempotentException.class.getName());
    assertThat(answer.getString("detail", "")).contains("Use command instead");
  }

  @Test
  void clusterCommandsWithoutHAAre400() throws Exception {
    for (final String command : new String[] { "disconnect cluster", "connect cluster x" }) {
      final JSONObject answer = post("/api/v1/server", new JSONObject().put("command", command), 400);
      assertThat(answer.getString("exception", "")).as(command)
          .isEqualTo(ServerControlPlane.OperationNotAvailableException.class.getName());
      assertThat(answer.getString("detail", "")).as(command).contains("High Availability");
    }
  }

  @Test
  void aFunctionReturningNoRecordUsedAsTargetIs400() throws Exception {
    final JSONObject answer = post("/api/v1/query/" + getDatabaseName(),
        new JSONObject().put("language", "sql").put("command", "SELECT FROM date('2024-01-01', 'yyyy-MM-dd')"), 400);
    assertThat(answer.getString("exception", "")).isEqualTo(CommandSemanticException.class.getName());
    assertThat(answer.getString("detail", "")).contains("did not return a record or RID");
  }

  @Test
  void createTriggerWithAnSqlBodyThatDoesNotParseIs400AndWritesKeepWorking() throws Exception {
    final JSONObject answer = command(
        "CREATE TRIGGER issue12904bad BEFORE CREATE ON TYPE Issue12904P EXECUTE SQL 'THIS IS NOT SQL'", 400);
    assertThat(answer.getString("exception", "")).isEqualTo(CommandSQLParsingException.class.getName());
    assertThat(getServerDatabase(0, getDatabaseName()).getSchema().existsTrigger("issue12904bad")).isFalse();

    command("INSERT INTO Issue12904P SET k = 1", 200);
  }

  @Test
  void createTriggerWithAJavaScriptBodyThatDoesNotParseIs400AndWritesKeepWorking() throws Exception {
    final JSONObject answer = command(
        "CREATE TRIGGER issue12904badjs BEFORE CREATE ON TYPE Issue12904P EXECUTE JAVASCRIPT 'var = ;'", 400);
    assertThat(answer.getString("exception", "")).isEqualTo(CommandParsingException.class.getName());
    assertThat(answer.getString("detail", "")).contains("issue12904badjs");
    assertThat(getServerDatabase(0, getDatabaseName()).getSchema().existsTrigger("issue12904badjs")).isFalse();

    command("INSERT INTO Issue12904P SET k = 3", 200);
  }

  @Test
  void createTriggerWithAnSqlBodyThatParsesIsStillAccepted() throws Exception {
    command("CREATE TRIGGER issue12904ok AFTER CREATE ON TYPE Issue12904P EXECUTE SQL \"INSERT INTO Issue12904Log SET what = 'x'\"",
        200);
    try {
      command("INSERT INTO Issue12904P SET k = 2", 200);
      assertThat(getServerDatabase(0, getDatabaseName()).countType("Issue12904Log", false)).isEqualTo(1);
    } finally {
      command("DROP TRIGGER issue12904ok", 200);
    }
  }

  private JSONObject command(final String sql, final int status) throws Exception {
    return post("/api/v1/command/" + getDatabaseName(), new JSONObject().put("language", "sql").put("command", sql), status);
  }

  private JSONObject post(final String path, final JSONObject payload, final int status) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(getServerHttpUrl(0, path)).toURL().openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    formatPayload(connection, payload);
    connection.connect();
    try {
      final int code = connection.getResponseCode();
      final String body;
      try (final InputStream in = code >= 400 ? connection.getErrorStream() : connection.getInputStream()) {
        body = FileUtils.readStreamAsString(in, "UTF-8");
      }
      assertThat(code).as(path + " " + payload + " -> " + body).isEqualTo(status);
      return new JSONObject(body);
    } finally {
      connection.disconnect();
    }
  }
}
