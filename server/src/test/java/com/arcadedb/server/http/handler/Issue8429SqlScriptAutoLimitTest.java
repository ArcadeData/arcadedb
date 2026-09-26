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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8429: {@code POST /api/v1/command} pushed its automatic {@code LIMIT} down into a {@code sqlscript} that
 * starts with {@code SELECT} by appending {@code " limit N"} to the WHOLE text, so the LIMIT landed on the script's
 * LAST statement: a trailing {@code DELETE} or {@code UPDATE} silently stopped at N records, and a trailing
 * {@code INSERT ... SET} failed to parse.
 * <p>
 * The default cap is lowered to {@link #DEFAULT_LIMIT} rows so the truncation shows on {@link #TOTAL_ROWS} records
 * instead of on the 20,001 of the report.
 */
class Issue8429SqlScriptAutoLimitTest extends BaseGraphServerTest {
  private static final int    DEFAULT_LIMIT = 10;
  private static final int    TOTAL_ROWS    = 30;
  private static final String TYPE_NAME     = "Issue8429Doc";

  private final HttpClient client = HttpClient.newHttpClient();

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    config.setValue(GlobalConfiguration.SERVER_HTTP_QUERY_DEFAULT_LIMIT, DEFAULT_LIMIT);
  }

  @BeforeEach
  void createRows() {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.getSchema().createDocumentType(TYPE_NAME);
    database.transaction(() -> {
      for (int i = 0; i < TOTAL_ROWS; i++)
        database.newDocument(TYPE_NAME).set("i", i).set("x", 0).save();
    });
  }

  @Test
  void aTrailingDeleteRemovesEveryRecord() throws Exception {
    final HttpResponse<String> response = sqlScript("SELECT count(*) FROM " + TYPE_NAME + "; DELETE FROM " + TYPE_NAME);

    assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
    assertThat(countWhere("")).isZero();
    // The statement's own report of what it deleted, which read 20001 in the issue.
    assertThat(new JSONObject(response.body()).getJSONArray("result").getJSONObject(0).getLong("count")).isEqualTo(TOTAL_ROWS);
  }

  @Test
  void aTrailingUpdateTouchesEveryRecord() throws Exception {
    final HttpResponse<String> response = sqlScript("SELECT count(*) FROM " + TYPE_NAME + "; UPDATE " + TYPE_NAME + " SET x = 1");

    assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
    assertThat(countWhere(" WHERE x = 1")).isEqualTo(TOTAL_ROWS);
  }

  @Test
  void aTrailingInsertSetParses() throws Exception {
    final HttpResponse<String> response = sqlScript("SELECT count(*) FROM " + TYPE_NAME + "; INSERT INTO " + TYPE_NAME + " SET i = -1");

    assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
    assertThat(countWhere(" WHERE i = -1")).isEqualTo(1L);
  }

  @Test
  void statementsSeparatedOnlyByALineBreakAreNotLimitedEither() throws Exception {
    // The script grammar makes the semicolon optional, so a text holding no ';' at all can still be two statements:
    // no textual probe for a separator can tell a single-statement script from this one.
    final HttpResponse<String> response = sqlScript("SELECT count(*) FROM " + TYPE_NAME + "\nDELETE FROM " + TYPE_NAME);

    assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
    assertThat(countWhere("")).isZero();
  }

  @Test
  void aSingleSelectScriptIsStillCappedByTheDefaultLimit() throws Exception {
    final HttpResponse<String> response = sqlScript("SELECT FROM " + TYPE_NAME);

    assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
    final JSONObject json = new JSONObject(response.body());
    assertThat(json.getJSONArray("result").length()).isEqualTo(DEFAULT_LIMIT);
    assertThat(json.getInt("returned")).isEqualTo(DEFAULT_LIMIT);
    assertThat(json.getBoolean("truncated")).isTrue();
  }

  @Test
  void aReadScriptEndingWithASelectIsStillCapped() throws Exception {
    // The LIMIT still lands on the last statement when that statement is the query whose rows the script returns.
    final HttpResponse<String> response = sqlScript("SELECT count(*) FROM " + TYPE_NAME + "; SELECT FROM " + TYPE_NAME);

    assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
    final JSONObject json = new JSONObject(response.body());
    assertThat(json.getJSONArray("result").length()).isEqualTo(DEFAULT_LIMIT);
    assertThat(json.getInt("returned")).isEqualTo(DEFAULT_LIMIT);
    assertThat(json.getBoolean("truncated")).isTrue();
  }

  private long countWhere(final String where) {
    return getServerDatabase(0, getDatabaseName()).query("sql", "SELECT count(*) AS n FROM " + TYPE_NAME + where).next()
        .<Number>getProperty("n").longValue();
  }

  private HttpResponse<String> sqlScript(final String script) throws Exception {
    final JSONObject payload = new JSONObject().put("language", "sqlscript").put("command", script);
    final HttpRequest request = HttpRequest.newBuilder(
            URI.create(getServerHttpUrl(0, "/api/v1/command/" + getDatabaseName())))
        .header("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(payload.toString()))
        .build();
    return client.send(request, BodyHandlers.ofString());
  }
}
