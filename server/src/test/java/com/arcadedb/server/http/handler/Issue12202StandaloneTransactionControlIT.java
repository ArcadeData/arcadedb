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

import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.http.HttpSessionManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12202: a SQL {@code BEGIN}, {@code COMMIT} or {@code ROLLBACK} sent as a command of its own outside a session was answered
 * OK and did nothing; issue #12232: so was an openCypher {@code START TRANSACTION}, {@code COMMIT} or {@code ROLLBACK}. The server
 * runs such a command in a transaction of that one request, so a {@code BEGIN} opened nothing the next command joined and a later
 * {@code ROLLBACK} undid nothing. It is refused now with a client error that names the transaction API. A {@code sqlscript}
 * holding the whole block, and a statement inside a session, run as before.
 */
class Issue12202StandaloneTransactionControlIT extends BaseGraphServerTest {

  /** The words of the refusal that no other answer carries. */
  private static final String REFUSAL = "cannot run as a command of its own";

  @ParameterizedTest
  @ValueSource(strings = { "begin", "BEGIN", "  commit", "rollback", "/* tx */ ROLLBACK", "begin isolation repeatable_read" })
  void aTransactionControlOutsideASessionIsAClientError(final String statement) throws Exception {
    final HttpResponse<String> response = command("sql", statement, null);

    assertThat(response.statusCode()).isEqualTo(400);
    assertThat(response.body()).contains("CommandSemanticException").contains("RemoteDatabase.begin()").contains("sqlscript");
  }

  /** The openCypher spelling has the same gap (issue #12232). */
  @ParameterizedTest
  @ValueSource(strings = { "START TRANSACTION", "start transaction isolation read_committed", "COMMIT", "rollback" })
  void anOpenCypherTransactionControlOutsideASessionIsAClientError(final String statement) throws Exception {
    final HttpResponse<String> response = command("opencypher", statement, null);

    assertThat(response.statusCode()).isEqualTo(400);
    assertThat(response.body()).contains("CommandSemanticException").contains("RemoteDatabase.begin()");
  }

  /**
   * Behind a prefix that still runs the statement (SQL PROFILE, openCypher PROFILE and EXPLAIN), the transaction control is refused
   * too (review of PR #12230).
   */
  @ParameterizedTest
  @CsvSource({ "opencypher,PROFILE START TRANSACTION", "opencypher,PROFILE COMMIT", "opencypher,PROFILE ROLLBACK",
      "opencypher,EXPLAIN START TRANSACTION", "sql,PROFILE BEGIN", "sql,profile rollback" })
  void aTransactionControlBehindAnExecutingPrefixIsAClientError(final String language, final String statement) throws Exception {
    final HttpResponse<String> response = command(language, statement, null);

    assertThat(response.statusCode()).as(response.body()).isEqualTo(400);
    assertThat(response.body()).contains("CommandSemanticException").contains("RemoteDatabase.begin()");
  }

  /**
   * The text probe in front of the parse has to let through every spelling the engines read as a transaction control: one behind
   * an openCypher {@code //} line comment, and one behind more than one SQL PROFILE, were answered OK and did nothing.
   */
  @ParameterizedTest
  @CsvSource({ "opencypher,'// undo the batch\nROLLBACK'", "opencypher,'// open\nSTART TRANSACTION'",
      "opencypher,'/* a */ // b\nPROFILE // c\n COMMIT'", "sql,PROFILE PROFILE BEGIN", "sql,'profile /* a */ PROFILE -- b\n rollback'" })
  void aTransactionControlBehindCommentsOrRepeatedPrefixesIsAClientError(final String language, final String statement)
      throws Exception {
    final HttpResponse<String> response = command(language, statement, null);

    assertThat(response.statusCode()).as(response.body()).isEqualTo(400);
    assertThat(response.body()).contains("CommandSemanticException").contains(REFUSAL);
  }

  /**
   * A level the engine cannot provide keeps the error of its own (issue #10386), which says more: the statement is not answered
   * with the refusal of a standalone transaction control, in either language.
   */
  @ParameterizedTest
  @CsvSource({ "sql,BEGIN ISOLATION SERIALIZABLE", "opencypher,START TRANSACTION ISOLATION SERIALIZABLE" })
  void anUnsupportedIsolationLevelKeepsItsOwnError(final String language, final String statement) throws Exception {
    final HttpResponse<String> response = command(language, statement, null);

    assertThat(response.statusCode()).as(response.body()).isBetween(400, 499);
    assertThat(response.body()).contains("SERIALIZABLE").contains("REPEATABLE_READ").doesNotContain(REFUSAL);
  }

  /** The probe lets a text through that merely starts like a transaction control: the parser decides, and nothing is refused. */
  @ParameterizedTest
  @CsvSource({ "sql,beginning of nothing", "sql,committed", "opencypher,started" })
  void aTextThatOnlyStartsLikeATransactionControlIsNotRefusedAsOne(final String language, final String statement) throws Exception {
    final HttpResponse<String> response = command(language, statement, null);

    assertThat(response.statusCode()).as(response.body()).isNotEqualTo(200);
    assertThat(response.body()).doesNotContain(REFUSAL);
  }

  /** A SQL EXPLAIN only plans the statement it wraps, so it is not refused. */
  @Test
  void aSqlExplainOfATransactionControlIsNotRefused() throws Exception {
    final HttpResponse<String> response = command("sql", "EXPLAIN BEGIN", null);

    assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
  }

  @Test
  void anOpenCypherCommitInsideASessionRunsAsBefore() throws Exception {
    final HttpResponse<String> begin = post("/api/v1/begin/" + getDatabaseName(), "", null);
    assertThat(begin.statusCode()).isEqualTo(204);
    final String session = begin.headers().firstValue(HttpSessionManager.ARCADEDB_SESSION_ID).orElseThrow();

    assertThat(command("opencypher", "CREATE (:C12232 {k: 1})", session).statusCode()).isEqualTo(200);
    assertThat(command("opencypher", "COMMIT", session).statusCode()).isEqualTo(200);
  }

  @Test
  void theJavaClientRaisesTheRefusalAndNothingIsRolledBackBehindTheCallersBack() {
    try (final RemoteDatabase db = new RemoteDatabase("localhost", getServerHttpPort(), getDatabaseName(), "root",
        DEFAULT_PASSWORD_FOR_TESTS)) {
      db.command("sql", "CREATE DOCUMENT TYPE D12202");

      assertThatThrownBy(() -> db.command("sql", "begin")).isInstanceOf(CommandSemanticException.class)
          .hasMessageContaining("RemoteDatabase.begin()");
      assertThatThrownBy(() -> db.command("sql", "rollback")).isInstanceOf(CommandSemanticException.class);

      // THE TRANSACTION API STILL ROLLS BACK
      db.begin();
      db.command("sql", "INSERT INTO D12202 SET k = 'rolledBack'");
      db.rollback();
      assertThat(db.query("sql", "SELECT count(@rid) AS c FROM D12202").next().<Number>getProperty("c").longValue()).isZero();
    }
  }

  @Test
  void aScriptHoldingTheWholeBlockStillRuns() throws Exception {
    assertThat(command("sql", "CREATE DOCUMENT TYPE S12202", null).statusCode()).isEqualTo(200);

    assertThat(command("sqlscript", "BEGIN; INSERT INTO S12202 SET k = 1; ROLLBACK;", null).statusCode()).isEqualTo(200);
    assertThat(command("sqlscript", "BEGIN; INSERT INTO S12202 SET k = 2; COMMIT;", null).statusCode()).isEqualTo(200);

    final HttpResponse<String> count = command("sql", "SELECT count(@rid) AS c FROM S12202", null);
    assertThat(new JSONObject(count.body()).getJSONArray("result").getJSONObject(0).getLong("c")).isEqualTo(1L);
  }

  @Test
  void aTransactionControlInsideASessionRunsAsBefore() throws Exception {
    assertThat(command("sql", "CREATE DOCUMENT TYPE T12202", null).statusCode()).isEqualTo(200);

    final HttpResponse<String> begin = post("/api/v1/begin/" + getDatabaseName(), "", null);
    assertThat(begin.statusCode()).isEqualTo(204);
    final String session = begin.headers().firstValue(HttpSessionManager.ARCADEDB_SESSION_ID).orElseThrow();

    assertThat(command("sql", "INSERT INTO T12202 SET k = 1", session).statusCode()).isEqualTo(200);
    assertThat(command("sql", "COMMIT", session).statusCode()).isEqualTo(200);
  }

  private HttpResponse<String> command(final String language, final String command, final String session) throws Exception {
    return post("/api/v1/command/" + getDatabaseName(), new JSONObject().put("language", language).put("command", command).toString(),
        session);
  }

  private HttpResponse<String> post(final String path, final String body, final String session) throws Exception {
    final HttpRequest.Builder request = HttpRequest.newBuilder()
        .uri(URI.create(getServerHttpUrl(path)))
        .header("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(body));
    if (session != null)
      request.header(HttpSessionManager.ARCADEDB_SESSION_ID, session);
    try (final HttpClient client = HttpClient.newHttpClient()) {
      return client.send(request.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
    }
  }
}
