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

import com.arcadedb.database.Database;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7571: three operations advertise {@code application/x-ndjson} under their 200, and only two of them
 * refused a statement that is not provably read-only before it ran.
 * {@code PostCommandHandler.requireStreamableStatement()} was private to that class, and {@code GetQueryHandler}
 * is a sibling under {@code AbstractQueryHandler} rather than a subclass, so the GET operation reached
 * {@code database.query()} with nothing but {@code SQLQueryEngine}'s idempotency check standing in for the gate.
 * <p>
 * That check is strictly weaker. {@code BackupDatabaseStatement.isIdempotent()} answers true - it mutates no
 * record - while {@code getOperationTypes()} reports {@code {READ, CREATE}} for the archive it writes to the
 * server filesystem. It is the only SQL statement in that intersection, which is why it is the statement that
 * made the asymmetry observable: refused with 400 at {@code POST /command}, streamed happily at {@code GET /query}.
 * <p>
 * Every assertion below is written as a comparison between the GET operation and its POST twin rather than as a
 * bare status check, because "all three operations promise the same thing" is the property the issue is about. A
 * test that only asserted 400 on GET would still pass if the two POST operations later diverged the other way.
 */
public class Issue7571GetQueryStreamingReadOnlyGateIT extends BaseGraphServerTest {
  private static final String   TYPE_NAME    = "Gate7571";
  private static final int      ROW_COUNT    = 6;
  private static final String   NDJSON       = "application/x-ndjson";
  private static final Duration TIMEOUT      = Duration.ofSeconds(30);

  @Override
  protected void populateDatabase() {
    super.populateDatabase();

    final Database db = getDatabase(0);
    db.transaction(() -> {
      db.command("sql", "CREATE DOCUMENT TYPE " + TYPE_NAME + " BUCKETS 1");
      db.command("sql", "CREATE PROPERTY " + TYPE_NAME + ".idx INTEGER");
      for (int i = 0; i < ROW_COUNT; i++)
        db.newDocument(TYPE_NAME).set("idx", i).save();
    });
  }

  /**
   * The statement from the issue. It is idempotent, so {@code SQLQueryEngine.query()} - the only thing that
   * guarded the GET operation before this fix - lets it through and the backup runs while the response streams.
   * The gate reads the declared operation types instead and refuses it, the way the POST operation always did.
   */
  @Test
  void getQueryRefusesToStreamBackupDatabaseExactlyAsPostCommandDoes() throws Exception {
    final HttpResponse<String> viaGet = send(getRequest("BACKUP DATABASE", NDJSON));
    final HttpResponse<String> viaPost = send(postRequest("BACKUP DATABASE", "command", NDJSON));

    assertThat(viaGet.statusCode())
        .as("BACKUP DATABASE writes an archive to the server filesystem; it must not stream from GET /query "
            + "when POST /command refuses it, answered %s", viaGet.body())
        .isEqualTo(400);
    assertThat(viaGet.body()).contains("read-only statement");
    assertThat(viaGet.body())
        .as("the gate has to run before the statement, not after: a body carrying 'backupFile' would mean the "
            + "archive was already on disk by the time the refusal was decided")
        .doesNotContain("backupFile");

    assertThat(viaPost.statusCode()).as("the POST twin must still refuse it").isEqualTo(400);
    assertThat(viaGet.statusCode())
        .as("the two operations advertise the same media type under their 200, so they must answer the same "
            + "statement the same way")
        .isEqualTo(viaPost.statusCode());
  }

  /**
   * A statement that writes was already refused on GET, but by {@code QueryNotIdempotentException} mapped to a
   * generic "Query is not idempotent" 400 - a different message from the one the POST operations send, for a
   * reason that has nothing to do with the encoding. After the fix the streaming gate reaches it first, so a
   * client gets the same actionable message ("request the buffered encoding") from all three operations.
   */
  @Test
  void getQueryRefusesAWritingStatementWithTheSameMessageThePostOperationsSend() throws Exception {
    final String update = "UPDATE " + TYPE_NAME + " SET touched = true RETURN AFTER";

    final HttpResponse<String> viaGet = send(getRequest(update, NDJSON));
    final HttpResponse<String> viaPost = send(postRequest(update, "query", NDJSON));

    assertThat(viaGet.statusCode()).isEqualTo(400);
    assertThat(viaGet.body())
        .as("the refusal must name the encoding as the reason and the buffered alternative as the remedy, "
            + "not merely report that the statement is not idempotent")
        .contains("read-only statement");
    assertThat(viaPost.statusCode()).isEqualTo(400);
    assertThat(viaPost.body()).contains("read-only statement");

    // And the refusal is real on the GET path too: nothing was written.
    assertThat(countTouched()).isZero();
  }

  /**
   * The gate reads the statement's declared operation types, which are a property of the parsed statement and
   * not of the command text, so the spelling of the verb cannot get past it. Checked because the cheap
   * alternative - matching the text - is what a reviewer would reach for, and it would pass a bare
   * "BACKUP DATABASE" test while admitting every one of these.
   */
  @Test
  void theGateReadsTheParsedStatementSoNoSpellingOfBackupGetsThrough() throws Exception {
    for (final String spelling : List.of("backup database", "BaCkUp   DaTaBaSe", "  BACKUP DATABASE  ")) {
      final HttpResponse<String> response = send(getRequest(spelling, NDJSON));

      assertThat(response.statusCode()).as("spelling '%s' answered %s", spelling, response.body()).isEqualTo(400);
      assertThat(response.body()).contains("read-only statement");
      assertThat(response.body()).doesNotContain("backupFile");
    }
  }

  /**
   * The gate is scoped to the streaming encoding and must stay there. A read that was streamable before is
   * still streamed, and the buffered encoding is untouched for everything the stream turns away - which is what
   * makes the refusal a redirection to a working alternative rather than a loss of function.
   */
  @Test
  void theGateChangesNothingForAReadOrForTheBufferedEncoding() throws Exception {
    final HttpResponse<String> streamedRead = send(getRequest("SELECT idx FROM " + TYPE_NAME, NDJSON));
    assertThat(streamedRead.statusCode())
        .as("a plain SELECT is provably read-only and must still stream, answered %s", streamedRead.body())
        .isEqualTo(200);
    assertThat(streamedRead.headers().firstValue("Content-Type").orElse("")).contains(NDJSON);
    assertThat(streamedRead.body().lines().filter(l -> !l.isBlank()).count())
        .as("every row plus the trailer")
        .isEqualTo(ROW_COUNT + 1);

    // The buffered encoding is not gated: BACKUP DATABASE over 'Accept: application/json' still runs, so the
    // fix narrows no capability, it only refuses the encoding that cannot carry the statement soundly.
    final HttpResponse<String> bufferedBackup = send(getRequest("BACKUP DATABASE", "application/json"));
    assertThat(bufferedBackup.statusCode())
        .as("the buffered encoding must keep accepting the statement the stream refuses, answered %s",
            bufferedBackup.body())
        .isEqualTo(200);
  }

  private int countTouched() throws Exception {
    final HttpResponse<String> response = send(postRequest(
        "SELECT count(*) AS n FROM " + TYPE_NAME + " WHERE touched = true", "query", "application/json"));
    assertThat(response.statusCode()).isEqualTo(200);
    return new JSONObject(response.body()).getJSONArray("result").getJSONObject(0).getInt("n");
  }

  private String baseUrl() {
    return "http://localhost:" + getServer(0).getHttpServer().getPort() + "/api/v1";
  }

  private static String authorization() {
    return "Basic " + Base64.getEncoder()
        .encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8));
  }

  private HttpRequest getRequest(final String command, final String accept) {
    return getRequest(command, accept, "sql");
  }

  private HttpRequest getRequest(final String command, final String accept, final String language) {
    // URLEncoder targets application/x-www-form-urlencoded, where a space is '+'. This is a path segment, where
    // '+' is a literal plus and the space has to be %20 - otherwise the server parses a different statement and
    // answers 400 for a reason that has nothing to do with the gate under test.
    final String url = baseUrl() + "/query/" + getDatabaseName() + "/" + language + "/"
        + URLEncoder.encode(command, StandardCharsets.UTF_8).replace("+", "%20");
    return HttpRequest.newBuilder()
        .uri(URI.create(url))
        .timeout(TIMEOUT)
        .header("Authorization", authorization())
        .header("Accept", accept)
        .GET()
        .build();
  }

  private HttpRequest postRequest(final String command, final String operation, final String accept) {
    return HttpRequest.newBuilder()
        .uri(URI.create(baseUrl() + "/" + operation + "/" + getDatabaseName()))
        .timeout(TIMEOUT)
        .header("Authorization", authorization())
        .header("Content-Type", "application/json")
        .header("Accept", accept)
        .POST(HttpRequest.BodyPublishers.ofString(
            new JSONObject().put("language", "sql").put("command", command).toString()))
        .build();
  }

  private HttpResponse<String> send(final HttpRequest request) throws Exception {
    return HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(30)).build()
        .send(request, HttpResponse.BodyHandlers.ofString());
  }
}
