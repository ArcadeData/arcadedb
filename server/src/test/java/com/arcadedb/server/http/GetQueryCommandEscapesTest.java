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

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #12913: {@code GET /api/v1/query/{db}/{language}/{command}} must decode every percent-escape of
 * the command the way {@code POST /api/v1/query/{db}} reads the same statement from its JSON body. {@code %2F} and
 * {@code %5C} used to reach the engine still encoded (Undertow leaves them undecoded in the path) and {@code %3F} cut the
 * command at the question mark.
 */
class GetQueryCommandEscapesTest extends BaseGraphServerTest {

  private static final String DATABASE_NAME = "graph";

  @Test
  void everyEscapeOfTheCommandMeansWhatItSays() throws Exception {
    testEachServer(serverIndex -> {
      // the statement as the user means it -> the GET and POST answers must be the same row
      final String[] statements = {//
          "select 'a/b' as s",//
          "select 'x\\\\y' as s",//
          "select 1/2 as s",//
          "select 'q?r' as s",//
          "select '?' as s",//
          "select 'a%b' as s",//
          "select 'a%2Fb' as s",//
          "select 'a+b c' as s",//
          "select 'h#i&j=k;l' as s",//
          "select '\u00e9\u20ac\uD83D\uDE00' as s",//
          "select 'a//b\\\\\\\\c' as s",//
      };

      for (final String statement : statements) {
        final JSONObject post = post(serverIndex, statement);
        assertThat(post.getInt("status")).as("POST of %s", statement).isEqualTo(200);

        final JSONObject get = get(serverIndex, percentEncode(statement));
        assertThat(get.getInt("status")).as("GET of %s", statement).isEqualTo(200);
        assertThat(rows(get)).as("GET of %s answers like POST", statement).isEqualTo(rows(post));
      }
    });
  }

  @Test
  void theGetAnswerIsTheStatementsOwnValue() throws Exception {
    testEachServer(serverIndex -> {
      assertThat(firstValue(get(serverIndex, "select%20%27a%2Fb%27%20as%20s"))).isEqualTo("a/b");
      assertThat(firstValue(get(serverIndex, "select%20%27x%5C%5Cy%27%20as%20s"))).isEqualTo("x\\y");
      assertThat(firstValue(get(serverIndex, "select%201%2F2%20as%20s"))).isEqualTo(0.5);
      assertThat(firstValue(get(serverIndex, "select%20%27a%3Fb%27%20as%20s"))).isEqualTo("a?b");
      // lowercase hex digits are the same escapes
      assertThat(firstValue(get(serverIndex, "select%20%27a%2fb%27%20as%20s"))).isEqualTo("a/b");
    });
  }

  @Test
  void aPlusStaysAPlusInThePath() throws Exception {
    testEachServer(serverIndex -> {
      assertThat(firstValue(get(serverIndex, "select%20%27a+b%27%20as%20s"))).isEqualTo("a+b");
      assertThat(firstValue(get(serverIndex, "select%201+2%20as%20s"))).isEqualTo(3);
    });
  }

  @Test
  void anUnencodedSlashIsStillNotPartOfTheCommand() throws Exception {
    testEachServer(serverIndex -> assertThat(get(serverIndex, "select%201/2%20as%20s").getInt("status")).isEqualTo(404));
  }

  @Test
  void queryParametersAreNotTakenForTheCommand() throws Exception {
    testEachServer(serverIndex -> {
      // the real query string still carries its parameters; only the encoded ? belongs to the command
      assertThat(firstValue(get(serverIndex, "select%20%27a%3Fb%27%20as%20s?serializer=record&limit=5"))).isEqualTo("a?b");
    });
  }

  private static String rows(final JSONObject answer) {
    return new JSONObject(answer.getString("body")).getJSONArray("result").toString();
  }

  private static Object firstValue(final JSONObject answer) {
    assertThat(answer.getInt("status")).as(answer.getString("body")).isEqualTo(200);
    final JSONArray result = new JSONObject(answer.getString("body")).getJSONArray("result");
    assertThat(result.length()).isEqualTo(1);
    return result.getJSONObject(0).get("s");
  }

  private JSONObject get(final int serverIndex, final String encodedCommand) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(
        getServerHttpUrl(serverIndex, "/api/v1/query/" + DATABASE_NAME + "/sql/" + encodedCommand)).toURL().openConnection();
    connection.setRequestMethod("GET");
    authenticate(connection);
    return answer(connection);
  }

  private JSONObject post(final int serverIndex, final String statement) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(
        getServerHttpUrl(serverIndex, "/api/v1/query/" + DATABASE_NAME)).toURL().openConnection();
    connection.setRequestMethod("POST");
    authenticate(connection);
    formatPayload(connection, new JSONObject().put("language", "sql").put("command", statement));
    connection.connect();
    return answer(connection);
  }

  private void authenticate(final HttpURLConnection connection) {
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
  }

  private JSONObject answer(final HttpURLConnection connection) throws Exception {
    try {
      final int status = connection.getResponseCode();
      final String body =
          status == 200 ? readResponse(connection) : connection.getErrorStream() == null ? "" : readError(connection);
      return new JSONObject().put("status", status).put("body", body);
    } finally {
      connection.disconnect();
    }
  }

  /** Every byte that is not an unreserved URL character becomes %XX (what curl --data-urlencode / requests.quote do). */
  private static String percentEncode(final String text) {
    final StringBuilder encoded = new StringBuilder();
    for (final byte b : text.getBytes(StandardCharsets.UTF_8)) {
      final int c = b & 0xFF;
      if (c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_' || c == '.' || c == '~')
        encoded.append((char) c);
      else
        encoded.append('%').append(Character.toUpperCase(Character.forDigit(c >> 4, 16))).append(
            Character.toUpperCase(Character.forDigit(c & 0xF, 16)));
    }
    return encoded.toString();
  }
}
