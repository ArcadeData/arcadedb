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

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.StaticBaseServerTest;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12425: the graph statements answered HTTP 500 for a mistake of the caller, and one of them leaked a raw
 * {@code java.lang.ClassCastException}. The HTTP API reference reserves 500 for a failure of the server itself and lists 400
 * for a statement the server refuses to run. Nothing is written by any of them.
 */
class Issue12425CallerMistakesInGraphStatementsAre400Test extends BaseGraphServerTest {

  @Test
  void anEndpointThatIsADocumentIsRefusedWithAMessageAndNothingIsWritten() throws Exception {
    final String documentRid = createFixtures();

    final Response response = command("sql", "CREATE EDGE Link12425 FROM " + documentRid + " TO " + vertexRid);

    assertThat(response.status).as(response.body).isEqualTo(400);
    assertThat(response.body).as("the answer names the cause").contains("Invalid vertex for edge creation");
    assertThat(response.body).as("the answer names the record").contains(documentRid);
    assertThat(response.body).as("no Java class name reaches the client").doesNotContain("java.lang.ClassCastException");
    assertThat(edgeCount()).as("nothing is written").isZero();
  }

  @Test
  void anEndpointThatIsADocumentRidGivenAsAStringIsRefusedNotAnsweredAsMissing() throws Exception {
    final String documentRid = createFixtures();

    final Response response = command("sql", "CREATE EDGE Link12425 FROM :from TO :to",
        new JSONObject().put("from", documentRid).put("to", vertexRid));

    assertThat(response.status).as("the record exists, so 404 would be a wrong answer: " + response.body).isEqualTo(400);
    assertThat(response.body).contains("Invalid vertex for edge creation");
    assertThat(response.body).contains(documentRid);
    assertThat(edgeCount()).as("nothing is written").isZero();
  }

  @Test
  void aQueryThatReturnsADocumentAsAnEndpointIsRefusedWithAMessageAndNothingIsWritten() throws Exception {
    createFixtures();

    final Response from = command("sql", "CREATE EDGE Link12425 FROM (SELECT FROM Doc12425) TO (SELECT FROM V12425)");
    assertThat(from.status).as(from.body).isEqualTo(400);
    assertThat(from.body).contains("Invalid vertex for edge creation");

    final Response to = command("sql", "CREATE EDGE Link12425 FROM (SELECT FROM V12425) TO (SELECT FROM Doc12425)");
    assertThat(to.status).as(to.body).isEqualTo(400);
    assertThat(to.body).contains("Invalid vertex for edge creation");

    assertThat(edgeCount()).as("nothing is written").isZero();
  }

  @Test
  void aRidThatIsNotARidIsRefusedWithAMessage() throws Exception {
    createFixtures();

    final Response response = command("sql", "MATCH {rid: 'garbage', as: p} RETURN p");

    assertThat(response.status).as(response.body).isEqualTo(400);
    assertThat(response.body).contains("Cannot convert to RID: garbage");
  }

  @Test
  void aBareRidOrderByOnTheRowsOfAMatchIsRefusedWithAMessage() throws Exception {
    createFixtures();

    final Response response = command("sql", "MATCH {type: V12425, as: p} RETURN p.k AS k ORDER BY @rid");

    assertThat(response.status).as(response.body).isEqualTo(400);
    assertThat(response.body).contains("ORDER BY @rid has no meaning on the rows of this MATCH");
  }

  @Test
  void aValidEdgeBetweenTwoVerticesIsStillCreated() throws Exception {
    createFixtures();

    final Response response = command("sql", "CREATE EDGE Link12425 FROM " + vertexRid + " TO " + vertexRid);

    assertThat(response.status).as(response.body).isEqualTo(200);
    assertThat(edgeCount()).isEqualTo(1L);
  }

  private String vertexRid;

  /** Creates the types and one record of each kind; answers the RID of the document and keeps the RID of the vertex. */
  private String createFixtures() throws Exception {
    for (final String sql : new String[] { "CREATE VERTEX TYPE V12425", "CREATE PROPERTY V12425.k STRING",
        "CREATE EDGE TYPE Link12425", "CREATE DOCUMENT TYPE Doc12425" })
      assertThat(command("sql", sql).status).as(sql).isEqualTo(200);

    vertexRid = ridOf(command("sql", "INSERT INTO V12425 SET k = 'a'"));
    return ridOf(command("sql", "INSERT INTO Doc12425 SET k = 'd'"));
  }

  private static String ridOf(final Response response) {
    assertThat(response.status).as(response.body).isEqualTo(200);
    return new JSONObject(response.body).getJSONArray("result").getJSONObject(0).getString("@rid");
  }

  private long edgeCount() throws Exception {
    final Response response = command("sql", "SELECT count(@rid) AS n FROM Link12425");
    assertThat(response.status).as(response.body).isEqualTo(200);
    return new JSONObject(response.body).getJSONArray("result").getJSONObject(0).getLong("n");
  }

  private Response command(final String language, final String command) throws Exception {
    return command(language, command, null);
  }

  private Response command(final String language, final String command, final JSONObject params) throws Exception {
    final HttpURLConnection connection =
        (HttpURLConnection) new URI(getServerHttpUrl("/api/v1/command/" + getDatabaseName())).toURL().openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Content-Type", "application/json");
    final String credentials =
        Base64.getEncoder().encodeToString(("root:" + StaticBaseServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8));
    connection.setRequestProperty("Authorization", "Basic " + credentials);
    connection.setDoOutput(true);
    final JSONObject request = new JSONObject().put("language", language).put("command", command);
    if (params != null)
      request.put("params", params);
    final byte[] payload = request.toString().getBytes(StandardCharsets.UTF_8);
    try (final OutputStream out = connection.getOutputStream()) {
      out.write(payload);
    }
    try {
      final int status = connection.getResponseCode();
      final InputStream in = status >= 400 ? connection.getErrorStream() : connection.getInputStream();
      final String body = in == null ? "" : new String(in.readAllBytes(), StandardCharsets.UTF_8);
      return new Response(status, body);
    } finally {
      connection.disconnect();
    }
  }

  private record Response(int status, String body) {
  }
}
