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
import com.arcadedb.exception.SchemaException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.utility.FileUtils;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.math.BigDecimal;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code POST /api/v1/batch} must never answer 200 for a load that did not store what it was sent, and a refusal must
 * tell how much was committed before it.
 */
class PostBatchHandlerIntegrityTest extends BaseGraphServerTest {
  /*
   * Two ways {@code POST /api/v1/batch} answered 200 for a load that did not store what it was sent.
   * <ul>
   *   <li>issue #11801: two vertices declaring the same temporary {@code @id} were both created, the later one took the id over and
   *   the edges naming it attached to the wrong vertex. The second declaration is refused now, with the line numbers;</li>
   *   <li>issue #11802: a CSV field was converted by its shape before the declared type of its property was applied, so a
   *   {@code STRING} property stored {@code 7} for {@code 007}. The declared type now decides how the text is read, and an undeclared
   *   column reads only what JSON would spell as a number or a boolean.</li>
   * </ul>
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  // Regression: #11801 #11802 a repeated temporary id and a CSV field read against its declared type
  @Test
  void aRepeatedTemporaryIdIsRefusedAtItsSecondDeclaration() throws Exception {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.command("sql", "CREATE VERTEX TYPE Issue11801P");
    database.command("sql", "CREATE PROPERTY Issue11801P.id INTEGER");
    database.command("sql", "CREATE EDGE TYPE Issue11801E");

    final String payload = """
        {"@type":"vertex","@class":"Issue11801P","@id":"a","id":1}
        {"@type":"vertex","@class":"Issue11801P","@id":"a","id":2}
        {"@type":"vertex","@class":"Issue11801P","@id":"b","id":3}
        {"@type":"edge","@class":"Issue11801E","@from":"b","@to":"a"}
        """;
    final HttpURLConnection connection = postBatch(payload, "application/x-ndjson");
    try {
      assertThat(connection.getResponseCode()).isEqualTo(400);
      final JSONObject error = new JSONObject(readBody(connection.getErrorStream()));
      assertThat(error.getString("error")).contains("Vertex at line 2 declares @id 'a'").contains("the vertex at line 1");
      // the duplicate was in the first, not yet flushed, chunk: nothing of the payload was committed
      assertThat(error.getLong("verticesCreated")).isZero();
      assertThat(error.getLong("edgesCreated")).isZero();
    } finally {
      connection.disconnect();
    }
    assertThat(database.countType("Issue11801P", false)).isZero();
    assertThat(database.countType("Issue11801E", false)).isZero();
  }

  // Regression: #11801 #11802 a repeated temporary id and a CSV field read against its declared type
  @Test
  void aRepeatedTemporaryIdAcrossChunksNamesTheVertexAlreadyCreated() throws Exception {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.command("sql", "CREATE VERTEX TYPE Issue11801Q");

    // a chunk of one vertex: the first declaration is committed before the second is read
    final String payload = """
        {"@type":"vertex","@class":"Issue11801Q","@id":"a"}
        {"@type":"vertex","@class":"Issue11801Q","@id":"a"}
        """;
    final HttpURLConnection connection = postBatch(payload, "application/x-ndjson", "vertexBatchSize=1");
    try {
      assertThat(connection.getResponseCode()).isEqualTo(400);
      final JSONObject error = new JSONObject(readBody(connection.getErrorStream()));
      assertThat(error.getString("error")).contains("Vertex at line 2 declares @id 'a'").contains("created as #");
      assertThat(error.getLong("verticesCreated")).isEqualTo(1);
      assertThat(error.getBoolean("partialCommit")).isTrue();
    } finally {
      connection.disconnect();
    }
    assertThat(database.countType("Issue11801Q", false)).isEqualTo(1);
  }

  // Regression: #11801 #11802 a repeated temporary id and a CSV field read against its declared type
  @Test
  void distinctTemporaryIdsStillResolveAndAreAllMapped() throws Exception {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.command("sql", "CREATE VERTEX TYPE Issue11801R");
    database.command("sql", "CREATE EDGE TYPE Issue11801F");

    final String payload = """
        {"@type":"vertex","@class":"Issue11801R","@id":"a","n":1}
        {"@type":"vertex","@class":"Issue11801R","@id":"b","n":2}
        {"@type":"vertex","@class":"Issue11801R","n":3}
        {"@type":"edge","@class":"Issue11801F","@from":"b","@to":"a"}
        """;
    final HttpURLConnection connection = postBatch(payload, "application/x-ndjson");
    try {
      assertThat(connection.getResponseCode()).isEqualTo(200);
      final JSONObject result = new JSONObject(readBody(connection.getInputStream()));
      assertThat(result.getLong("verticesCreated")).isEqualTo(3);
      assertThat(result.getLong("edgesCreated")).isEqualTo(1);
      final JSONObject mapping = result.getJSONObject("idMapping");
      assertThat(mapping.keySet()).containsExactlyInAnyOrder("a", "b");
      for (final String id : mapping.keySet())
        assertThat(mapping.getString(id)).startsWith("#").doesNotStartWith("#-");
    } finally {
      connection.disconnect();
    }
    try (final ResultSet rs = database.query("sql", "SELECT out.n AS o, in.n AS i FROM Issue11801F")) {
      final Result edge = rs.next();
      assertThat(edge.<Integer>getProperty("o")).isEqualTo(2);
      assertThat(edge.<Integer>getProperty("i")).isEqualTo(1);
    }
  }

  // Regression: #11801 #11802 a repeated temporary id and a CSV field read against its declared type
  @Test
  void aCsvFieldKeepsItsTextInAStringPropertyAsJsonlDoes() throws Exception {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.command("sql", "CREATE VERTEX TYPE Issue11802Part");
    database.command("sql", "CREATE PROPERTY Issue11802Part.code STRING");
    database.command("sql", "CREATE PROPERTY Issue11802Part.src STRING");

    final List<String> values = List.of("007", "00123", "1.50", "0.10", "1e3", "+5", "-0", "5.", "000", "False", "TRUE", "abc",
        "12345678901234567890", "42", "true");
    final StringBuilder csv = new StringBuilder("@type,@class,@id,src,code\n");
    final StringBuilder jsonl = new StringBuilder();
    for (int i = 0; i < values.size(); i++) {
      csv.append("vertex,Issue11802Part,c").append(i).append(",csv,").append(values.get(i)).append('\n');
      jsonl.append(new JSONObject().put("@type", "vertex").put("@class", "Issue11802Part").put("@id", "j" + i).put("src", "jsonl")
          .put("code", values.get(i))).append('\n');
    }
    assertOk(postBatch(csv.toString(), "text/csv"));
    assertOk(postBatch(jsonl.toString(), "application/x-ndjson"));

    final Map<String, List<Object>> stored = new HashMap<>();
    try (final ResultSet rs = database.query("sql", "SELECT src, code FROM Issue11802Part")) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        stored.computeIfAbsent(row.getProperty("src"), k -> new ArrayList<>()).add(row.getProperty("code"));
      }
    }
    assertThat(stored.get("csv")).as("a STRING property stores the CSV text as sent").containsExactlyInAnyOrderElementsOf(values);
    assertThat(stored.get("jsonl")).containsExactlyInAnyOrderElementsOf(values);
  }

  // Regression: #11801 #11802 a repeated temporary id and a CSV field read against its declared type
  @Test
  void aCsvFieldIsReadByTheDeclaredTypeAndByItsJsonSpellingWhenUndeclared() throws Exception {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.command("sql", "CREATE VERTEX TYPE Issue11802Typed");
    database.command("sql", "CREATE PROPERTY Issue11802Typed.i INTEGER");
    database.command("sql", "CREATE PROPERTY Issue11802Typed.b BOOLEAN");
    database.command("sql", "CREATE PROPERTY Issue11802Typed.d DECIMAL");

    final String csv = """
        @type,@class,i,b,d,u1,u2,u3,u4,u5,u6
        vertex,Issue11802Typed,007,TRUE,1.50,007,42,1.5,true,False,1e3
        """;
    assertOk(postBatch(csv, "text/csv"));

    try (final ResultSet rs = database.query("sql", "SELECT FROM Issue11802Typed")) {
      final Result row = rs.next();
      // declared: the type reads the text, as leniently as before
      assertThat(row.<Integer>getProperty("i")).isEqualTo(7);
      assertThat(row.<Boolean>getProperty("b")).isTrue();
      assertThat(row.<BigDecimal>getProperty("d")).isEqualByComparingTo("1.50");
      assertThat(row.<BigDecimal>getProperty("d").scale()).as("the scale the text carried").isEqualTo(2);
      // undeclared: a JSON literal is read as one, anything else stays text
      assertThat(row.<Object>getProperty("u1")).isEqualTo("007");
      assertThat(row.<Object>getProperty("u2")).isEqualTo(42L);
      assertThat(row.<Object>getProperty("u3")).isEqualTo(1.5);
      assertThat(row.<Object>getProperty("u4")).isEqualTo(true);
      assertThat(row.<Object>getProperty("u5")).isEqualTo("False");
      assertThat(row.<Object>getProperty("u6")).isEqualTo(1000.0);
    }
  }

  private static void assertOk(final HttpURLConnection connection) throws Exception {
    try {
      final int status = connection.getResponseCode();
      assertThat(status).as(status >= 400 ? readBody(connection.getErrorStream()) : "").isEqualTo(200);
    } finally {
      connection.disconnect();
    }
  }

  private HttpURLConnection postBatch(final String body, final String contentType) throws Exception {
    return postBatch(body, contentType, null);
  }

  private HttpURLConnection postBatch(final String body, final String contentType, final String query) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(
        getServerHttpUrl(0, "/api/v1/batch/" + getDatabaseName() + (query != null ? "?" + query : ""))).toURL().openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    connection.setRequestProperty("Content-Type", contentType);
    connection.setDoOutput(true);
    final byte[] data = body.getBytes(StandardCharsets.UTF_8);
    connection.setRequestProperty("Content-Length", Integer.toString(data.length));
    try (final DataOutputStream out = new DataOutputStream(connection.getOutputStream())) {
      out.write(data);
    }
    connection.connect();
    return connection;
  }

  private static String readBody(final InputStream in) throws Exception {
    try (in) {
      return FileUtils.readStreamAsString(in, "UTF-8");
    }
  }

  /*
   * Issue #11633: an edge naming a type the schema does not have is refused by {@code GraphBatch} with a {@code SchemaException}
   * rather than an {@code IllegalArgumentException}. A batch load is not atomic, so the refusal must still carry how much of the
   * payload was committed before it, or a client re-posting the whole payload duplicates the committed vertices.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  // Regression: #11633 a missing edge type is refused and still reports what was committed
  @Test
  void aMissingEdgeTypeStillReportsWhatWasCommitted() throws Exception {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.command("sql", "CREATE VERTEX TYPE Issue11633V");

    final String payload = """
        {"@type":"vertex","@class":"Issue11633V","@id":"a"}
        {"@type":"vertex","@class":"Issue11633V","@id":"b"}
        {"@type":"edge","@class":"Issue11633NoSuchEdge","@from":"a","@to":"b"}
        """;
    // a chunk of one vertex: both vertices are committed before the edge is read
    final HttpURLConnection connection = postBatchWithQuery(payload, "vertexBatchSize=1");
    try {
      assertThat(connection.getResponseCode()).isEqualTo(400);
      final JSONObject error = new JSONObject(readBody(connection.getErrorStream()));
      assertThat(error.getString("error")).contains("Issue11633NoSuchEdge");
      assertThat(error.getString("exception")).isEqualTo(SchemaException.class.getName());
      assertThat(error.getLong("verticesCreated")).isEqualTo(2);
      assertThat(error.getLong("edgesCreated")).isZero();
      assertThat(error.getBoolean("partialCommit")).isTrue();
    } finally {
      connection.disconnect();
    }
    assertThat(database.countType("Issue11633V", false)).isEqualTo(2);
  }

  /*
   * Issue #12895: an edge endpoint written as a RID was taken on trust by GraphBatch. One that does not exist (404), or is not a
   * vertex (500, ClassCastException), failed at the close of the batch with the edge records written and no vertex leading to
   * them, valid edges before the bad one included. It is refused on its line now, like a malformed RID, and what came before
   * is flushed and reachable as for any other refusal at accept time.
   */
  // Regression: #12895 an edge endpoint RID that does not exist or is not a vertex
  @Test
  void anEdgeEndpointRidThatDoesNotExistIsRefusedAndTheRestIsReachable() throws Exception {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.command("sql", "CREATE VERTEX TYPE Issue12895V");
    database.command("sql", "CREATE EDGE TYPE Issue12895E");

    final String payload = """
        {"@type":"vertex","@class":"Issue12895V","@id":"a","n":1}
        {"@type":"vertex","@class":"Issue12895V","@id":"b","n":2}
        {"@type":"edge","@class":"Issue12895E","@from":"a","@to":"b"}
        {"@type":"edge","@class":"Issue12895E","@from":"a","@to":"#99:0"}
        """;
    assertEndpointRefusedAndFirstEdgeReachable(database, payload, "Issue12895V", "Issue12895E", "'#99:0' at line 4",
        "does not exist");
  }

  // Regression: #12895 an edge endpoint RID that does not exist or is not a vertex
  @Test
  void anEdgeEndpointRidOfADocumentIsRefusedAndTheRestIsReachable() throws Exception {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.command("sql", "CREATE VERTEX TYPE Issue12895W");
    database.command("sql", "CREATE EDGE TYPE Issue12895F");
    database.command("sql", "CREATE DOCUMENT TYPE Issue12895D");
    final String documentRid;
    try (final ResultSet rs = database.command("sql", "INSERT INTO Issue12895D SET x = 1")) {
      documentRid = rs.next().getIdentity().get().toString();
    }

    final String payload = """
        {"@type":"vertex","@class":"Issue12895W","@id":"a","n":1}
        {"@type":"vertex","@class":"Issue12895W","@id":"b","n":2}
        {"@type":"edge","@class":"Issue12895F","@from":"a","@to":"b"}
        {"@type":"edge","@class":"Issue12895F","@from":"%s","@to":"a"}
        """.formatted(documentRid);
    assertEndpointRefusedAndFirstEdgeReachable(database, payload, "Issue12895W", "Issue12895F", "'" + documentRid + "' at line 4",
        "is not a vertex");
  }

  // Regression: #12895 an edge endpoint RID that does not exist or is not a vertex
  @Test
  void anEdgeEndpointRidOfAnExistingVertexStillLoads() throws Exception {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.command("sql", "CREATE VERTEX TYPE Issue12895X");
    database.command("sql", "CREATE EDGE TYPE Issue12895G");
    final String existing;
    try (final ResultSet rs = database.command("sql", "CREATE VERTEX Issue12895X SET n = 0")) {
      existing = rs.next().getIdentity().get().toString();
    }

    final String payload = """
        {"@type":"vertex","@class":"Issue12895X","@id":"a","n":1}
        {"@type":"edge","@class":"Issue12895G","@from":"a","@to":"%s"}
        {"@type":"edge","@class":"Issue12895G","@from":"%s","@to":"a"}
        """.formatted(existing, existing);
    final HttpURLConnection connection = postBatch(payload, "application/x-ndjson");
    try {
      assertThat(connection.getResponseCode()).isEqualTo(200);
      assertThat(new JSONObject(readBody(connection.getInputStream())).getLong("edgesCreated")).isEqualTo(2);
    } finally {
      connection.disconnect();
    }
    try (final ResultSet rs = database.query("sql", "SELECT out('Issue12895G').size() AS o, in('Issue12895G').size() AS i FROM Issue12895X WHERE n = 1")) {
      final Result vertex = rs.next();
      assertThat(vertex.<Integer>getProperty("o")).isEqualTo(1);
      assertThat(vertex.<Integer>getProperty("i")).isEqualTo(1);
    }
  }

  private void assertEndpointRefusedAndFirstEdgeReachable(final Database database, final String payload, final String vertexType,
      final String edgeType, final String endpointAtLine, final String reason) throws Exception {
    final HttpURLConnection connection = postBatch(payload, "application/x-ndjson");
    try {
      assertThat(connection.getResponseCode()).isEqualTo(400);
      final JSONObject error = new JSONObject(readBody(connection.getErrorStream()));
      assertThat(error.getString("error")).contains(endpointAtLine).contains(reason);
      assertThat(error.getLong("verticesCreated")).isEqualTo(2);
      assertThat(error.getLong("edgesCreated")).isEqualTo(1);
      assertThat(error.getBoolean("partialCommit")).isTrue();
    } finally {
      connection.disconnect();
    }
    // the edge before the refused line is a whole edge: its record exists and the vertex leads to it
    assertThat(database.countType(edgeType, false)).isEqualTo(1);
    try (final ResultSet rs = database.query("sql", "SELECT out('" + edgeType + "').size() AS o FROM " + vertexType + " WHERE n = 1")) {
      assertThat(rs.next().<Integer>getProperty("o")).isEqualTo(1);
    }
  }

  private HttpURLConnection postBatchWithQuery(final String body, final String query) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(
        getServerHttpUrl(0, "/api/v1/batch/" + getDatabaseName() + "?" + query)).toURL().openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    connection.setRequestProperty("Content-Type", "application/x-ndjson");
    connection.setDoOutput(true);
    final byte[] data = body.getBytes(StandardCharsets.UTF_8);
    connection.setRequestProperty("Content-Length", Integer.toString(data.length));
    try (final DataOutputStream out = new DataOutputStream(connection.getOutputStream())) {
      out.write(data);
    }
    connection.connect();
    return connection;
  }
}
