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
package com.arcadedb.server;

import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.DataOutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

class PostBatchHandlerIT extends BaseGraphServerTest {

  @Test
  void jsonlVerticesAndEdges() throws Exception {
    testEachServer(serverIndex -> {
      final String body = """
          {"@type":"vertex","@class":"V1","@id":"a","id":100}
          {"@type":"vertex","@class":"V1","@id":"b","id":101}
          {"@type":"edge","@class":"E1","@from":"a","@to":"b"}
          """;

      final JSONObject result = postBatch(serverIndex, body, "application/x-ndjson", "");
      assertThat(result.getInt("verticesCreated")).isEqualTo(2);
      assertThat(result.getInt("edgesCreated")).isEqualTo(1);
      assertThat(result.has("idMapping")).isTrue();
      assertThat(result.getLong("elapsedMs")).isGreaterThanOrEqualTo(0);

      // Verify the graph was created correctly
      final JSONObject query = executeCommand(serverIndex, "sql", "SELECT FROM V1 WHERE id >= 100 ORDER BY id ASC");
      assertThat(query.getJSONObject("result").getJSONArray("records").length()).isEqualTo(2);

      // Verify edge exists
      final JSONObject edgeQuery = executeCommand(serverIndex, "sql", "SELECT FROM E1");
      assertThat(edgeQuery.getJSONObject("result").getJSONArray("records").length()).isGreaterThanOrEqualTo(1);
    });
  }

  @Test
  void csvVerticesAndEdges() throws Exception {
    testEachServer(serverIndex -> {
      final String body = """
          @type,@class,@id,id
          vertex,V1,c1,200
          vertex,V1,c2,201
          ---
          @type,@class,@from,@to
          edge,E1,c1,c2
          """;

      final JSONObject result = postBatch(serverIndex, body, "text/csv", "");
      assertThat(result.getInt("verticesCreated")).isEqualTo(2);
      assertThat(result.getInt("edgesCreated")).isEqualTo(1);

      // Verify
      final JSONObject query = executeCommand(serverIndex, "sql", "SELECT FROM V1 WHERE id >= 200 ORDER BY id ASC");
      assertThat(query.getJSONObject("result").getJSONArray("records").length()).isEqualTo(2);
    });
  }

  @Test
  void vertexOnlyImport() throws Exception {
    testEachServer(serverIndex -> {
      final String body = """
          {"@type":"vertex","@class":"V1","id":300}
          {"@type":"vertex","@class":"V1","id":301}
          {"@type":"vertex","@class":"V1","id":302}
          """;

      final JSONObject result = postBatch(serverIndex, body, "application/x-ndjson", "");
      assertThat(result.getInt("verticesCreated")).isEqualTo(3);
      assertThat(result.getInt("edgesCreated")).isEqualTo(0);
    });
  }

  @Test
  void edgeWithExistingRid() throws Exception {
    testEachServer(serverIndex -> {
      // First create some vertices and get their RIDs
      final String createBody = """
          {"@type":"vertex","@class":"V1","@id":"x1","id":400}
          {"@type":"vertex","@class":"V1","@id":"x2","id":401}
          """;

      final JSONObject createResult = postBatch(serverIndex, createBody, "application/x-ndjson", "");
      final JSONObject mapping = createResult.getJSONObject("idMapping");
      final String rid1 = mapping.getString("x1");
      final String rid2 = mapping.getString("x2");

      // Now create edges using RIDs directly
      final String edgeBody = "{\"@type\":\"edge\",\"@class\":\"E1\",\"@from\":\"" + rid1 + "\",\"@to\":\"" + rid2 + "\"}\n";

      final JSONObject edgeResult = postBatch(serverIndex, edgeBody, "application/x-ndjson", "");
      assertThat(edgeResult.getInt("edgesCreated")).isEqualTo(1);
    });
  }

  /**
   * Issue #5470: on a replicated database every vertex batch becomes one Raft entry, so a load of large
   * records must be able to shrink it. The temporary ids of vertices flushed in an earlier batch must still
   * resolve for the edges that follow.
   */
  @Test
  void customVertexBatchSize() throws Exception {
    testEachServer(serverIndex -> {
      final StringBuilder body = new StringBuilder();
      for (int i = 0; i < 7; i++)
        body.append("{\"@type\":\"vertex\",\"@class\":\"V1\",\"@id\":\"vbs").append(i).append("\",\"id\":")
            .append(500 + i).append("}\n");
      body.append("{\"@type\":\"edge\",\"@class\":\"E1\",\"@from\":\"vbs0\",\"@to\":\"vbs6\"}\n");

      final JSONObject result = postBatch(serverIndex, body.toString(), "application/x-ndjson", "vertexBatchSize=2");
      assertThat(result.getInt("verticesCreated")).isEqualTo(7);
      assertThat(result.getInt("edgesCreated")).isEqualTo(1);

      final JSONObject query = executeCommand(serverIndex, "sql",
          "SELECT FROM V1 WHERE id >= 500 AND id <= 506");
      assertThat(query.getJSONObject("result").getJSONArray("records").length()).isEqualTo(7);
    });
  }

  @Test
  void invalidVertexBatchSizeReturnsError() throws Exception {
    testEachServer(serverIndex -> {
      final String body = """
          {"@type":"vertex","@class":"V1","id":600}
          """;

      final HttpURLConnection conn = openBatchConnection(serverIndex, "application/x-ndjson", "vertexBatchSize=0");
      writeBody(conn, body);
      conn.connect();

      assertThat(conn.getResponseCode()).isEqualTo(400);
      conn.disconnect();
    });
  }

  @Test
  void unknownTempIdReturnsError() throws Exception {
    testEachServer(serverIndex -> {
      final String body = """
          {"@type":"edge","@class":"E1","@from":"nonexistent","@to":"also_nonexistent"}
          """;

      final HttpURLConnection conn = openBatchConnection(serverIndex, "application/x-ndjson", "");
      writeBody(conn, body);
      conn.connect();

      // Should return an error (IllegalArgumentException -> 400)
      assertThat(conn.getResponseCode()).isEqualTo(400);
      conn.disconnect();
    });
  }

  /**
   * Regression for issue #5036: when a batch fails mid-stream (here an edge that references an unknown
   * temporary id after its vertices were already flushed), the error response must report how many
   * records were persisted so far and flag the partial commit, so a client can reconcile instead of
   * blindly retrying the whole file (which would duplicate the already-committed vertices).
   */
  @Test
  void partialCommitErrorReportsPersistedCounts() throws Exception {
    testEachServer(serverIndex -> {
      final String body = """
          {"@type":"vertex","@class":"V1","@id":"pcA","id":900}
          {"@type":"vertex","@class":"V1","@id":"pcB","id":901}
          {"@type":"edge","@class":"E1","@from":"pcA","@to":"ghostNode"}
          """;

      final HttpURLConnection conn = openBatchConnection(serverIndex, "application/x-ndjson", "commitEvery=1");
      writeBody(conn, body);
      conn.connect();

      assertThat(conn.getResponseCode()).isEqualTo(400);

      final JSONObject error = new JSONObject(readError(conn));
      assertThat(error.getInt("verticesCreated")).isEqualTo(2);
      assertThat(error.getInt("edgesCreated")).isEqualTo(0);
      assertThat(error.getBoolean("partialCommit")).isTrue();
      assertThat(error.getString("error")).contains("ghostNode");
      conn.disconnect();
    });
  }

  /**
   * Regression for issue #5036: when the very first record fails (nothing committed yet) the error
   * body must report {@code partialCommit=false} with zero counts, so a client knows no reconciliation
   * is needed and a full retry is safe.
   */
  @Test
  void partialCommitFalseWhenFirstRecordFails() throws Exception {
    testEachServer(serverIndex -> {
      final String body = """
          {"@type":"edge","@class":"E1","@from":"noVertexA","@to":"noVertexB"}
          """;

      final HttpURLConnection conn = openBatchConnection(serverIndex, "application/x-ndjson", "");
      writeBody(conn, body);
      conn.connect();

      assertThat(conn.getResponseCode()).isEqualTo(400);

      final JSONObject error = new JSONObject(readError(conn));
      assertThat(error.getInt("verticesCreated")).isEqualTo(0);
      assertThat(error.getInt("edgesCreated")).isEqualTo(0);
      assertThat(error.getBoolean("partialCommit")).isFalse();
      conn.disconnect();
    });
  }

  /**
   * Regression for issue #5036: the partial-commit contract must hold for the CSV
   * {@link com.arcadedb.server.http.handler.batch.CsvBatchRecordStream} path too, not just JSONL -
   * the counters live in {@code execute}, independent of the stream format.
   */
  @Test
  void partialCommitErrorReportsPersistedCountsCsv() throws Exception {
    testEachServer(serverIndex -> {
      final String body = """
          @type,@class,@id,id
          vertex,V1,pccA,910
          vertex,V1,pccB,911
          ---
          @type,@class,@from,@to
          edge,E1,pccA,ghostCsvNode
          """;

      final HttpURLConnection conn = openBatchConnection(serverIndex, "text/csv", "commitEvery=1");
      writeBody(conn, body);
      conn.connect();

      assertThat(conn.getResponseCode()).isEqualTo(400);

      final JSONObject error = new JSONObject(readError(conn));
      assertThat(error.getInt("verticesCreated")).isEqualTo(2);
      assertThat(error.getInt("edgesCreated")).isEqualTo(0);
      assertThat(error.getBoolean("partialCommit")).isTrue();
      assertThat(error.getString("error")).contains("ghostCsvNode");
      conn.disconnect();
    });
  }

  /**
   * Regression for discussion #4040: posting a JSONL line that omits the {@code @type} meta key
   * must return a clear HTTP 400 error, not bubble up a raw {@code JSONObject[@type] not found}
   * JSONException as HTTP 500.
   */
  @Test
  void missingTypeReturnsHttp400() throws Exception {
    testEachServer(serverIndex -> {
      final String body = "{\"@class\":\"V1\",\"id\":700}\n";

      final HttpURLConnection conn = openBatchConnection(serverIndex, "application/x-ndjson", "");
      writeBody(conn, body);
      conn.connect();

      assertThat(conn.getResponseCode()).isEqualTo(400);

      final String error = readError(conn);
      assertThat(error).contains("@type");
      assertThat(error).contains("line 1");
      conn.disconnect();
    });
  }

  /**
   * Regression for discussion #4040: posting a JSONL line that omits the {@code @class} meta key
   * must return a clear HTTP 400 error.
   */
  @Test
  void missingClassReturnsHttp400() throws Exception {
    testEachServer(serverIndex -> {
      final String body = "{\"@type\":\"vertex\",\"id\":701}\n";

      final HttpURLConnection conn = openBatchConnection(serverIndex, "application/x-ndjson", "");
      writeBody(conn, body);
      conn.connect();

      assertThat(conn.getResponseCode()).isEqualTo(400);

      final String error = readError(conn);
      assertThat(error).contains("@class");
      conn.disconnect();
    });
  }

  /**
   * Regression for discussion #4040: posting a body that is not valid JSON must return a clear
   * HTTP 400 error.
   */
  @Test
  void malformedJsonReturnsHttp400() throws Exception {
    testEachServer(serverIndex -> {
      final String body = "this is not json\n";

      final HttpURLConnection conn = openBatchConnection(serverIndex, "application/x-ndjson", "");
      writeBody(conn, body);
      conn.connect();

      assertThat(conn.getResponseCode()).isEqualTo(400);
      conn.disconnect();
    });
  }

  /**
   * Regression for discussion #4040: a user mistakenly sending a JSON array (the format expected
   * by INSERT INTO ... CONTENT [...]) instead of JSONL must get a clear HTTP 400 with guidance,
   * not HTTP 500.
   */
  @Test
  void jsonArrayBodyReturnsHttp400WithGuidance() throws Exception {
    testEachServer(serverIndex -> {
      final String body = "[{\"@type\":\"vertex\",\"@class\":\"V1\",\"id\":702}]\n";

      final HttpURLConnection conn = openBatchConnection(serverIndex, "application/x-ndjson", "");
      writeBody(conn, body);
      conn.connect();

      assertThat(conn.getResponseCode()).isEqualTo(400);

      final String error = readError(conn);
      assertThat(error.toLowerCase()).contains("jsonl");
      conn.disconnect();
    });
  }

  @Test
  void lightEdgesParameter() throws Exception {
    testEachServer(serverIndex -> {
      final String body = """
          {"@type":"vertex","@class":"V1","@id":"le1","id":500}
          {"@type":"vertex","@class":"V1","@id":"le2","id":501}
          {"@type":"edge","@class":"E1","@from":"le1","@to":"le2"}
          """;

      final JSONObject result = postBatch(serverIndex, body, "application/x-ndjson", "lightEdges=true");
      assertThat(result.getInt("verticesCreated")).isEqualTo(2);
      assertThat(result.getInt("edgesCreated")).isEqualTo(1);
    });
  }

  /**
   * Regression for issue #4069: posting a JSONL vertex whose property is declared as
   * {@code LIST OF STRING} in the schema must accept the JSON array (the JSONL parser
   * returned a {@link com.arcadedb.serializer.json.JSONArray}, which is not a
   * {@link java.util.Collection}, so {@code Type.convert} wrapped it in a singleton list,
   * yielding a nested array and a "value of type 'null'" validation error).
   */
  @Test
  void listOfStringPropertyFromJsonl() throws Exception {
    testEachServer(serverIndex -> {
      executeCommand(serverIndex, "sql", "CREATE VERTEX TYPE EntityA IF NOT EXISTS");
      executeCommand(serverIndex, "sql", "CREATE PROPERTY EntityA.names IF NOT EXISTS LIST OF STRING");

      final String body = """
          {"@type":"vertex","@class":"EntityA","@id":"a4069","names":["ANONYMOUS WARD V/FOO BAR","SECOND"]}
          """;

      final JSONObject result = postBatch(serverIndex, body, "application/x-ndjson", "");
      assertThat(result.getInt("verticesCreated")).isEqualTo(1);

      final JSONObject query = executeCommand(serverIndex, "sql",
          "SELECT names FROM EntityA");
      final JSONObject record = query.getJSONObject("result").getJSONArray("records").getJSONObject(0);
      assertThat(record.getJSONArray("names").length()).isEqualTo(2);
      assertThat(record.getJSONArray("names").getString(0)).isEqualTo("ANONYMOUS WARD V/FOO BAR");
      assertThat(record.getJSONArray("names").getString(1)).isEqualTo("SECOND");
    });
  }

  @Test
  void edgeWithProperties() throws Exception {
    testEachServer(serverIndex -> {
      final String body = """
          {"@type":"vertex","@class":"V1","@id":"ep1","id":600}
          {"@type":"vertex","@class":"V1","@id":"ep2","id":601}
          {"@type":"edge","@class":"E1","@from":"ep1","@to":"ep2","weight":0.75,"label":"friend"}
          """;

      final JSONObject result = postBatch(serverIndex, body, "application/x-ndjson", "");
      assertThat(result.getInt("edgesCreated")).isEqualTo(1);
    });
  }

  /**
   * Regression for issue #4142: the GraphBatch edge serializer landed strings on
   * {@link com.arcadedb.utility.DateUtils#dateTimeToTimestamp(Object, java.time.temporal.ChronoUnit)}
   * which only accepted {@link java.time.LocalDateTime#parse(CharSequence)} formats. Vertices
   * tolerated ISO-8601 strings with a {@code Z} or {@code ±HH:mm} zone designator (the default
   * {@code Date.toJSON()} output in JS/Python/Go/Java); edges threw HTTP 500. Both paths must
   * accept the same set of formats.
   */
  @Test
  void edgeIsoDateTimeWithZSuffix() throws Exception {
    testEachServer(serverIndex -> {
      executeCommand(serverIndex, "sql", "CREATE VERTEX TYPE V4142 IF NOT EXISTS");
      executeCommand(serverIndex, "sql", "CREATE EDGE TYPE E4142 IF NOT EXISTS");
      executeCommand(serverIndex, "sql", "CREATE PROPERTY V4142.ts IF NOT EXISTS DATETIME");
      executeCommand(serverIndex, "sql", "CREATE PROPERTY E4142.ts IF NOT EXISTS DATETIME");

      // Vertex first: was already accepted before the fix - asserts no regression on that path.
      final String vertexBody = "{\"@type\":\"vertex\",\"@class\":\"V4142\",\"@id\":\"v1\",\"ts\":\"2026-05-08T18:37:54Z\"}\n";
      final JSONObject vResult = postBatch(serverIndex, vertexBody, "application/x-ndjson", "");
      assertThat(vResult.getInt("verticesCreated")).isEqualTo(1);

      // Edge with Z suffix - the broken case before the fix.
      final String edgeBody = """
          {"@type":"vertex","@class":"V4142","@id":"a4142"}
          {"@type":"vertex","@class":"V4142","@id":"b4142"}
          {"@type":"edge","@class":"E4142","@from":"a4142","@to":"b4142","ts":"2026-05-08T18:37:54Z"}
          """;
      final JSONObject eResult = postBatch(serverIndex, edgeBody, "application/x-ndjson", "");
      assertThat(eResult.getInt("verticesCreated")).isEqualTo(2);
      assertThat(eResult.getInt("edgesCreated")).isEqualTo(1);

      // Confirm the timestamp survived the round-trip: the rebased wall-clock matches what
      // {@code parseIsoDateTime} produces given the database's configured timezone. The
      // computation mirrors the engine path so the assertion stays stable regardless of the
      // JVM's default zone.
      final JSONObject query = executeCommand(serverIndex, "sql", "SELECT ts FROM E4142");
      final String ts = query.getJSONObject("result").getJSONArray("records").getJSONObject(0).getString("ts");
      final ZonedDateTime input = ZonedDateTime.parse("2026-05-08T18:37:54Z");
      final String expectedDate = input.withZoneSameInstant(ZoneId.systemDefault())
          .toLocalDateTime()
          .toLocalDate()
          .toString();
      assertThat(ts).startsWith(expectedDate);
    });
  }

  /**
   * Regression for issue #4142: the milliseconds + {@code Z} suffix variant
   * ({@code 2026-05-08T18:37:54.000Z}) is the most common shape produced by
   * {@code new Date().toISOString()} in JavaScript and friends.
   */
  @Test
  void edgeIsoDateTimeWithMillisAndZSuffix() throws Exception {
    testEachServer(serverIndex -> {
      executeCommand(serverIndex, "sql", "CREATE VERTEX TYPE V4142b IF NOT EXISTS");
      executeCommand(serverIndex, "sql", "CREATE EDGE TYPE E4142b IF NOT EXISTS");
      executeCommand(serverIndex, "sql", "CREATE PROPERTY E4142b.ts IF NOT EXISTS DATETIME");

      final String body = """
          {"@type":"vertex","@class":"V4142b","@id":"a4142b"}
          {"@type":"vertex","@class":"V4142b","@id":"b4142b"}
          {"@type":"edge","@class":"E4142b","@from":"a4142b","@to":"b4142b","ts":"2026-05-08T18:37:54.000Z"}
          """;
      final JSONObject result = postBatch(serverIndex, body, "application/x-ndjson", "");
      assertThat(result.getInt("edgesCreated")).isEqualTo(1);
    });
  }

  /**
   * Regression for issue #4142: ISO-8601 with explicit numeric offset
   * ({@code 2026-05-08T18:37:54+00:00}) is what {@code java.time.OffsetDateTime.toString()}
   * produces.
   */
  @Test
  void edgeIsoDateTimeWithExplicitOffset() throws Exception {
    testEachServer(serverIndex -> {
      executeCommand(serverIndex, "sql", "CREATE VERTEX TYPE V4142c IF NOT EXISTS");
      executeCommand(serverIndex, "sql", "CREATE EDGE TYPE E4142c IF NOT EXISTS");
      executeCommand(serverIndex, "sql", "CREATE PROPERTY E4142c.ts IF NOT EXISTS DATETIME");

      final String body = """
          {"@type":"vertex","@class":"V4142c","@id":"a4142c"}
          {"@type":"vertex","@class":"V4142c","@id":"b4142c"}
          {"@type":"edge","@class":"E4142c","@from":"a4142c","@to":"b4142c","ts":"2026-05-08T18:37:54+00:00"}
          """;
      final JSONObject result = postBatch(serverIndex, body, "application/x-ndjson", "");
      assertThat(result.getInt("edgesCreated")).isEqualTo(1);
    });
  }

  /**
   * Issue #5470: the response used to echo the whole temporary-id mapping whatever its size, so a bulk load of
   * millions of vertices built a second copy of the map as one JSON string and died of an OutOfMemoryError at the
   * very last step of an otherwise successful import. Past 10,000 ids the mapping is replaced by its size, and a
   * client that needs it anyway (RemoteGraphBatch resolving edges across requests) asks for it explicitly.
   */
  @Test
  void aHugeIdMappingIsOmittedUnlessTheClientAsksForIt() throws Exception {
    final int vertices = 10_001;

    final JSONObject omitted = postBatch(0, hugeBody(vertices, 700_000), "application/x-ndjson", "");
    assertThat(omitted.getInt("verticesCreated")).isEqualTo(vertices);
    assertThat(omitted.has("idMapping")).isFalse();
    assertThat(omitted.getBoolean("idMappingOmitted")).isTrue();
    assertThat(omitted.getInt("idMappingSize")).isEqualTo(vertices);

    // Fresh ids: V1.id is unique, so the second load must not repeat the first one's values.
    final JSONObject requested = postBatch(0, hugeBody(vertices, 800_000), "application/x-ndjson", "idMapping=true");
    assertThat(requested.getJSONObject("idMapping").keySet()).hasSize(vertices);
  }

  /**
   * Issue #5470: with {@code refMode=ordinal} the edges name the vertices by their position in the payload, so the
   * server keeps two primitive arrays (12 bytes per vertex) instead of a map of ids, and resolving an edge is an
   * array read. The position may be left implicit or spelled out, as a number or in the {@code v<n>} form.
   */
  @Test
  void ordinalModeResolvesEdgesByPositionInThePayload() throws Exception {
    testEachServer(serverIndex -> {
      final String body = """
          {"@type":"vertex","@class":"V1","id":810000}
          {"@type":"vertex","@class":"V1","@id":1,"id":810001}
          {"@type":"vertex","@class":"V1","@id":"v2","id":810002}
          {"@type":"edge","@class":"E1","@from":0,"@to":2}
          {"@type":"edge","@class":"E1","@from":"v1","@to":"0"}
          """;

      final JSONObject result = postBatch(serverIndex, body, "application/x-ndjson", "refMode=ordinal");
      assertThat(result.getInt("verticesCreated")).isEqualTo(3);
      assertThat(result.getInt("edgesCreated")).isEqualTo(2);
      assertThat(result.getJSONObject("idMapping").keySet()).containsExactlyInAnyOrder("0", "1", "2");

      final JSONObject query = executeCommand(serverIndex, "sql",
          "SELECT out('E1').id as target FROM V1 WHERE id = 810000");
      assertThat(query.getJSONObject("result").getJSONArray("records").getJSONObject(0).getJSONArray("target")
          .getInt(0)).isEqualTo(810002);
    });
  }

  /**
   * Edges must still be able to point at vertices already in the database, and a load that spans several vertex
   * batches must keep resolving positions across them.
   */
  @Test
  void ordinalModeAcrossSeveralBatchesAndAgainstExistingRids() throws Exception {
    testEachServer(serverIndex -> {
      final StringBuilder body = new StringBuilder();
      for (int i = 0; i < 7; i++)
        body.append("{\"@type\":\"vertex\",\"@class\":\"V1\",\"id\":").append(820_000 + i).append("}\n");
      body.append("{\"@type\":\"edge\",\"@class\":\"E1\",\"@from\":0,\"@to\":6}\n");

      final JSONObject result = postBatch(serverIndex, body.toString(), "application/x-ndjson",
          "refMode=ordinal&vertexBatchSize=2");
      assertThat(result.getInt("verticesCreated")).isEqualTo(7);
      assertThat(result.getInt("edgesCreated")).isEqualTo(1);

      final String firstRid = result.getJSONObject("idMapping").getString("0");
      final String body2 = "{\"@type\":\"vertex\",\"@class\":\"V1\",\"id\":820100}\n"
          + "{\"@type\":\"edge\",\"@class\":\"E1\",\"@from\":0,\"@to\":\"" + firstRid + "\"}\n";

      final JSONObject result2 = postBatch(serverIndex, body2, "application/x-ndjson", "refMode=ordinal");
      assertThat(result2.getInt("edgesCreated")).isEqualTo(1);

      final JSONObject query = executeCommand(serverIndex, "sql",
          "SELECT out('E1').id as target FROM V1 WHERE id = 820100");
      assertThat(query.getJSONObject("result").getJSONArray("records").getJSONObject(0).getJSONArray("target")
          .getInt(0)).isEqualTo(820000);
    });
  }

  /**
   * A load split across requests keeps one counter, so the second payload starts where the first stopped:
   * {@code ordinalBase} tells the server where its numbering begins. This is what lets RemoteGraphBatch, whose
   * counter spans every flush, use ordinal mode.
   */
  @Test
  void ordinalBaseContinuesTheNumberingOfAnEarlierRequest() throws Exception {
    testEachServer(serverIndex -> {
      final String first = """
          {"@type":"vertex","@class":"V1","id":840000}
          {"@type":"vertex","@class":"V1","id":840001}
          """;
      final JSONObject result = postBatch(serverIndex, first, "application/x-ndjson", "refMode=ordinal");
      assertThat(result.getJSONObject("idMapping").keySet()).containsExactlyInAnyOrder("0", "1");

      // Second request: its own vertices are numbered from 2, and the edge points back at vertex 0 by RID.
      final String firstRid = result.getJSONObject("idMapping").getString("0");
      final String second = "{\"@type\":\"vertex\",\"@class\":\"V1\",\"@id\":2,\"id\":840002}\n"
          + "{\"@type\":\"edge\",\"@class\":\"E1\",\"@from\":2,\"@to\":\"" + firstRid + "\"}\n";

      final JSONObject result2 = postBatch(serverIndex, second, "application/x-ndjson",
          "refMode=ordinal&ordinalBase=2");
      assertThat(result2.getInt("verticesCreated")).isEqualTo(1);
      assertThat(result2.getInt("edgesCreated")).isEqualTo(1);
      assertThat(result2.getJSONObject("idMapping").keySet()).containsExactly("2");

      final JSONObject query = executeCommand(serverIndex, "sql",
          "SELECT out('E1').id as target FROM V1 WHERE id = 840002");
      assertThat(query.getJSONObject("result").getJSONArray("records").getJSONObject(0).getJSONArray("target")
          .getInt(0)).isEqualTo(840000);

      // A position that belongs to the earlier request cannot be resolved here: it must come as a RID.
      final String dangling = "{\"@type\":\"vertex\",\"@class\":\"V1\",\"@id\":3,\"id\":840003}\n"
          + "{\"@type\":\"edge\",\"@class\":\"E1\",\"@from\":3,\"@to\":0}\n";
      assertThat(postBatchError(serverIndex, dangling, "refMode=ordinal&ordinalBase=3").getString("error"))
          .contains("earlier request");
    });
  }

  @Test
  void ordinalModeRejectsWhatItCannotResolve() throws Exception {
    testEachServer(serverIndex -> {
      // An @id that is not the position of the vertex: fails on that line, not later on an edge.
      final String misnumbered = """
          {"@type":"vertex","@class":"V1","@id":"__natural/key","id":830000}
          """;
      assertThat(postBatchError(serverIndex, misnumbered, "refMode=ordinal").getString("error"))
          .contains("at line 1").contains("refMode=ordinal");

      final String danglingRef = """
          {"@type":"vertex","@class":"V1","id":830001}
          {"@type":"edge","@class":"E1","@from":0,"@to":9}
          """;
      assertThat(postBatchError(serverIndex, danglingRef, "refMode=ordinal").getString("error"))
          .contains("only 1 vertices were loaded");

      // An unknown refMode is rejected like every other bad query parameter: 400, message sanitized by the base
      // handler (same contract as invalidVertexBatchSizeReturnsError).
      final String badMode = """
          {"@type":"vertex","@class":"V1","id":830002}
          """;
      final HttpURLConnection conn = openBatchConnection(serverIndex, "application/x-ndjson", "refMode=whatever");
      writeBody(conn, badMode);
      conn.connect();
      assertThat(conn.getResponseCode()).isEqualTo(400);
      conn.disconnect();
    });
  }

  /**
   * Issue #5470: a chunked upload announces no length, so the only thing that can prove it arrived whole is the
   * client saying how much it was going to send. Without it, a body that ends early because the producer feeding
   * the stream stopped - not because the connection broke - looks exactly like a complete one and is answered 200
   * with a partial count.
   */
  @Test
  void expectedRecordsTurnsAShortPayloadIntoAFailure() throws Exception {
    testEachServer(serverIndex -> {
      final String body = """
          {"@type":"vertex","@class":"V1","@id":"er1","id":850000}
          {"@type":"vertex","@class":"V1","@id":"er2","id":850001}
          {"@type":"edge","@class":"E1","@from":"er1","@to":"er2"}
          """;

      // The count matches: nothing changes.
      final JSONObject ok = postBatch(serverIndex, body, "application/x-ndjson", "expectedRecords=3");
      assertThat(ok.getInt("verticesCreated")).isEqualTo(2);
      assertThat(ok.getInt("edgesCreated")).isEqualTo(1);

      // Fewer records than declared: the payload ended early, so it is answered like any other truncation - 408
      // with the counts, which is what a client needs to resume.
      final String shortBody = """
          {"@type":"vertex","@class":"V1","id":850010}
          {"@type":"vertex","@class":"V1","id":850011}
          """;
      final HttpURLConnection conn = openBatchConnection(serverIndex, "application/x-ndjson", "expectedRecords=1000");
      writeBody(conn, shortBody);
      conn.connect();
      assertThat(conn.getResponseCode()).isEqualTo(408);

      final JSONObject truncated = new JSONObject(readError(conn));
      conn.disconnect();
      assertThat(truncated.getString("error")).contains("Expected 1000 records but 2 were received");
      assertThat(truncated.getLong("verticesCreated")).isEqualTo(2);
      assertThat(truncated.getBoolean("partialCommit")).isTrue();

      // More records than declared: the request and its declaration disagree, and repeating it blindly would make
      // things worse. V1.id is unique, so every post below carries its own ids.
      final String excess = """
          {"@type":"vertex","@class":"V1","@id":"er3","id":850020}
          {"@type":"vertex","@class":"V1","@id":"er4","id":850021}
          {"@type":"edge","@class":"E1","@from":"er3","@to":"er4"}
          """;
      assertThat(postBatchError(serverIndex, excess, "expectedRecords=2").getString("error"))
          .contains("Expected 2 records but 3 were received");

      final String negative = """
          {"@type":"vertex","@class":"V1","id":850030}
          """;
      assertThat(postBatchError(serverIndex, negative, "expectedRecords=-1").getString("error")).isNotEmpty();
    });
  }

  private JSONObject postBatchError(final int serverIndex, final String body, final String queryParams)
      throws Exception {
    final HttpURLConnection conn = openBatchConnection(serverIndex, "application/x-ndjson", queryParams);
    writeBody(conn, body);
    conn.connect();

    try {
      assertThat(conn.getResponseCode()).isEqualTo(400);
      return new JSONObject(readError(conn));
    } finally {
      conn.disconnect();
    }
  }

  private String hugeBody(final int vertices, final int firstId) {
    final StringBuilder body = new StringBuilder(vertices * 64);
    for (int i = 0; i < vertices; i++)
      body.append("{\"@type\":\"vertex\",\"@class\":\"V1\",\"@id\":\"big").append(firstId + i).append("\",\"id\":")
          .append(firstId + i).append("}\n");
    return body.toString();
  }

  private JSONObject postBatch(final int serverIndex, final String body, final String contentType,
      final String queryParams) throws Exception {
    final HttpURLConnection conn = openBatchConnection(serverIndex, contentType, queryParams);
    writeBody(conn, body);
    conn.connect();

    try {
      final String response = readResponse(conn);
      assertThat(conn.getResponseCode()).isEqualTo(200);
      return new JSONObject(response);
    } finally {
      conn.disconnect();
    }
  }

  private HttpURLConnection openBatchConnection(final int serverIndex, final String contentType,
      final String queryParams) throws Exception {
    String url = "http://127.0.0.1:248" + serverIndex + "/api/v1/batch/graph";
    if (queryParams != null && !queryParams.isEmpty())
      url += "?" + queryParams;

    final HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
    conn.setRequestMethod("POST");
    conn.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    conn.setRequestProperty("Content-Type", contentType);
    conn.setDoOutput(true);
    return conn;
  }

  private void writeBody(final HttpURLConnection conn, final String body) throws Exception {
    final byte[] data = body.getBytes(StandardCharsets.UTF_8);
    conn.setRequestProperty("Content-Length", Integer.toString(data.length));
    try (final DataOutputStream wr = new DataOutputStream(conn.getOutputStream())) {
      wr.write(data);
    }
  }
}
