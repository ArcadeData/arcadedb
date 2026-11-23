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

import com.arcadedb.database.Database;
import com.arcadedb.database.Record;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.handler.openapi.CoreApiSpec;
import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Paths;
import io.swagger.v3.oas.models.media.Schema;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.Socket;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Iterator;
import java.util.List;

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
      // since #9378 a batch can store light edges only into a type that declares LIGHTWEIGHT
      executeCommand(serverIndex, "sql", "CREATE EDGE TYPE ELight IF NOT EXISTS LIGHTWEIGHT");

      final String body = """
          {"@type":"vertex","@class":"V1","@id":"le1","id":500}
          {"@type":"vertex","@class":"V1","@id":"le2","id":501}
          {"@type":"edge","@class":"ELight","@from":"le1","@to":"le2"}
          """;

      final JSONObject result = postBatch(serverIndex, body, "application/x-ndjson", "lightEdges=true");
      assertThat(result.getInt("verticesCreated")).isEqualTo(2);
      assertThat(result.getInt("edgesCreated")).isEqualTo(1);
    });
  }

  /**
   * Regression for issue #9398: lightEdges=true into an edge type that does not declare LIGHTWEIGHT is refused with 400.
   */
  @Test
  void lightEdgesParameterRefusedOnUndeclaredType() throws Exception {
    testEachServer(serverIndex -> {
      final String body = """
          {"@type":"vertex","@class":"V1","@id":"lu1","id":502}
          {"@type":"vertex","@class":"V1","@id":"lu2","id":503}
          {"@type":"edge","@class":"E1","@from":"lu1","@to":"lu2"}
          """;

      final HttpURLConnection conn = openBatchConnection(serverIndex, "application/x-ndjson", "lightEdges=true");
      writeBody(conn, body);
      conn.connect();

      assertThat(conn.getResponseCode()).isEqualTo(400);
      assertThat(readError(conn)).contains("does not declare LIGHTWEIGHT");
      conn.disconnect();
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
      // {@code DateUtils.parseDateTime} produces given the database's configured timezone. The
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
    String url = "http://127.0.0.1:" + getServerHttpPort(serverIndex) + "/api/v1/batch/graph";
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

  /*
   * Issue #5618: a 17-million-vertex load stopped on {@code Unknown temporary ID 'co/32945720' at line 17234792}, for a
   * vertex that was sitting in the payload thousands of lines above the edge - and the vertex was not in the database
   * either. Nothing in the response, and nothing in the log, could tell whether the server had lost records or the
   * payload had never carried them: the counts it reported were of records it CREATED, with nothing to compare them
   * against. The user had to count the lines of their own file with grep to find out that two million vertices were
   * missing, and that still did not say which side had dropped them.
   * <p>
   * So the loader now accounts for the payload line by line. Every answer carries {@code linesRead} and
   * {@code linesSkipped}, which makes {@code linesRead - linesSkipped == verticesCreated + edgesCreated} something a
   * client can check against its own file - and something the server checks itself before answering 200, so a record
   * read and turned into nothing is reported as a failure instead of hiding inside a successful load.
   * <p>
   * The other half is the diagnosis. "Vertices must appear before edges that reference them" was the whole explanation
   * an unresolvable reference got, and it was the wrong one here; the message now reports what the load actually knows,
   * including how many vertices declared no {@code @id} at all and therefore cannot be referenced by anything.
   */

  // Regression: #5618 every payload line of a load is accounted for, and a lost reference says why
  /**
   * The equality the user had to establish by hand: what the file holds, what the server read, what it created.
   */
  @Test
  void everyLineOfASuccessfulLoadIsAccountedFor() throws Exception {
    createEntitySchema();

    final String body = """
        {"@type":"vertex","@class":"Ent1","@id":"co/1","fid":"co/1"}

        {"@type":"vertex","@class":"Ent1","@id":"co/2","fid":"co/2"}
        {"@type":"vertex","@class":"Ent2","@id":"co/3","fid":"co/3"}

        {"@type":"edge","@class":"REL","@from":"co/1","@to":"co/3"}
        """;

    final JSONObject result = loadExpectingSuccess(body, "");

    assertThat(result.getLong("verticesCreated")).isEqualTo(3);
    assertThat(result.getLong("edgesCreated")).isEqualTo(1);
    assertThat(result.getLong("linesRead")).isEqualTo(6);
    assertThat(result.getLong("linesSkipped")).isEqualTo(2);
    assertThat(result.getLong("linesRead") - result.getLong("linesSkipped"))
        .as("every line read is either a record or a declared skip")
        .isEqualTo(result.getLong("verticesCreated") + result.getLong("edgesCreated"));
    assertThat(result.has("verticesWithoutId")).as("every vertex declared an @id").isFalse();
  }

  // Regression: #5618 every payload line of a load is accounted for, and a lost reference says why
  /**
   * A load that fails mid-stream needs the accounting more than a successful one does: it is the only way to place
   * the failure inside the file. The reported line was the entire diagnosis available on #5618.
   */
  @Test
  void aFailedLoadStillReportsHowMuchOfThePayloadItRead() throws Exception {
    createEntitySchema();

    final String body = """
        {"@type":"vertex","@class":"Ent1","@id":"a1","fid":"a1"}
        {"@type":"vertex","@class":"Ent1","@id":"a2","fid":"a2"}
        {"@type":"edge","@class":"REL","@from":"a1","@to":"a2"}
        {"@type":"edge","@class":"REL","@from":"a1","@to":"never-declared"}
        {"@type":"edge","@class":"REL","@from":"a2","@to":"a1"}
        """;

    final JSONObject error = loadExpecting(400, body, "");

    assertThat(error.getString("error")).contains("Unknown temporary ID 'never-declared' at line 4");
    assertThat(error.getLong("verticesCreated")).isEqualTo(2);
    assertThat(error.getLong("edgesCreated")).isEqualTo(1);
    assertThat(error.getLong("linesRead")).as("the failing line was read too").isEqualTo(4);
    assertThat(error.getLong("linesSkipped")).isZero();
  }

  // Regression: #5618 every payload line of a load is accounted for, and a lost reference says why
  /**
   * The message that sent the reporter looking in the wrong place. It asserted ordering as the cause; the payload
   * ordering was fine. It now reports what is established - how many ids this payload declared - and names the two
   * causes it cannot tell apart, one of which (a reference to a vertex loaded by an EARLIER request) is invisible
   * from inside a single request and is exactly what a client splitting a large file hits.
   */
  @Test
  void anUnresolvedReferenceReportsWhatTheLoadKnowsInsteadOfAssumingBadOrdering() throws Exception {
    createEntitySchema();

    final String body = """
        {"@type":"vertex","@class":"Ent1","@id":"b1","fid":"b1"}
        {"@type":"vertex","@class":"Ent1","@id":"b2","fid":"b2"}
        {"@type":"edge","@class":"REL","@from":"b1","@to":"b9"}
        """;

    final JSONObject error = loadExpecting(400, body, "");
    final String message = error.getString("error");

    assertThat(message).contains("2 ids mapped so far");
    assertThat(message).contains("each request resolves only the ids of its OWN payload");
    assertThat(message).contains("#bucket:position");
  }

  // Regression: #5618 every payload line of a load is accounted for, and a lost reference says why
  /**
   * A vertex with no {@code @id} is legal - nothing has to point at it - but it is also the one way a payload can
   * hold a vertex that is loaded and unreachable. It used to be completely silent, so an edge that meant to
   * reference one reported the same misleading "vertices must appear before edges". Both the count and the message
   * now say so.
   */
  @Test
  void verticesThatDeclareNoIdAreReportedInsteadOfDisappearing() throws Exception {
    createEntitySchema();

    final String vertexOnly = """
        {"@type":"vertex","@class":"Ent1","@id":"c1","fid":"c1"}
        {"@type":"vertex","@class":"Ent1","fid":"c2"}
        {"@type":"vertex","@class":"Ent1","fid":"c3"}
        """;

    final JSONObject loaded = loadExpectingSuccess(vertexOnly, "");
    assertThat(loaded.getLong("verticesCreated")).isEqualTo(3);
    assertThat(loaded.getInt("verticesWithoutId"))
        .as("two vertices are durable but no edge can ever reference them")
        .isEqualTo(2);

    final String withEdge = """
        {"@type":"vertex","@class":"Ent1","@id":"d1","fid":"d1"}
        {"@type":"vertex","@class":"Ent1","fid":"d2"}
        {"@type":"edge","@class":"REL","@from":"d1","@to":"d2"}
        """;

    final JSONObject error = loadExpecting(400, withEdge, "");
    assertThat(error.getString("error")).contains("1 vertices carried no @id at all");
    assertThat(error.getInt("verticesWithoutId")).isEqualTo(1);
  }

  // Regression: #5618 every payload line of a load is accounted for, and a lost reference says why
  /**
   * CSV reads more lines than it produces records - a header per section plus the {@code ---} separator - so the
   * accounting has to name them, or a good load would look like it dropped four records.
   */
  @Test
  void csvHeadersAndSeparatorsAreCountedAsSkippedNotAsLostRecords() throws Exception {
    createEntitySchema();

    final String body = """
        @type,@class,@id,fid
        vertex,Ent1,e1,e1
        vertex,Ent1,e2,e2
        ---
        @type,@class,@from,@to
        edge,REL,e1,e2
        """;

    final JSONObject result = loadExpectingSuccess(body, "", "text/csv");

    assertThat(result.getLong("verticesCreated")).isEqualTo(2);
    assertThat(result.getLong("edgesCreated")).isEqualTo(1);
    assertThat(result.getLong("linesRead")).isEqualTo(6);
    assertThat(result.getLong("linesSkipped")).isEqualTo(3);
    assertThat(result.getLong("linesRead") - result.getLong("linesSkipped"))
        .isEqualTo(result.getLong("verticesCreated") + result.getLong("edgesCreated"));
  }

  // Regression: #5618 every payload line of a load is accounted for, and a lost reference says why
  /**
   * The shape of the reported payload, scaled down to something a build can run: many vertices, several types
   * interleaved so the loader keeps flushing partial batches on the type change, and an edge for every single
   * vertex. Every temporary id has to resolve and every vertex has to be on disk - the claim the issue makes is
   * that one of them was not.
   */
  @Test
  @Tag("slow")
  void aLargeMultiTypeLoadLosesNoVertex() throws Exception {
    createEntitySchema();

    final int total = 200_000;
    final StringBuilder body = new StringBuilder(total * 100);
    for (int i = 0; i < total; i++) {
      final String id = "co/" + (30_000_000 + i);
      body.append("{\"@type\":\"vertex\",\"@class\":\"Ent").append(1 + (i / 997) % 3).append("\",\"@id\":\"")
          .append(id).append("\",\"fid\":\"").append(id).append("\"}\n");
    }
    for (int i = 0; i < total; i++)
      body.append("{\"@type\":\"edge\",\"@class\":\"REL\",\"@from\":\"co/").append(30_000_000 + i)
          .append("\",\"@to\":\"co/").append(30_000_000 + (total - 1 - i)).append("\"}\n");

    final JSONObject result = loadExpectingSuccess(body.toString(), "vertexBatchSize=1000&idMapping=false");

    assertThat(result.getLong("verticesCreated")).isEqualTo(total);
    assertThat(result.getLong("edgesCreated")).isEqualTo(total);
    assertThat(result.getLong("linesRead")).isEqualTo(2L * total);
    assertThat(result.getLong("linesSkipped")).isZero();

    final Database db = getServerDatabase(0, getDatabaseName());
    assertThat(db.countType("Ent1", false) + db.countType("Ent2", false) + db.countType("Ent3", false))
        .isEqualTo(total);
  }

  private void createEntitySchema() {
    final Database db = getServerDatabase(0, getDatabaseName());
    if (db.getSchema().existsType("Ent1"))
      return;

    db.getSchema().createVertexType("Ent1");
    db.getSchema().createVertexType("Ent2");
    db.getSchema().createVertexType("Ent3");
    db.getSchema().createEdgeType("REL");
  }

  private JSONObject loadExpectingSuccess(final String body, final String queryParams) throws Exception {
    return loadExpectingSuccess(body, queryParams, "application/x-ndjson");
  }

  private JSONObject loadExpectingSuccess(final String body, final String queryParams, final String contentType)
      throws Exception {
    return postBatchExpecting(200, body, contentType, null, queryParams);
  }

  private JSONObject loadExpecting(final int status, final String body, final String queryParams)
      throws Exception {
    return postBatchExpecting(status, body, "application/x-ndjson", null, queryParams);
  }

  /*
   * Issue #7570, the half that is not documentation: the reported payload
   * <pre>{"@type":"vertex","@class":"Person","properties":{"name":"Alice"}}</pre>
   * answered 200 with {@code verticesCreated: 1} and stored a property literally named {@code properties} holding the
   * map. The counters were right and the data was wrong, and nothing failed until someone queried for {@code name} and
   * found nothing.
   * <p>
   * The unit tests in {@code BatchControlKeyValidationTest} pin the parsers. This one proves the refusal survives the
   * whole HTTP path - that it is reported as a 400 and not as the 408 a truncated body gets, that it carries the
   * partial-commit accounting the other {@code /batch} failures carry, and above all that the database is left with
   * nothing rather than with the wrong thing.
   */

  // Regression: #7570 a reserved or unknown control key is refused with a 400, not stored verbatim
  /** The payload copied from the issue. */
  @Test
  void nestingPropertiesUnderAPropertiesKeyIsRefusedInsteadOfStoredVerbatim() throws Exception {
    createVertexTypeIfAbsent("NestedProps");

    final JSONObject error = postBatchExpecting(400,
        "{\"@type\":\"vertex\",\"@class\":\"NestedProps\",\"properties\":{\"name\":\"Alice\"}}\n",
        "application/x-ndjson");

    assertThat(error.getString("error"))
        .contains("'properties'")
        .contains("line 1")
        .contains("flat");
    assertThat(error.getLong("verticesCreated")).isZero();

    final Database db = getServerDatabase(0, getDatabaseName());
    assertThat(db.countType("NestedProps", false))
        .as("the misread line must leave nothing behind, not a vertex carrying a 'properties' map")
        .isZero();
  }

  // Regression: #7570 a reserved or unknown control key is refused with a 400, not stored verbatim
  /** A scalar of the same name is data, not a mistake, and still loads. */
  @Test
  void aScalarPropertiesValueStillLoads() throws Exception {
    createVertexTypeIfAbsent("ScalarProps");

    final JSONObject result = postBatchExpecting(200,
        "{\"@type\":\"vertex\",\"@class\":\"ScalarProps\",\"@id\":\"s1\",\"properties\":\"public\"}\n",
        "application/x-ndjson");

    assertThat(result.getLong("verticesCreated")).isEqualTo(1);

    final Database db = getServerDatabase(0, getDatabaseName());
    final List<Object> stored = new ArrayList<>();
    final Iterator<Record> records = db.iterateType("ScalarProps", false);
    while (records.hasNext())
      stored.add(records.next().asVertex().get("properties"));

    assertThat(stored).as("a scalar under this name is data, and has to be stored under it")
        .containsExactly("public");
  }

  // Regression: #7570 a reserved or unknown control key is refused with a 400, not stored verbatim
  /** An unrecognised control key used to be stored as a property whose name begins with {@code @}. */
  @Test
  void anUnknownAtPrefixedKeyIsRefused() throws Exception {
    createVertexTypeIfAbsent("UnknownAtKey");

    final JSONObject error = postBatchExpecting(400,
        "{\"@type\":\"vertex\",\"@class\":\"UnknownAtKey\",\"@id\":\"u1\",\"@rid\":\"#1:0\"}\n",
        "application/x-ndjson");

    assertThat(error.getString("error")).contains("'@rid'").contains("line 1");
  }

  // Regression: #7570 a reserved or unknown control key is refused with a 400, not stored verbatim
  /**
   * The refusal has to land as a 400 on a body that arrived whole. {@code PostBatchHandler} answers 408 for a
   * {@code MalformedBatchRecordException} raised on a body that ended early, which is why the reserved-key throw is
   * a plain {@code IllegalArgumentException}: a client sent a complete, well-formed line that the server declines,
   * and telling it to go hunting for a truncated upload would send it to the wrong place.
   */
  @Test
  void theRefusalKeepsTheAccountingAndTheLinesTheLoadHadReached() throws Exception {
    createVertexTypeIfAbsent("Accounting");

    final String body = """
        {"@type":"vertex","@class":"Accounting","@id":"a1","name":"Alice"}
        {"@type":"vertex","@class":"Accounting","@id":"a2","name":"Bob"}
        {"@type":"vertex","@class":"Accounting","@id":"a3","@cat":"v"}
        """;

    final JSONObject error = postBatchExpecting(400, body, "application/x-ndjson");

    assertThat(error.getString("error")).contains("'@cat'").contains("line 3");
    assertThat(error.getLong("linesRead")).as("the failing line was read too").isEqualTo(3);
    assertThat(error.getLong("linesSkipped")).isZero();
    assertThat(error.has("partialCommit")).isTrue();
  }

  // Regression: #7570 a reserved or unknown control key is refused with a 400, not stored verbatim
  /**
   * The streaming encoding answers 200 for a load that failed <em>after</em> it acknowledged a chunk, because the
   * status line is already gone by then. A reserved key on line 1 is rejected before anything is acknowledged, so the
   * request must still get its real 400 and the buffered error body - the guarantee the operation documents for its
   * 400 and 408, which a refusal introduced on this path must not quietly break.
   */
  @Test
  void theRefusalIsStillA400UnderTheStreamingEncoding() throws Exception {
    createVertexTypeIfAbsent("Streaming");

    final JSONObject error = postBatchExpecting(400,
        "{\"@type\":\"vertex\",\"@class\":\"Streaming\",\"properties\":{\"name\":\"Alice\"}}\n",
        "application/x-ndjson", "application/x-ndjson");

    assertThat(error.getString("error")).contains("'properties'").contains("line 1");
  }

  // Regression: #7570 a reserved or unknown control key is refused with a 400, not stored verbatim
  /** Same defect, CSV spelling: the control keys are column names there, so the header is where it is caught. */
  @Test
  void anUnknownAtPrefixedCsvColumnIsRefused() throws Exception {
    createVertexTypeIfAbsent("CsvUnknownAtCol");

    final String body = """
        @type,@class,@id,@rid,name
        vertex,CsvUnknownAtCol,c1,#1:0,Carol
        """;

    final JSONObject error = postBatchExpecting(400, body, "text/csv");

    assertThat(error.getString("error")).contains("'@rid'").contains("line 1");

    final Database db = getServerDatabase(0, getDatabaseName());
    assertThat(db.query("sql", "SELECT FROM CsvUnknownAtCol WHERE name = 'Carol'").stream().count()).isZero();
  }

  // Regression: #7570 a reserved or unknown control key is refused with a 400, not stored verbatim
  /** A CSV column named {@code properties} carries a scalar and is ordinary data, so it must keep working. */
  @Test
  void aCsvPropertiesColumnStillLoads() throws Exception {
    createVertexTypeIfAbsent("CsvScalarProps");

    final String body = """
        @type,@class,@id,properties
        vertex,CsvScalarProps,c2,public
        """;

    final JSONObject result = postBatchExpecting(200, body, "text/csv");

    assertThat(result.getLong("verticesCreated")).isEqualTo(1);
  }

  /**
   * Every test gets its own vertex type. The server database is shared across the methods of this class and the
   * assertions here are about what a refused load left behind, so a type another method had already written to would
   * make them pass or fail on execution order rather than on the fix.
   */
  private void createVertexTypeIfAbsent(final String vertexType) {
    final Database db = getServerDatabase(0, getDatabaseName());
    if (!db.getSchema().existsType(vertexType))
      db.getSchema().createVertexType(vertexType);
  }

  /*
   * Issue #7574 over the wire. {@code BatchControlKeyValidationTest} pins the two parsers; this proves the
   * refusal reaches the client as a 400 rather than as the 408 a truncated body gets, that it carries the same
   * partial-commit accounting every other {@code /batch} failure carries, and that the load left nothing behind.
   * <p>
   * The payload in the first test is the one from the issue: before the fix it answered 200 with
   * {@code edgesCreated: 1} and the {@code @id} nowhere - neither stored nor reported.
   *
   * @see <a href="https://github.com/ArcadeData/arcadedb/issues/7574">issue #7574</a>
   */

  // Regression: #7574 a control key on the wrong kind of line is refused with a 400
  @Test
  void anIdOnAnEdgeLineIsRefusedWithA400InsteadOfBeingDropped() throws Exception {
    createVertexAndEdgeTypesIfAbsent("Misplaced7574A");

    final String body = """
        {"@type":"vertex","@class":"Misplaced7574A","@id":"p1","name":"Alice"}
        {"@type":"vertex","@class":"Misplaced7574A","@id":"p2","name":"Bob"}
        {"@type":"edge","@class":"Misplaced7574AEdge","@from":"p1","@to":"p2","@id":"e1"}
        """;

    final JSONObject error = postBatchExpecting(400, body, "application/x-ndjson");

    assertThat(error.getString("error"))
        .contains("'@id'")
        .contains("line 3")
        .contains("vertex line");
    // The accounting a client needs to reconcile: the two vertices were attempted before the offending line.
    assertThat(error.has("verticesCreated")).as("a batch failure reports what it had attempted").isTrue();
    assertThat(error.has("partialCommit")).isTrue();
  }

  // Regression: #7574 a control key on the wrong kind of line is refused with a 400
  @Test
  void aFromOnAVertexLineIsRefusedAndStoresNothing() throws Exception {
    createVertexAndEdgeTypesIfAbsent("Misplaced7574B");

    final JSONObject error = postBatchExpecting(400,
        "{\"@type\":\"vertex\",\"@class\":\"Misplaced7574B\",\"@id\":\"p1\",\"@from\":\"somewhere\"}\n",
        "application/x-ndjson");

    assertThat(error.getString("error")).contains("'@from'").contains("edge line");

    final Database db = getServerDatabase(0, getDatabaseName());
    assertThat(db.countType("Misplaced7574B", false))
        .as("the misplaced key is refused before the record is created, not after")
        .isZero();
  }

  // Regression: #7574 a control key on the wrong kind of line is refused with a 400
  /**
   * The CSV rule refuses a value, not a column, so the single-header form a client may already be sending keeps
   * loading. This is the payload that would have broken had the refusal been keyed on the header.
   */
  @Test
  void oneCsvHeaderAcrossBothSectionsStillLoadsWhenTheUnusedColumnsAreEmpty() throws Exception {
    createVertexAndEdgeTypesIfAbsent("Misplaced7574C");

    final String body = """
        @type,@class,@id,@from,@to,name
        vertex,Misplaced7574C,p1,,,Alice
        vertex,Misplaced7574C,p2,,,Bob
        edge,Misplaced7574CEdge,,p1,p2,
        """;

    final JSONObject result = postBatchExpecting(200, body, "text/csv");

    assertThat(result.getLong("verticesCreated")).isEqualTo(2);
    assertThat(result.getLong("edgesCreated")).isEqualTo(1);
  }

  // Regression: #7574 a control key on the wrong kind of line is refused with a 400
  /** And the same header refuses a value the row's kind cannot use. */
  @Test
  void aCsvRowCarryingAControlValueItsKindCannotUseIsRefused() throws Exception {
    createVertexAndEdgeTypesIfAbsent("Misplaced7574D");

    final String body = """
        @type,@class,@id,@from,@to,name
        vertex,Misplaced7574D,p1,nowhere,,Alice
        """;

    final JSONObject error = postBatchExpecting(400, body, "text/csv");

    assertThat(error.getString("error")).contains("'@from'").contains("line 2");
  }

  /**
   * Every test gets its own types, because the server database is shared across the methods of this class and the
   * assertions are about what a refused load left behind.
   */
  private void createVertexAndEdgeTypesIfAbsent(final String vertexType) {
    final Database db = getServerDatabase(0, getDatabaseName());
    if (!db.getSchema().existsType(vertexType))
      db.getSchema().createVertexType(vertexType);
    if (!db.getSchema().existsType(vertexType + "Edge"))
      db.getSchema().createEdgeType(vertexType + "Edge");
  }

  /*
   * Issue #7578, the half the sweeps cannot do: a {@code required} list is a claim about what a HANDLER sends, so
   * the only way to know it is true is to make the handler send one.
   * <p>
   * {@code BatchError} is the schema where getting it wrong was easiest, and where this PR did get it wrong first
   * time round (code review on PR #7749). It is bound to two statuses, not one: {@code POST /batch} answers 400
   * for a line it refuses and 408 for a body that did not arrive whole, and the two are built by different methods.
   * {@code exception} is unconditional on the 400 and absent on two of the three 408 paths - a body that simply
   * ended before its announced length, and a malformed record that turned out to be a cut upload - because
   * {@code partialPayloadResponse} writes it only when there is a class to name.
   * <p>
   * Both statuses are driven here and every name the schema requires is checked against what actually arrived. The
   * expectation is read out of {@link CoreApiSpec}'s own document, so the assertion cannot drift from the thing it
   * is checking: adding a name to the required list and not to the response fails this test.
   *
   * @see <a href="https://github.com/ArcadeData/arcadedb/issues/7578">issue #7578</a>
   */

  /** The names {@code BatchError} promises, read from the document rather than written out here. */
  private static List<String> requiredByTheDocument() {
    final OpenAPI openAPI = new OpenAPI();
    openAPI.setPaths(new Paths());
    openAPI.setComponents(new Components());
    new CoreApiSpec().contribute(openAPI);

    final Schema<?> batchError = openAPI.getComponents().getSchemas().get("BatchError");
    assertThat(batchError).as("BatchError must be registered").isNotNull();
    assertThat(batchError.getRequired()).as("and it must declare what it always sends").isNotEmpty();
    return batchError.getRequired();
  }

  private static void assertCarriesEveryRequiredName(final JSONObject body) {
    for (final String required : requiredByTheDocument())
      assertThat(body.has(required))
          .as("BatchError promises '%s' on every failure; the response was %s", required, body)
          .isTrue();
  }

  // Regression: #7578 every name BatchError requires is in what the handler sends, on 400 and 408
  /** The 400: a line the parser refuses. Here `exception` IS present, which is why it reads as unconditional. */
  @Test
  void aRefusedLineCarriesEveryNameTheSchemaRequires() throws Exception {
    createVertexTypeIfAbsent("Batch7578A");

    final JSONObject body = postWholeBody(400,
        "{\"@type\":\"vertex\",\"@class\":\"Batch7578A\",\"@id\":\"p1\",\"@rid\":\"#1:0\"}\n");

    assertCarriesEveryRequiredName(body);
    assertThat(body.has("exception"))
        .as("a refused line is an IllegalArgumentException, so the 400 does name a class")
        .isTrue();
  }

  // Regression: #7578 every name BatchError requires is in what the handler sends, on 400 and 408
  /**
   * The 408 that makes {@code exception} conditional, and the reason this test exists at all.
   * <p>
   * A line the parser cannot read is normally a 400 naming it - but when the body ALSO ended before its
   * announced length, {@code PostBatchHandler} reports the load as truncated instead, because a cut upload hands
   * the parser stale bytes that look like a malformed line the client never sent (issue #5470). That branch
   * calls {@code truncatedBody} with a null exception class: nothing threw, the parser simply refused a line, so
   * there is no class to name and {@code partialPayloadResponse} omits the member.
   * <p>
   * Reached by announcing more bytes than are sent and then half-closing. The parser refuses line one on its own
   * content - it never reaches a read that could raise - and only THEN does {@code bodyEndedEarly} probe and find
   * the peer gone. That ordering is what separates this path from the one below, where the line is valid, the
   * parser loops for more input, and Undertow raises the I/O failure itself.
   */
  @Test
  void aRefusedLineOnACutUploadCarriesNoExceptionWhichIsWhyItIsNotRequired() throws Exception {
    createVertexTypeIfAbsent("Batch7578B");

    // A JSON array, which the parser refuses on sight as a MalformedBatchRecordException.
    final String sent = "[{\"@type\":\"vertex\",\"@class\":\"Batch7578B\"}]\n";
    final JSONObject body = postRaw(sent, sent.length() + 512, 408, true);

    assertCarriesEveryRequiredName(body);
    assertThat(body.has("exception"))
        .as("nothing threw - the parser refused a line and the body was short, so there is no class to name. "
            + "This is why 'exception' must not be in BatchError's required list. The response was %s", body)
        .isFalse();
    assertThat(body.getString("error")).contains("truncated");
  }

  // Regression: #7578 every name BatchError requires is in what the handler sends, on 400 and 408
  /**
   * The other 408 path, kept as the contrast: when the line is VALID, the parser loops for the bytes that never
   * arrive, Undertow raises an {@link java.io.IOException} itself, and the same response DOES name it.
   * <p>
   * So {@code exception} is not "absent on the 408" either - it is present on one of that status's three paths
   * and absent on the other two, which is exactly the shape a {@code required} list cannot express and must
   * therefore leave out.
   */
  @Test
  void aPeerThatGoesAwayMidBodyDoesNameItsException() throws Exception {
    createVertexTypeIfAbsent("Batch7578C");

    final String sent = "{\"@type\":\"vertex\",\"@class\":\"Batch7578C\",\"@id\":\"p1\",\"name\":\"Alice\"}\n";
    final JSONObject body = postRaw(sent, sent.length() + 512, 408, true);

    assertCarriesEveryRequiredName(body);
    assertThat(body.has("exception"))
        .as("the peer closing mid-body IS an IOException, so this path names a class. The response was %s", body)
        .isTrue();
    assertThat(body.getString("error")).contains("truncated");
  }

  private String host() {
    return "127.0.0.1";
  }

  private int port() {
    return getServer(0).getHttpServer().getPort();
  }

  private static String authorization() {
    return "Basic " + Base64.getEncoder()
        .encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8));
  }

  private JSONObject postWholeBody(final int expectedStatus, final String payload) throws Exception {
    return postRaw(payload, payload.getBytes(StandardCharsets.UTF_8).length, expectedStatus, false);
  }

  /**
   * @param halfClose true to shut the write side down after the body, which makes Undertow report the peer as
   *                  gone and raise an IOException; false to leave it open, so a parser that refuses a line does
   *                  so without anything throwing
   */
  private JSONObject postRaw(final String payload, final int contentLength, final int expectedStatus,
      final boolean halfClose) throws Exception {
    final byte[] sent = payload.getBytes(StandardCharsets.UTF_8);

    try (final Socket socket = new Socket(host(), port())) {
      socket.setSoTimeout(30_000);
      final OutputStream out = socket.getOutputStream();
      out.write(("POST /api/v1/batch/" + getDatabaseName() + " HTTP/1.1\r\n"
          + "Host: " + host() + ":" + port() + "\r\n"
          + "Authorization: " + authorization() + "\r\n"
          + "Content-Type: application/x-ndjson\r\n"
          + "Content-Length: " + contentLength + "\r\n"
          + "Connection: close\r\n\r\n").getBytes(StandardCharsets.UTF_8));
      out.write(sent);
      out.flush();
      if (halfClose)
        // The server sees the peer go away mid-body, which Undertow reports as an IOException.
        socket.shutdownOutput();

      return readRawResponse(socket, expectedStatus);
    }
  }

  private static JSONObject readRawResponse(final Socket socket, final int expectedStatus) throws Exception {
    final BufferedReader reader = new BufferedReader(
        new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));

    final String statusLine = reader.readLine();
    assertThat(statusLine).as("the server must answer at all").isNotNull();

    String line;
    boolean chunked = false;
    while ((line = reader.readLine()) != null && !line.isEmpty())
      if (line.toLowerCase().startsWith("transfer-encoding:") && line.toLowerCase().contains("chunked"))
        chunked = true;

    final StringBuilder body = new StringBuilder();
    while ((line = reader.readLine()) != null)
      body.append(line);

    final String raw = body.toString();
    // The JSON object is the only thing on the body, whether or not chunk sizes are interleaved around it.
    final int start = raw.indexOf('{');
    final int end = raw.lastIndexOf('}');
    assertThat(start).as("response carried no JSON body: status '%s', raw '%s'", statusLine, raw).isNotNegative();

    assertThat(statusLine)
        .as("expected %d, body was %s", expectedStatus, raw)
        .contains(" " + expectedStatus + " ");
    assertThat(chunked || !raw.isEmpty()).isTrue();

    return new JSONObject(raw.substring(start, end + 1));
  }

  /*
   * Issue #8625: a batch loaded with {@code bidirectional=false} used to skip the incoming side of an edge type declared
   * bidirectional, leaving edges no query could reach from their target. The load is now refused with a client error,
   * and a unidirectional type still loads.
   */

  // Regression: #8625 a unidirectional load on a bidirectional edge type is a client error
  @Test
  void aUnidirectionalBatchOnABidirectionalTypeIsAClientError() throws Exception {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.getSchema().getOrCreateVertexType("Issue8625V");
    database.getSchema().createEdgeType("Issue8625Both");

    final JSONObject error = postBatchExpecting(400, bidirectionalBody("Issue8625Both"), "application/x-ndjson", null, "bidirectional=false");
    assertThat(error.getString("error")).contains("Edge type 'Issue8625Both' is bidirectional");
    assertThat(database.countType("Issue8625Both", false)).isZero();
  }

  // Regression: #8625 a unidirectional load on a bidirectional edge type is a client error
  @Test
  void aUnidirectionalBatchOnAUnidirectionalTypeLoads() throws Exception {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.getSchema().getOrCreateVertexType("Issue8625V");
    database.getSchema().buildEdgeType().withName("Issue8625One").withBidirectional(false).create();

    final JSONObject result = postBatchExpecting(200, bidirectionalBody("Issue8625One"), "application/x-ndjson", null, "bidirectional=false");
    assertThat(result.getLong("edgesCreated")).isEqualTo(1);
    assertThat(database.countType("Issue8625One", false)).isEqualTo(1);
  }

  private static String bidirectionalBody(final String edgeType) {
    return "{\"@type\":\"vertex\",\"@class\":\"Issue8625V\",\"@id\":\"a\"}\n"
        + "{\"@type\":\"vertex\",\"@class\":\"Issue8625V\",\"@id\":\"b\"}\n"
        + "{\"@type\":\"edge\",\"@class\":\"" + edgeType + "\",\"@from\":\"a\",\"@to\":\"b\"}\n";
  }

  private JSONObject postBatchExpecting(final int expectedStatus, final String body, final String contentType)
      throws Exception {
    return postBatchExpecting(expectedStatus, body, contentType, null, null);
  }

  private JSONObject postBatchExpecting(final int expectedStatus, final String body, final String contentType,
      final String accept) throws Exception {
    return postBatchExpecting(expectedStatus, body, contentType, accept, null);
  }

  /**
   * Posts {@code body} to {@code /api/v1/batch} of server 0, asserts the status and returns the parsed answer: the response
   * stream below 400, the error stream from 400 on.
   */
  private JSONObject postBatchExpecting(final int expectedStatus, final String body, final String contentType,
      final String accept, final String queryParams) throws Exception {
    // Never a hardcoded 2480: the server binds the first free port of the configured range, and a port pinned to
    // 2480 is answered by whatever else already listens there - which surfaces as a 403 rather than as a conflict.
    String url = getServerHttpUrl("/api/v1/batch/") + getDatabaseName();
    if (queryParams != null && !queryParams.isEmpty())
      url += "?" + queryParams;

    final HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
    conn.setRequestMethod("POST");
    conn.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    conn.setRequestProperty("Content-Type", contentType);
    if (accept != null)
      conn.setRequestProperty("Accept", accept);
    conn.setDoOutput(true);

    final byte[] data = body.getBytes(StandardCharsets.UTF_8);
    conn.setRequestProperty("Content-Length", Integer.toString(data.length));
    try (final DataOutputStream out = new DataOutputStream(conn.getOutputStream())) {
      out.write(data);
    }
    conn.connect();

    try {
      final int status = conn.getResponseCode();
      final InputStream in = status < 400 ? conn.getInputStream() : conn.getErrorStream();
      final String response = new String(in.readAllBytes(), StandardCharsets.UTF_8);
      assertThat(status).as("response: %s", response).isEqualTo(expectedStatus);
      return new JSONObject(response);
    } finally {
      conn.disconnect();
    }
  }
}
