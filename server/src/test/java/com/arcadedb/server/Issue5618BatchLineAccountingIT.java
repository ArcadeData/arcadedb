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
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.DataOutputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
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
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5618BatchLineAccountingIT extends BaseGraphServerTest {

  @Override
  protected int getServerCount() {
    return 1;
  }

  /**
   * The equality the user had to establish by hand: what the file holds, what the server read, what it created.
   */
  @Test
  void everyLineOfASuccessfulLoadIsAccountedFor() throws Exception {
    createSchema();

    final String body = """
        {"@type":"vertex","@class":"Ent1","@id":"co/1","fid":"co/1"}

        {"@type":"vertex","@class":"Ent1","@id":"co/2","fid":"co/2"}
        {"@type":"vertex","@class":"Ent2","@id":"co/3","fid":"co/3"}

        {"@type":"edge","@class":"REL","@from":"co/1","@to":"co/3"}
        """;

    final JSONObject result = postBatch(body, "");

    assertThat(result.getLong("verticesCreated")).isEqualTo(3);
    assertThat(result.getLong("edgesCreated")).isEqualTo(1);
    assertThat(result.getLong("linesRead")).isEqualTo(6);
    assertThat(result.getLong("linesSkipped")).isEqualTo(2);
    assertThat(result.getLong("linesRead") - result.getLong("linesSkipped"))
        .as("every line read is either a record or a declared skip")
        .isEqualTo(result.getLong("verticesCreated") + result.getLong("edgesCreated"));
    assertThat(result.has("verticesWithoutId")).as("every vertex declared an @id").isFalse();
  }

  /**
   * A load that fails mid-stream needs the accounting more than a successful one does: it is the only way to place
   * the failure inside the file. The reported line was the entire diagnosis available on #5618.
   */
  @Test
  void aFailedLoadStillReportsHowMuchOfThePayloadItRead() throws Exception {
    createSchema();

    final String body = """
        {"@type":"vertex","@class":"Ent1","@id":"a1","fid":"a1"}
        {"@type":"vertex","@class":"Ent1","@id":"a2","fid":"a2"}
        {"@type":"edge","@class":"REL","@from":"a1","@to":"a2"}
        {"@type":"edge","@class":"REL","@from":"a1","@to":"never-declared"}
        {"@type":"edge","@class":"REL","@from":"a2","@to":"a1"}
        """;

    final JSONObject error = postBatchExpecting(400, body, "");

    assertThat(error.getString("error")).contains("Unknown temporary ID 'never-declared' at line 4");
    assertThat(error.getLong("verticesCreated")).isEqualTo(2);
    assertThat(error.getLong("edgesCreated")).isEqualTo(1);
    assertThat(error.getLong("linesRead")).as("the failing line was read too").isEqualTo(4);
    assertThat(error.getLong("linesSkipped")).isZero();
  }

  /**
   * The message that sent the reporter looking in the wrong place. It asserted ordering as the cause; the payload
   * ordering was fine. It now reports what is established - how many ids this payload declared - and names the two
   * causes it cannot tell apart, one of which (a reference to a vertex loaded by an EARLIER request) is invisible
   * from inside a single request and is exactly what a client splitting a large file hits.
   */
  @Test
  void anUnresolvedReferenceReportsWhatTheLoadKnowsInsteadOfAssumingBadOrdering() throws Exception {
    createSchema();

    final String body = """
        {"@type":"vertex","@class":"Ent1","@id":"b1","fid":"b1"}
        {"@type":"vertex","@class":"Ent1","@id":"b2","fid":"b2"}
        {"@type":"edge","@class":"REL","@from":"b1","@to":"b9"}
        """;

    final JSONObject error = postBatchExpecting(400, body, "");
    final String message = error.getString("error");

    assertThat(message).contains("2 ids mapped so far");
    assertThat(message).contains("each request resolves only the ids of its OWN payload");
    assertThat(message).contains("#bucket:position");
  }

  /**
   * A vertex with no {@code @id} is legal - nothing has to point at it - but it is also the one way a payload can
   * hold a vertex that is loaded and unreachable. It used to be completely silent, so an edge that meant to
   * reference one reported the same misleading "vertices must appear before edges". Both the count and the message
   * now say so.
   */
  @Test
  void verticesThatDeclareNoIdAreReportedInsteadOfDisappearing() throws Exception {
    createSchema();

    final String vertexOnly = """
        {"@type":"vertex","@class":"Ent1","@id":"c1","fid":"c1"}
        {"@type":"vertex","@class":"Ent1","fid":"c2"}
        {"@type":"vertex","@class":"Ent1","fid":"c3"}
        """;

    final JSONObject loaded = postBatch(vertexOnly, "");
    assertThat(loaded.getLong("verticesCreated")).isEqualTo(3);
    assertThat(loaded.getInt("verticesWithoutId"))
        .as("two vertices are durable but no edge can ever reference them")
        .isEqualTo(2);

    final String withEdge = """
        {"@type":"vertex","@class":"Ent1","@id":"d1","fid":"d1"}
        {"@type":"vertex","@class":"Ent1","fid":"d2"}
        {"@type":"edge","@class":"REL","@from":"d1","@to":"d2"}
        """;

    final JSONObject error = postBatchExpecting(400, withEdge, "");
    assertThat(error.getString("error")).contains("1 vertices carried no @id at all");
    assertThat(error.getInt("verticesWithoutId")).isEqualTo(1);
  }

  /**
   * CSV reads more lines than it produces records - a header per section plus the {@code ---} separator - so the
   * accounting has to name them, or a good load would look like it dropped four records.
   */
  @Test
  void csvHeadersAndSeparatorsAreCountedAsSkippedNotAsLostRecords() throws Exception {
    createSchema();

    final String body = """
        @type,@class,@id,fid
        vertex,Ent1,e1,e1
        vertex,Ent1,e2,e2
        ---
        @type,@class,@from,@to
        edge,REL,e1,e2
        """;

    final JSONObject result = postBatch(body, "", "text/csv");

    assertThat(result.getLong("verticesCreated")).isEqualTo(2);
    assertThat(result.getLong("edgesCreated")).isEqualTo(1);
    assertThat(result.getLong("linesRead")).isEqualTo(6);
    assertThat(result.getLong("linesSkipped")).isEqualTo(3);
    assertThat(result.getLong("linesRead") - result.getLong("linesSkipped"))
        .isEqualTo(result.getLong("verticesCreated") + result.getLong("edgesCreated"));
  }

  /**
   * The shape of the reported payload, scaled down to something a build can run: many vertices, several types
   * interleaved so the loader keeps flushing partial batches on the type change, and an edge for every single
   * vertex. Every temporary id has to resolve and every vertex has to be on disk - the claim the issue makes is
   * that one of them was not.
   */
  @Test
  @Tag("slow")
  void aLargeMultiTypeLoadLosesNoVertex() throws Exception {
    createSchema();

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

    final JSONObject result = postBatch(body.toString(), "vertexBatchSize=1000&idMapping=false");

    assertThat(result.getLong("verticesCreated")).isEqualTo(total);
    assertThat(result.getLong("edgesCreated")).isEqualTo(total);
    assertThat(result.getLong("linesRead")).isEqualTo(2L * total);
    assertThat(result.getLong("linesSkipped")).isZero();

    final Database db = getServerDatabase(0, getDatabaseName());
    assertThat(db.countType("Ent1", false) + db.countType("Ent2", false) + db.countType("Ent3", false))
        .isEqualTo(total);
  }

  private void createSchema() {
    final Database db = getServerDatabase(0, getDatabaseName());
    if (db.getSchema().existsType("Ent1"))
      return;

    db.getSchema().createVertexType("Ent1");
    db.getSchema().createVertexType("Ent2");
    db.getSchema().createVertexType("Ent3");
    db.getSchema().createEdgeType("REL");
  }

  private JSONObject postBatch(final String body, final String queryParams) throws Exception {
    return postBatch(body, queryParams, "application/x-ndjson");
  }

  private JSONObject postBatch(final String body, final String queryParams, final String contentType)
      throws Exception {
    return post(200, body, queryParams, contentType);
  }

  private JSONObject postBatchExpecting(final int status, final String body, final String queryParams)
      throws Exception {
    return post(status, body, queryParams, "application/x-ndjson");
  }

  private JSONObject post(final int expectedStatus, final String body, final String queryParams,
      final String contentType) throws Exception {
    String url = "http://127.0.0.1:2480/api/v1/batch/" + getDatabaseName();
    if (queryParams != null && !queryParams.isEmpty())
      url += "?" + queryParams;

    final HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
    conn.setRequestMethod("POST");
    conn.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    conn.setRequestProperty("Content-Type", contentType);
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
