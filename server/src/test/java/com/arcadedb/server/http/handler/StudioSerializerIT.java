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

import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;
import java.util.Collections;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Records, vertices and edges returned over HTTP through the default and the Studio serializer ({@code serializer=studio}),
 * which builds the graph view and filters out the edges that are not connected to the returned records.
 */
class StudioSerializerIT extends BaseGraphServerTest {
  /* Regression test for issue #4689: MATCH (u:IssueUser) RETURN u must not throw NSE via HTTP. */
  // Regression: #4689 MATCH ... RETURN of a whole vertex must not throw over HTTP
  @Test
  void cypherMatchReturnWholeVertexShouldNotThrow() throws Exception {
    executeCommand(0, "opencypher", "CREATE (u:IssueUser {name: 'Alice', age: 30})");
    executeCommand(0, "opencypher", "CREATE (u:IssueUser {name: 'Bob', age: 25})");

    final JSONObject response = executeCommand(0, "opencypher", "MATCH (u:IssueUser) RETURN u");

    assertThat(response).isNotNull();
    assertThat(response.has("result")).as("Response should have 'result' key - query must not throw").isTrue();
    final JSONObject result = response.getJSONObject("result");
    assertThat(result.getJSONArray("records").length()).as("Should return 2 User vertices").isEqualTo(2);
  }

  // Regression: #4689 MATCH ... RETURN of a whole vertex must not throw over HTTP
  @Test
  void cypherMatchReturnVertexProjectionWorkaround() throws Exception {
    executeCommand(0, "opencypher", "CREATE (u:IssueUserProj {name: 'Charlie', age: 35})");

    final JSONObject response = executeCommand(0, "opencypher", "MATCH (u:IssueUserProj) RETURN u{.*} as user");

    assertThat(response).isNotNull();
    assertThat(response.has("result")).isTrue();
    final JSONObject result = response.getJSONObject("result");
    assertThat(result.getJSONArray("records").length()).as("Projection workaround should return 1 vertex").isEqualTo(1);
  }

  // Regression: #4689 MATCH ... RETURN of a whole vertex must not throw over HTTP
  @Test
  void sqlSelectFromTypeShouldNotThrow() throws Exception {
    executeCommand(0, "sql", "CREATE VERTEX TYPE SqlSelectAll");
    executeCommand(0, "sql", "INSERT INTO SqlSelectAll SET name = 'Alice', age = 30");
    executeCommand(0, "sql", "INSERT INTO SqlSelectAll SET name = 'Bob', age = 25");

    final JSONObject response = executeCommand(0, "sql", "SELECT FROM SqlSelectAll");

    assertThat(response).isNotNull();
    assertThat(response.has("result")).as("SELECT FROM should not throw NoSuchElementException").isTrue();
    final JSONObject result = response.getJSONObject("result");
    assertThat(result.getJSONArray("records").length()).as("Should return 2 records").isEqualTo(2);
  }

  // Regression: #4689 MATCH ... RETURN of a whole vertex must not throw over HTTP
  @Test
  void sqlSelectFieldsShouldWork() throws Exception {
    executeCommand(0, "sql", "CREATE VERTEX TYPE SqlSelectFields");
    executeCommand(0, "sql", "INSERT INTO SqlSelectFields SET name = 'Dave', age = 40");

    final JSONObject response = executeCommand(0, "sql", "SELECT name FROM SqlSelectFields");

    assertThat(response).isNotNull();
    assertThat(response.has("result")).isTrue();
    final JSONObject result = response.getJSONObject("result");
    assertThat(result.getJSONArray("records").length()).as("Should return 1 record").isEqualTo(1);
  }

  // Regression: #4689 MATCH ... RETURN of a whole vertex must not throw over HTTP
  @Test
  void cypherMatchReturnManyVerticesShouldWork() throws Exception {
    // Create 110 vertices in a single command to test multi-batch pagination (>100 default batch)
    executeCommand(0, "opencypher",
        "UNWIND range(0, 109) AS i CREATE (:ManyUsers {idx: i, name: 'User' + toString(i)})");

    final JSONObject response = executeCommand(0, "opencypher", "MATCH (u:ManyUsers) RETURN u");

    assertThat(response).isNotNull();
    assertThat(response.has("result")).isTrue();
    final JSONObject result = response.getJSONObject("result");
    assertThat(result.getJSONArray("records").length())
        .as("Should return all 110 vertices across pagination batches")
        .isEqualTo(110);
  }

  // Regression: #4689 MATCH ... RETURN of a whole vertex must not throw over HTTP
  @Test
  void cypherMatchReturnVertexWithEdgesShouldWork() throws Exception {
    executeCommand(0, "opencypher", "CREATE (u:UserEdgeTest {name: 'Src', role: 'admin'})");
    executeCommand(0, "opencypher", "CREATE (u:UserEdgeTest {name: 'Dst', role: 'user'})");
    executeCommand(0, "sql",
        "CREATE EDGE E1 FROM (SELECT FROM UserEdgeTest WHERE name = 'Src') TO (SELECT FROM UserEdgeTest WHERE name = 'Dst')");

    final JSONObject response = executeCommand(0, "opencypher", "MATCH (u:UserEdgeTest) RETURN u");

    assertThat(response).isNotNull();
    assertThat(response.has("result")).isTrue();
    final JSONObject result = response.getJSONObject("result");
    final JSONArray records = result.getJSONArray("records");
    assertThat(records.length()).as("Should return 2 vertices").isEqualTo(2);
    // Verify edge counts are populated via setMetadata() - at least one vertex must have edges
    boolean hasEdgeCounts = false;
    for (int i = 0; i < records.length(); i++) {
      final JSONObject rec = records.getJSONObject(i);
      if (rec.getInt("@out", 0) > 0 || rec.getInt("@in", 0) > 0) {
        hasEdgeCounts = true;
        break;
      }
    }
    assertThat(hasEdgeCounts).as("At least one vertex should have non-zero edge count in @out or @in").isTrue();
  }

  // Regression: #4689 MATCH ... RETURN of a whole vertex must not throw over HTTP
  @Test
  void cypherMatchReturnVertexWithDefaultSerializer() throws Exception {
    executeCommand(0, "opencypher", "CREATE (u:DefaultSerTest {name: 'Alice2', age: 30})");
    executeCommand(0, "opencypher", "CREATE (u:DefaultSerTest {name: 'Bob2', age: 25})");

    final HttpURLConnection connection = (HttpURLConnection) new URL(
        getServerHttpUrl("/api/v1/command/") + getDatabaseName()).openConnection();
    try {
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      formatPayload(connection, "opencypher", "MATCH (u:DefaultSerTest) RETURN u", null, Collections.emptyMap());
      connection.connect();

      assertThat(connection.getResponseCode())
          .as("MATCH RETURN u with default serializer should return HTTP 200, not 500").isEqualTo(200);

      final String responseBody = readResponse(connection);
      final JSONObject response = new JSONObject(responseBody);
      assertThat(response.has("result")).isTrue();
      final JSONArray result = response.getJSONArray("result");
      assertThat(result.length()).as("Should return 2 vertices").isEqualTo(2);
    } finally {
      connection.disconnect();
    }
  }

  /*
   * Regression test for issue #4689 over the Studio serializer (serializer=studio), which builds the
   * full graph (vertices + edges + records) and runs the "filter out not connected edges" loop that
   * iterates getEdges() on every returned vertex - the studio-only path HTTP/Bolt clients never hit.
   */
  /**
   * Helper that always sends serializer=studio (what {@link #executeCommand(int, String, String)}
   * already does) and returns the {@code result} object containing vertices/edges/records.
   */
  private JSONObject studioResult(final String language, final String command) throws Exception {
    final JSONObject response = executeCommand(0, language, command);
    assertThat(response).as("Studio query '%s' must not throw (no HTTP 500)", command).isNotNull();
    assertThat(response.has("result")).isTrue();
    return response.getJSONObject("result");
  }

  // Regression: #4689 the same over the Studio serializer, which iterates the edges of every returned vertex
  @Test
  void matchReturnVertexWithEdgesBetweenReturnedVertices() throws Exception {
    // Two returned vertices connected to each other - exercises the FILTER OUT NOT CONNECTED EDGES loop
    executeCommand(0, "opencypher", "CREATE (u:StudioUser {name: 'Alice'})");
    executeCommand(0, "opencypher", "CREATE (u:StudioUser {name: 'Bob'})");
    executeCommand(0, "sql", "CREATE EDGE TYPE Knows");
    executeCommand(0, "sql",
        "CREATE EDGE Knows FROM (SELECT FROM StudioUser WHERE name = 'Alice') TO (SELECT FROM StudioUser WHERE name = 'Bob')");

    final JSONObject result = studioResult("opencypher", "MATCH (u:StudioUser) RETURN u");

    assertThat(result.getJSONArray("records").length()).as("Should return 2 vertices").isEqualTo(2);
    assertThat(result.getJSONArray("vertices").length()).as("Both vertices should be in the graph").isEqualTo(2);
    assertThat(result.getJSONArray("edges").length())
        .as("The connecting edge should be surfaced by the studio filter loop").isEqualTo(1);
  }

  // Regression: #4689 the same over the Studio serializer, which iterates the edges of every returned vertex
  @Test
  void matchReturnVertexWithEdgesToVerticesOutsideResult() throws Exception {
    // The returned vertex has an edge to a vertex NOT matched by the query - the filter loop
    // must iterate the edge and exclude it (the target is not in includedVertices)
    executeCommand(0, "opencypher", "CREATE (u:StudioHub {name: 'Hub'})");
    executeCommand(0, "opencypher", "CREATE (u:StudioLeaf {name: 'Leaf'})");
    executeCommand(0, "sql", "CREATE EDGE TYPE Points");
    executeCommand(0, "sql",
        "CREATE EDGE Points FROM (SELECT FROM StudioHub WHERE name = 'Hub') TO (SELECT FROM StudioLeaf WHERE name = 'Leaf')");

    // Only match the hub; its edge points outside the result set
    final JSONObject result = studioResult("opencypher", "MATCH (u:StudioHub) RETURN u");

    assertThat(result.getJSONArray("records").length()).as("Should return the hub vertex").isEqualTo(1);
    assertThat(result.getJSONArray("vertices").length()).as("Only the hub is in the graph").isEqualTo(1);
    assertThat(result.getJSONArray("edges").length())
        .as("Edge to an unmatched vertex must be filtered out").isEqualTo(0);
  }

  // Regression: #4689 the same over the Studio serializer, which iterates the edges of every returned vertex
  @Test
  void matchReturnVertexWithSelfLoopEdge() throws Exception {
    // Self-referencing edge - both endpoints are the same returned vertex
    executeCommand(0, "opencypher", "CREATE (u:StudioSelf {name: 'Solo'})");
    executeCommand(0, "sql", "CREATE EDGE TYPE SelfRef");
    executeCommand(0, "sql",
        "CREATE EDGE SelfRef FROM (SELECT FROM StudioSelf WHERE name = 'Solo') TO (SELECT FROM StudioSelf WHERE name = 'Solo')");

    final JSONObject result = studioResult("opencypher", "MATCH (u:StudioSelf) RETURN u");

    assertThat(result.getJSONArray("records").length()).as("Should return the vertex").isEqualTo(1);
    assertThat(result.getJSONArray("vertices").length()).isEqualTo(1);
    assertThat(result.getJSONArray("edges").length()).as("Self-loop edge should be surfaced").isEqualTo(1);
  }

  // Regression: #4689 the same over the Studio serializer, which iterates the edges of every returned vertex
  @Test
  void matchReturnHubVertexWithManyEdges() throws Exception {
    // A hub vertex with many outgoing edges - stresses the per-vertex getEdges() iteration
    executeCommand(0, "opencypher", "CREATE (u:StudioBigHub {name: 'Center'})");
    for (int i = 0; i < 25; i++)
      executeCommand(0, "opencypher", "CREATE (u:StudioSpoke {idx: " + i + "})");
    executeCommand(0, "sql", "CREATE EDGE TYPE Spoke");
    executeCommand(0, "sql",
        "CREATE EDGE Spoke FROM (SELECT FROM StudioBigHub WHERE name = 'Center') TO (SELECT FROM StudioSpoke)");

    // Match the whole connected component so every spoke edge connects two included vertices
    final JSONObject result = studioResult("opencypher", "MATCH (u) WHERE u:StudioBigHub OR u:StudioSpoke RETURN u");

    assertThat(result.getJSONArray("records").length()).as("Hub + 25 spokes").isEqualTo(26);
    assertThat(result.getJSONArray("vertices").length()).isEqualTo(26);
    assertThat(result.getJSONArray("edges").length()).as("All 25 spoke edges should be surfaced").isEqualTo(25);
  }

  // Regression: #4689 the same over the Studio serializer, which iterates the edges of every returned vertex
  @Test
  void sqlSelectWholeRecordOverStudio() throws Exception {
    // SQL whole-record SELECT over the studio serializer with connected vertices
    executeCommand(0, "sql", "CREATE VERTEX TYPE StudioSqlV");
    executeCommand(0, "sql", "INSERT INTO StudioSqlV SET name = 'A'");
    executeCommand(0, "sql", "INSERT INTO StudioSqlV SET name = 'B'");
    executeCommand(0, "sql", "CREATE EDGE TYPE StudioSqlE");
    executeCommand(0, "sql",
        "CREATE EDGE StudioSqlE FROM (SELECT FROM StudioSqlV WHERE name = 'A') TO (SELECT FROM StudioSqlV WHERE name = 'B')");

    final JSONObject result = studioResult("sql", "SELECT FROM StudioSqlV");

    assertThat(result.getJSONArray("records").length()).as("Should return 2 records").isEqualTo(2);
    assertThat(result.getJSONArray("vertices").length()).isEqualTo(2);
    assertThat(result.getJSONArray("edges").length()).as("Connecting edge surfaced").isEqualTo(1);
  }

  // Regression: #4689 the same over the Studio serializer, which iterates the edges of every returned vertex
  @Test
  void matchReturnVertexWithDanglingEdgeDoesNotReturn500() throws Exception {
    // Reproduces the customer's exact failure: a vertex whose edge segment still references an edge
    // record that no longer exists (dangling pointer). The studio serializer's "filter out not connected
    // edges" pass iterates every returned vertex's edges; loading the dangling edge under REPEATABLE_READ
    // isolation used to skip it and then throw NoSuchElementException, surfacing as an HTTP 500. Whole-record
    // queries (RETURN u / SELECT FROM) hit this; scalar projections (RETURN u{.*}) do not. See issue #4689.
    final Database db = getServerDatabase(0, getDatabaseName());

    final RID[] danglingEdge = new RID[1];
    db.transaction(() -> {
      db.command("sql", "CREATE VERTEX TYPE DanglingUser");
      db.command("sql", "CREATE EDGE TYPE DanglingKnows");
      final MutableVertex a = db.newVertex("DanglingUser").set("name", "A").save();
      final MutableVertex b = db.newVertex("DanglingUser").set("name", "B").save();
      final Edge e = a.newEdge("DanglingKnows", b);
      danglingEdge[0] = e.getIdentity();
    });

    // Create the dangling pointer: remove the edge RECORD at bucket level, bypassing graph cleanup.
    db.transaction(() ->
        TestHelper.deleteRecordAtLowLevel(db, danglingEdge[0]));

    final Database.TRANSACTION_ISOLATION_LEVEL previous = db.getTransactionIsolationLevel();
    db.setTransactionIsolationLevel(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    try {
      final JSONObject result = studioResult("opencypher", "MATCH (u:DanglingUser) RETURN u");

      assertThat(result.getJSONArray("records").length()).as("Both vertices should be returned").isEqualTo(2);
      assertThat(result.getJSONArray("vertices").length()).isEqualTo(2);
      assertThat(result.getJSONArray("edges").length())
          .as("The dangling edge must be filtered out, not surfaced, and must not cause an error").isZero();
    } finally {
      db.setTransactionIsolationLevel(previous);
    }
  }

  // Regression: #4689 the same over the Studio serializer, which iterates the edges of every returned vertex
  @Test
  void matchReturnVertexWithBidirectionalEdges() throws Exception {
    // Two vertices with edges in BOTH directions - exercises both the OUT and IN edge loops
    executeCommand(0, "opencypher", "CREATE (u:StudioBidir {name: 'X'})");
    executeCommand(0, "opencypher", "CREATE (u:StudioBidir {name: 'Y'})");
    executeCommand(0, "sql", "CREATE EDGE TYPE Fwd");
    executeCommand(0, "sql", "CREATE EDGE TYPE Bwd");
    executeCommand(0, "sql",
        "CREATE EDGE Fwd FROM (SELECT FROM StudioBidir WHERE name = 'X') TO (SELECT FROM StudioBidir WHERE name = 'Y')");
    executeCommand(0, "sql",
        "CREATE EDGE Bwd FROM (SELECT FROM StudioBidir WHERE name = 'Y') TO (SELECT FROM StudioBidir WHERE name = 'X')");

    final JSONObject result = studioResult("opencypher", "MATCH (u:StudioBidir) RETURN u");

    assertThat(result.getJSONArray("records").length()).isEqualTo(2);
    assertThat(result.getJSONArray("vertices").length()).isEqualTo(2);
    assertThat(result.getJSONArray("edges").length()).as("Both directed edges should be surfaced").isEqualTo(2);
  }

  /*
   * Regression test for ops issue #738 (Locstat / Johan Hoogenboezem).
   * <p>
   * A query that returns a single EDGE over the Studio serializer must surface only that edge in the
   * graph, not every sibling edge that happens to run between the same two endpoints. The two Account
   * vertices below are connected by many TRANSFER edges; a query pinned to one transaction returns a
   * single record but used to render all of them in the Graph view, because the "filter out not
   * connected edges" pass added every edge between two included vertices - and an edge row adds both of
   * its endpoints to that set.
   */
  // Regression: ops #738 a single returned edge must not surface its sibling edges in the graph view
  @Test
  void selectSingleEdgeDoesNotSurfaceSiblingEdges() throws Exception {
    // Schema modelled on the client's: Account vertices, TRANSFER edges keyed by (transactionId, date).
    executeCommand(0, "sql", "CREATE VERTEX TYPE Account");
    executeCommand(0, "sql", "CREATE PROPERTY Account.accountNumber STRING");
    executeCommand(0, "sql", "CREATE EDGE TYPE TRANSFER");
    executeCommand(0, "sql", "CREATE PROPERTY TRANSFER.transactionId STRING");
    executeCommand(0, "sql", "CREATE PROPERTY TRANSFER.date DATE");
    executeCommand(0, "sql", "CREATE INDEX TRANSFER_PK ON TRANSFER (transactionId, date) UNIQUE");

    executeCommand(0, "sql", "INSERT INTO Account SET accountNumber = 'from'");
    executeCommand(0, "sql", "INSERT INTO Account SET accountNumber = 'to'");

    // 11 TRANSFER edges between the SAME two accounts, one per day, distinct transactionIds.
    final int edgeCount = 11;
    for (int i = 0; i < edgeCount; i++) {
      final String txId = "745298" + (10 + i);
      final String date = String.format("2026-02-%02d", 1 + i);
      executeCommand(0, "sql", "CREATE EDGE TRANSFER "
          + "FROM (SELECT FROM Account WHERE accountNumber = 'from') "
          + "TO (SELECT FROM Account WHERE accountNumber = 'to') "
          + "SET transactionId = '" + txId + "', date = date('" + date + "', 'yyyy-MM-dd')");
    }

    // Client's query: pinned to one (transactionId, date) via the unique index.
    final JSONObject response = executeCommand(0, "sql",
        "SELECT FROM TRANSFER WHERE transactionId = '74529810' AND date = date('2026-02-01', 'yyyy-MM-dd')");
    assertThat(response).isNotNull();
    final JSONObject result = response.getJSONObject("result");

    assertThat(result.getJSONArray("records").length()).as("Exactly one TRANSFER matches the predicate").isEqualTo(1);
    assertThat(result.getJSONArray("edges").length())
        .as("Only the queried edge must be in the graph, not the %d sibling TRANSFER edges", edgeCount).isEqualTo(1);
    // Both endpoints of the single edge are still expected in the graph.
    assertThat(result.getJSONArray("vertices").length()).as("The two endpoints of the queried edge").isEqualTo(2);
  }
}
