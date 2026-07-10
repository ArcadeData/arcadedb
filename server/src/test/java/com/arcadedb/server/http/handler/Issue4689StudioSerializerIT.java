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
import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #4689 over the Studio serializer (serializer=studio), which builds the
 * full graph (vertices + edges + records) and runs the "filter out not connected edges" loop that
 * iterates getEdges() on every returned vertex - the studio-only path HTTP/Bolt clients never hit.
 */
class Issue4689StudioSerializerIT extends BaseGraphServerTest {

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
        db.getSchema().getBucketById(danglingEdge[0].getBucketId()).deleteRecord(danglingEdge[0]));

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
}
