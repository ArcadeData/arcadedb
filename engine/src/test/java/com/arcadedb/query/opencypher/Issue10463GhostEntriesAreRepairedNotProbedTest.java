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
package com.arcadedb.query.opencypher;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.GhostEdgeReporter;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10463, what is decided for the walks that answer from the adjacency entries and read no edge: the fixed-length anonymous hop
 * ({@code MATCH (a)-[:T]->(b)}), {@code getVertices()} and SQL {@code out()}/{@code in()}, and every operator that counts or joins the
 * entries (the degree of a vertex, the fused {@code count(*)} chains, the analytical views built from the adjacency). They follow a
 * dangling entry - one whose edge record is gone - because that is what answering from the entries means: probing every entry would
 * make the count of a vertex's edges a walk of its edge pages, and a cold graph would pay one edge page per entry on the commonest
 * expand path. The variable-length walks skip and report a ghost (#10149), where the entry is probed once per vertex reached.
 * <p>
 * The anomaly itself is repaired, not worked around at every read: {@code CHECK DATABASE ... FIX} prunes the dangling entries, after
 * which every walk agrees. This pins that sequence, so the two halves of the contract cannot drift apart.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10463GhostEntriesAreRepairedNotProbedTest extends TestHelper {

  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    // The fixture leaves dangling adjacency entries on purpose, and the test repairs them
    return false;
  }

  @Override
  protected void beginTest() {
    database.getSchema().createVertexType("G");
    database.getSchema().createEdgeType("LINK");
    database.getSchema().buildEdgeType().withName("UNI").withBidirectional(false).create();
  }

  @Test
  void theWalksThatAnswerFromTheEntriesFollowTheGhostUntilTheDatabaseIsChecked() {
    ghostEdge("LINK");

    // answered from the entries: the ghost is followed, as the degree of the vertex counts it
    assertThat(names("MATCH (a:G {name: 'ga'})-[:LINK]->(b:G) RETURN b.name AS name")).containsExactly("gb", "gc");
    assertThat(names("SELECT out('LINK').name AS name FROM G WHERE name = 'ga'")).contains("gb", "gc");
    assertThat(degree("ga")).isEqualTo(2L);

    // the walks that skip a ghost agree with the other half of the contract: a variable-length hop, and a read relationship
    final long before = GhostEdgeReporter.getTotalSkipped();
    assertThat(names("MATCH (a:G {name: 'ga'})-[:LINK*1..1]->(b:G) RETURN b.name AS name")).containsExactly("gc");
    // the relationship is read: a variable nothing reads is an anonymous hop, which follows the entry
    assertThat(names("MATCH (a:G {name: 'ga'})-[r:LINK]->(b:G) RETURN b.name AS name, r.tag AS tag")).containsExactly("gc");
    assertThat(GhostEdgeReporter.getTotalSkipped()).as("the ghost is reported by the walks that skip it").isGreaterThan(before);
  }

  @Test
  void checkDatabaseFixPrunesTheDanglingEntriesAndEveryWalkAgrees() {
    ghostEdge("LINK");

    final Result report = fix();
    assertThat(((Number) report.getProperty("prunedDanglingEntries")).longValue()).as("one entry in each list of the edge")
        .isEqualTo(2L);

    for (final String hop : new String[] { "[:LINK]", "[:LINK*1..1]", "[:LINK*1..3]", "[r:LINK]" })
      assertThat(names("MATCH (a:G {name: 'ga'})-" + hop + "->(b:G) RETURN b.name AS name")).as(hop).containsExactly("gc");
    assertThat(names("SELECT out('LINK').name AS name FROM G WHERE name = 'ga'")).containsExactly("gc");
    assertThat(degree("ga")).isEqualTo(1L);
  }

  /** A unidirectional type has the entry in the outgoing list of its source only, and the repair finds that one too. */
  @Test
  void theEntryOfAUnidirectionalTypeIsPrunedToo() {
    ghostEdge("UNI");

    assertThat(names("MATCH (a:G {name: 'ga'})-[:UNI]->(b:G) RETURN b.name AS name")).containsExactly("gb", "gc");

    final Result report = fix();
    assertThat(((Number) report.getProperty("prunedDanglingEntries")).longValue()).isEqualTo(1L);

    for (final String hop : new String[] { "-[:UNI]->", "-[:UNI*1..1]->", "-[:UNI*1..3]-", "-[r:UNI]->" })
      assertThat(names("MATCH (a:G {name: 'ga'})" + hop + "(b:G) RETURN b.name AS name")).as(hop).containsExactly("gc");
  }

  /** The graph of the issue: {@code ga -T-> gb} and {@code ga -T-> gc}, with the record of the first edge removed. */
  private void ghostEdge(final String type) {
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("G").set("name", "ga").save();
      final MutableVertex b = database.newVertex("G").set("name", "gb").save();
      final MutableVertex c = database.newVertex("G").set("name", "gc").save();
      a.newEdge(type, b).set("tag", "ghost").save();
      a.newEdge(type, c).set("tag", "live").save();
    });

    final RID ghost;
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (:G {name: 'ga'})-[r:" + type + " {tag: 'ghost'}]->(:G {name: 'gb'}) RETURN r")) {
      ghost = ((Edge) rs.next().getProperty("r")).getIdentity();
    }
    database.transaction(() -> TestHelper.deleteRecordAtLowLevel(database, ghost));
  }

  private Result fix() {
    try (final ResultSet rs = database.command("sql", "CHECK DATABASE FIX")) {
      return rs.next();
    }
  }

  /** What the vertex says its degree is, which counts the entries of its list. */
  private long degree(final String name) {
    try (final ResultSet rs = database.query("sql", "SELECT FROM G WHERE name = ?", name)) {
      final Vertex vertex = rs.next().getVertex().orElseThrow();
      return vertex.countEdges(Vertex.DIRECTION.OUT, "LINK");
    }
  }

  /** The values of the first column of the query, one per row, in value order. */
  private List<String> names(final String query) {
    final List<String> names = new ArrayList<>();
    final String language = query.startsWith("SELECT") ? "sql" : "opencypher";
    try (final ResultSet rs = database.query(language, query)) {
      while (rs.hasNext()) {
        final Object value = rs.next().getProperty("name");
        if (value instanceof Iterable<?> many)
          for (final Object each : many)
            names.add(String.valueOf(each));
        else
          names.add(String.valueOf(value));
      }
    }
    names.sort(null);
    return names;
  }
}
