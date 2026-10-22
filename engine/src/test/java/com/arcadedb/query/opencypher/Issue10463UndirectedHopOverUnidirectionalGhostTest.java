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
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10463, the stored side of an undirected hop over a unidirectional edge type: the variable-length walks skip and report a
 * ghost (an adjacency entry whose edge record is gone, #10149), except here. An undirected hop over a unidirectional type is answered
 * by {@code IncomingEdgeLookup}, whose outgoing half is the stored list of the vertex read as vertices, with no edge RID to ask the
 * bucket about, so {@code MATCH (a)-[:U*1..3]-(b) RETURN DISTINCT b.name} named a vertex that
 * {@code MATCH (a)-[r:U*1..3]-(b) RETURN DISTINCT b.name, size(r)} did not, and reported nothing.
 * <p>
 * The entries of that list are answered with their edge RIDs now, and the walk probes them as it probes every other entry: once per
 * vertex it is about to reach.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10463UndirectedHopOverUnidirectionalGhostTest extends TestHelper {
  private static final String FROM_GA = "MATCH (a:G {name: 'ga'})";

  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    // The fixture leaves dangling adjacency entries on purpose
    return false;
  }

  @Override
  protected void beginTest() {
    database.getSchema().createVertexType("G");
    database.getSchema().buildEdgeType().withName("U").withBidirectional(false).create();
    database.getSchema().createEdgeType("B");
  }

  /** Every spelling of the undirected hop answers the vertices the live relationships lead to, whatever it returns. */
  @Test
  void everySpellingOfAnUndirectedHopAnswersTheSameVertices() {
    ghostEdge(false);

    for (final String hop : new String[] { ":U*1..3", ":U*1..1", ":U*1..", ":U*", "*1..3" }) {
      assertThat(rows(FROM_GA + "-[" + hop + "]-(b:G) RETURN DISTINCT b.name AS name")).as("DISTINCT, relationship unread: %s", hop)
          .containsExactly("gc");
      // The raw rows: the set of them would hide a ghost answered as a second row
      assertThat(rows(FROM_GA + "-[" + hop + "]-(b:G) RETURN b.name AS name")).as("not DISTINCT, relationship unread: %s", hop)
          .containsExactly("gc");
      assertThat(rows(FROM_GA + "-[" + hop + "]-(b:G) RETURN b.name AS name, count(*) AS c")).as("aggregated: %s", hop)
          .containsExactly("gc");
      assertThat(rows(FROM_GA + "-[r" + hop + "]-(b:G) RETURN DISTINCT b.name AS name, size(r) AS n")).as("relationship returned: %s",
          hop).containsExactly("gc");
      assertThat(rows("MATCH p = (a:G {name: 'ga'})-[" + hop + "]-(b:G) RETURN DISTINCT b.name AS name")).as("path returned: %s", hop)
          .containsExactly("gc");
    }
  }

  /** The lower bounds: 0 makes the start an end vertex, above 1 leaves the distinct-endpoint walk for the path walk. */
  @Test
  void theBoundsOfTheHopDoNotChangeTheAnswer() {
    ghostEdge(false);

    assertThat(rows(FROM_GA + "-[:U*0..3]-(b:G) RETURN DISTINCT b.name AS name")).containsExactly("ga", "gc");
    assertThat(rows(FROM_GA + "-[:U*0..3]-(b:G) RETURN b.name AS name")).containsExactly("ga", "gc");
    assertThat(rows(FROM_GA + "-[:U*2..3]-(b:G) RETURN DISTINCT b.name AS name")).isEmpty();
    assertThat(rows(FROM_GA + "-[:U*2..3]-(b:G) RETURN b.name AS name")).isEmpty();
  }

  /** The ghost is reported by every shape, not only by the one that loads the relationship. */
  @Test
  void everyShapeReportsTheGhost() {
    ghostEdge(false);

    for (final String query : new String[] {
        FROM_GA + "-[:U*1..3]-(b:G) RETURN DISTINCT b.name AS name",
        FROM_GA + "-[:U*1..3]-(b:G) RETURN b.name AS name",
        FROM_GA + "-[:U*0..3]-(b:G) RETURN DISTINCT b.name AS name",
        FROM_GA + "-[*1..3]-(b:G) RETURN DISTINCT b.name AS name",
        FROM_GA + "-[r:U*1..3]-(b:G) RETURN DISTINCT b.name AS name, size(r) AS n" }) {
      final long before = GhostEdgeReporter.getTotalSkipped();
      rows(query);
      assertThat(GhostEdgeReporter.getTotalSkipped()).as("the ghost is reported by: %s", query).isGreaterThan(before);
    }
  }

  /** A ghost does not spend the vertex it names: gb is also reachable through gc, one hop later than the ghost pretended. */
  @Test
  void aGhostDoesNotHideALiveRouteToTheSameVertex() {
    ghostEdge(true);

    for (final String distinct : new String[] { "DISTINCT ", "" }) {
      assertThat(rows(FROM_GA + "-[:U*1..3]-(b:G) RETURN " + distinct + "b.name AS name")).containsExactly("gb", "gc");
      assertThat(rows(FROM_GA + "-[:U*1..1]-(b:G) RETURN " + distinct + "b.name AS name")).containsExactly("gc");
      assertThat(rows(FROM_GA + "-[:U*2..2]-(b:G) RETURN " + distinct + "b.name AS name")).containsExactly("gb");
    }
  }

  /**
   * Two relationships to the same vertex, one of them a ghost: the vertex is reached through the live one, once, whichever of the two
   * entries the walk meets first (the newest entry of an adjacency list is walked first, so the ghost is created last to be met
   * first).
   */
  @ParameterizedTest
  @ValueSource(booleans = { true, false })
  void aLiveEntryToTheSameVertexReachesItWhicheverEntryTheWalkMeetsFirst(final boolean ghostMetFirst) {
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("G").set("name", "ga").save();
      final MutableVertex b = database.newVertex("G").set("name", "gb").save();
      a.newEdge("U", b).set("tag", ghostMetFirst ? "live" : "ghost").save();
      a.newEdge("U", b).set("tag", ghostMetFirst ? "ghost" : "live").save();
    });
    deleteTheGhost();

    // The raw rows, not their set: a walk that followed both entries answers the vertex twice
    for (final String distinct : new String[] { "DISTINCT ", "" })
      for (final String hop : new String[] { "*1..1", "*1..3", ":U*1..2" })
        assertThat(rows(FROM_GA + "-[" + hop + "]-(b:G) RETURN " + distinct + "b.name AS name")).as("%s%s", distinct, hop)
            .containsExactly("gb");
  }

  /**
   * Found alongside: an undirected path walk asks each edge for its endpoints to tell a self-loop, which loads the record of an edge
   * the adjacency entry did not carry the endpoints of, and a ghost failed the whole walk with a {@code RecordNotFoundException}
   * instead of being skipped and reported. Over a bidirectional type too, from either end of the ghost.
   */
  @Test
  void anUndirectedPathWalkOverABidirectionalTypeSkipsTheGhostInsteadOfFailing() {
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("G").set("name", "ga").save();
      final MutableVertex b = database.newVertex("G").set("name", "gb").save();
      final MutableVertex c = database.newVertex("G").set("name", "gc").save();
      a.newEdge("B", b).set("tag", "ghost").save();
      a.newEdge("B", c).set("tag", "live").save();
    });
    final RID ghost;
    try (final ResultSet rs = database.query("opencypher", "MATCH (:G {name: 'ga'})-[r:B {tag: 'ghost'}]->(:G {name: 'gb'}) RETURN r")) {
      ghost = ((Edge) rs.next().getProperty("r")).getIdentity();
    }
    database.transaction(() -> TestHelper.deleteRecordAtLowLevel(database, ghost));

    final long before = GhostEdgeReporter.getTotalSkipped();
    assertThat(rows("MATCH (a:G {name: 'ga'})-[r:B*1..3]-(b:G) RETURN b.name AS name")).containsExactly("gc");
    assertThat(rows("MATCH (a:G {name: 'gb'})-[r:B*1..3]-(b:G) RETURN b.name AS name")).isEmpty();
    assertThat(rows("MATCH p = (a:G {name: 'ga'})-[:B*1..3]-(b:G) RETURN b.name AS name")).containsExactly("gc");
    assertThat(GhostEdgeReporter.getTotalSkipped()).as("the ghost is reported").isGreaterThan(before);
  }

  /** Nothing changes on a graph without a ghost: regular edges, a cycle, and the edges of the open transaction. */
  @Test
  void aCleanGraphIsWalkedAsBefore() {
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("G").set("name", "ga").save();
      final MutableVertex b = database.newVertex("G").set("name", "gb").save();
      final MutableVertex c = database.newVertex("G").set("name", "gc").save();
      final MutableVertex d = database.newVertex("G").set("name", "gd").save();
      a.newEdge("U", b).set("tag", "ab").save();
      a.newEdge("U", c).set("tag", "ac").save();
      c.newEdge("U", d).set("tag", "cd").save();
      d.newEdge("U", a).set("tag", "da").save();
    });

    final long before = GhostEdgeReporter.getTotalSkipped();
    for (final String distinct : new String[] { "DISTINCT ", "" }) {
      assertThat(rows(FROM_GA + "-[:U*1..1]-(b:G) RETURN " + distinct + "b.name AS name")).containsExactly("gb", "gc", "gd");
      // the cycle ga, gc, gd, ga is three distinct relationships long: it ends in ga
      assertThat(rows(FROM_GA + "-[:U*1..3]-(b:G) RETURN DISTINCT b.name AS name")).containsExactly("ga", "gb", "gc", "gd");
    }
    assertThat(GhostEdgeReporter.getTotalSkipped()).as("no ghost to report").isEqualTo(before);

    // an edge the open transaction wrote is live to the walk
    database.transaction(() -> {
      database.newVertex("G").set("name", "ge").save();
      database.command("opencypher", "MATCH (a:G {name: 'ga'}), (e:G {name: 'ge'}) CREATE (a)-[:U {tag: 'ae'}]->(e)").close();
      assertThat(rows(FROM_GA + "-[:U*1..1]-(b:G) RETURN DISTINCT b.name AS name")).contains("ge");
    });
  }

  /**
   * The graph of the issue: {@code ga -U-> gb} and {@code ga -U-> gc}, with the record of the first edge removed and the entry naming it
   * left behind in the outgoing list of ga (a unidirectional type has no incoming entry). With {@code liveRouteToB} there is also a
   * live {@code gc -U-> gb}.
   */
  private void ghostEdge(final boolean liveRouteToB) {
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("G").set("name", "ga").save();
      final MutableVertex b = database.newVertex("G").set("name", "gb").save();
      final MutableVertex c = database.newVertex("G").set("name", "gc").save();
      a.newEdge("U", b).set("tag", "ghost").save();
      a.newEdge("U", c).set("tag", "live-ac").save();
      if (liveRouteToB)
        c.newEdge("U", b).set("tag", "live-cb").save();
    });

    deleteTheGhost();
  }

  /** Only the edge record tagged "ghost" goes: the entry naming it in the adjacency list of its source stays behind. */
  private void deleteTheGhost() {
    final RID ghost;
    try (final ResultSet rs = database.query("opencypher", "MATCH (:G {name: 'ga'})-[r:U {tag: 'ghost'}]->(:G {name: 'gb'}) RETURN r")) {
      ghost = ((Edge) rs.next().getProperty("r")).getIdentity();
    }
    database.transaction(() -> TestHelper.deleteRecordAtLowLevel(database, ghost));
  }

  /** The values of the first column of the query, one per row, in value order: a row answered twice shows twice. */
  private List<String> rows(final String query) {
    final List<String> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext())
        rows.add(String.valueOf(rs.next().<Object>getProperty("name")));
    }
    rows.sort(null);
    return rows;
  }
}
