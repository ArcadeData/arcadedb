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
package com.arcadedb.graph.olap;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10335: a vertex property removed after a Graph Analytical View was built kept its old value in the view until the
 * database was reopened. The vertex's new values were recorded from the names the record still has, so a removed property was
 * not in the map and the overlay, which applied the map over the old values, left the old one in place. A property set to null
 * stays among the record's names, which is why that edit was right.
 * <p>
 * The values a vertex is reported with are its complete state, so the overlay takes them as that: whatever the vertex no longer
 * has reads as missing, whatever the view's property filter is.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10335ViewRemovedPropertyTest extends TestHelper {
  private static final String ROWS = "MATCH (x:P)-[:K]->(y:P) RETURN x.id AS xi, y.id AS yi";

  private final RID[] rids = new RID[3];

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE P");
    database.command("sql", "CREATE EDGE TYPE K");
    // a path 2 -> 3 -> 4
    database.transaction(() -> {
      final MutableVertex[] vertices = new MutableVertex[3];
      for (int i = 0; i < 3; i++) {
        vertices[i] = database.newVertex("P").set("id", (long) (2 + i)).set("name", "n" + i).save();
        rids[i] = vertices[i].getIdentity();
      }
      vertices[0].newEdge("K", vertices[1]).save();
      vertices[1].newEdge("K", vertices[2]).save();
    });
  }

  @ParameterizedTest
  @ValueSource(strings = { "SYNCHRONOUS", "ASYNCHRONOUS" })
  void removalThroughTheJavaApi(final String mode) {
    final GraphAnalyticalView view = createView(mode, "PROPERTIES (id)");
    database.transaction(() -> removeId(rids[1]));
    awaitMerged(view);

    assertRemoved(view);
  }

  @ParameterizedTest
  @ValueSource(strings = { "SYNCHRONOUS", "ASYNCHRONOUS" })
  void removalThroughSql(final String mode) {
    final GraphAnalyticalView view = createView(mode, "PROPERTIES (id)");
    database.transaction(() -> database.command("sql", "UPDATE " + rids[1] + " REMOVE id").close());
    awaitMerged(view);

    assertRemoved(view);
  }

  @ParameterizedTest
  @ValueSource(strings = { "SYNCHRONOUS", "ASYNCHRONOUS" })
  void removalThroughCypher(final String mode) {
    final GraphAnalyticalView view = createView(mode, "PROPERTIES (id)");
    database.transaction(() -> database.command("opencypher", "MATCH (y:P) WHERE id(y) = '" + rids[1] + "' REMOVE y.id").close());
    awaitMerged(view);

    assertRemoved(view);
  }

  @ParameterizedTest
  @ValueSource(strings = { "SYNCHRONOUS", "ASYNCHRONOUS" })
  void removalOnAViewWithNoPropertyFilter(final String mode) {
    // a view that materialises every property has no list of names to walk to find the removed one
    final GraphAnalyticalView view = createView(mode, "");
    database.transaction(() -> removeId(rids[1]));
    awaitMerged(view);

    assertRemoved(view);
  }

  @Test
  void aRemovalKeepsTheVertexOtherProperties() {
    final GraphAnalyticalView view = createView("SYNCHRONOUS", "PROPERTIES (id, name)");
    database.transaction(() -> removeId(rids[1]));

    assertThat(view.getProperty(view.getNodeId(rids[1]), "id")).isNull();
    assertThat(view.getProperty(view.getNodeId(rids[1]), "name")).isEqualTo("n1");
    // the neighbours are untouched
    assertThat(view.getProperty(view.getNodeId(rids[0]), "id")).isEqualTo(2L);
    assertThat(view.getProperty(view.getNodeId(rids[2]), "id")).isEqualTo(4L);
  }

  @Test
  void anUpdateOfOnePropertyKeepsTheOthersOfAVertexWithSeveralInTheView() {
    final GraphAnalyticalView view = createView("SYNCHRONOUS", "PROPERTIES (id, name)");
    database.transaction(() -> {
      final MutableVertex vertex = rids[1].asVertex().modify();
      vertex.set("name", "renamed");
      vertex.save();
    });

    assertThat(view.getProperty(view.getNodeId(rids[1]), "id")).isEqualTo(3L);
    assertThat(view.getProperty(view.getNodeId(rids[1]), "name")).isEqualTo("renamed");
    assertThat(rows()).containsExactlyInAnyOrder("[xi=2, yi=3]", "[xi=3, yi=4]");
  }

  @Test
  void aRemovedPropertyCanBeSetAgain() {
    final GraphAnalyticalView view = createView("SYNCHRONOUS", "PROPERTIES (id)");
    database.transaction(() -> removeId(rids[1]));
    assertThat(view.getProperty(view.getNodeId(rids[1]), "id")).isNull();

    database.transaction(() -> rids[1].asVertex().modify().set("id", 10L).save());
    assertThat(view.getProperty(view.getNodeId(rids[1]), "id")).isEqualTo(10L);
    assertThat(rows()).containsExactlyInAnyOrder("[xi=2, yi=10]", "[xi=10, yi=4]");
  }

  @Test
  void aRemovalOnAVertexCreatedAfterTheBuild() {
    final GraphAnalyticalView view = createView("SYNCHRONOUS", "PROPERTIES (id)");
    final RID[] created = new RID[1];
    database.transaction(() -> {
      final MutableVertex v = database.newVertex("P").set("id", 7L).set("name", "late").save();
      created[0] = v.getIdentity();
      rids[2].asVertex().modify().newEdge("K", v).save();
    });
    assertThat(view.getProperty(view.getNodeId(created[0]), "id")).isEqualTo(7L);

    database.transaction(() -> removeId(created[0]));

    assertThat(view.getProperty(view.getNodeId(created[0]), "id")).as("a vertex of the overlay").isNull();
    assertThat(view.getProperty(view.getNodeId(created[0]), "name")).isEqualTo("late");
    assertThat(rows()).containsExactlyInAnyOrder("[xi=2, yi=3]", "[xi=3, yi=4]", "[xi=4, yi=null]");
  }

  @Test
  void aRemovalSurvivesAnotherUpdateOfTheSameVertex() {
    final GraphAnalyticalView view = createView("SYNCHRONOUS", "PROPERTIES (id)");
    database.transaction(() -> removeId(rids[1]));
    database.transaction(() -> rids[1].asVertex().modify().set("name", "renamed").save());

    assertThat(view.getProperty(view.getNodeId(rids[1]), "id")).isNull();
    assertThat(view.getProperty(view.getNodeId(rids[1]), "name")).isEqualTo("renamed");
  }

  @Test
  void aPropertyOutsideTheFilterRemovedLeavesTheFilteredOnesAlone() {
    final GraphAnalyticalView view = createView("SYNCHRONOUS", "PROPERTIES (id)");
    database.transaction(() -> {
      final MutableVertex vertex = rids[1].asVertex().modify();
      vertex.remove("name");
      vertex.save();
    });

    assertThat(view.getProperty(view.getNodeId(rids[1]), "id")).isEqualTo(3L);
    assertThat(view.getProperty(view.getNodeId(rids[1]), "name")).isNull();
    assertThat(rows()).containsExactlyInAnyOrder("[xi=2, yi=3]", "[xi=3, yi=4]");
  }

  /** The path 2 -> 3 -> 4 after the middle vertex lost its id: [2 -> null] and [null -> 4], and no pair compares as less than. */
  private void assertRemoved(final GraphAnalyticalView view) {
    assertThat(view.getProperty(view.getNodeId(rids[1]), "id")).as("the view's own value").isNull();
    assertThat(rows()).containsExactlyInAnyOrder("[xi=2, yi=null]", "[xi=null, yi=4]");

    try (final ResultSet rs = database.query("opencypher", "MATCH (x:P)-[:K]->(y:P) WHERE x.id < y.id RETURN count(*) AS n")) {
      assertThat(rs.next().<Number>getProperty("n").longValue()).isZero();
    }
    try (final ResultSet rs = database.query("opencypher", "MATCH (y:P)<-[:K]-(:P) WHERE y.id = 3 RETURN count(*) AS n")) {
      assertThat(rs.next().<Number>getProperty("n").longValue()).isZero();
    }
  }

  private void removeId(final RID rid) {
    final MutableVertex vertex = rid.asVertex().modify();
    vertex.remove("id");
    vertex.save();
  }

  private List<String> rows() {
    final List<String> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", ROWS)) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        rows.add("[xi=" + r.getProperty("xi") + ", yi=" + r.getProperty("yi") + "]");
      }
    }
    Collections.sort(rows);
    return rows;
  }

  private GraphAnalyticalView createView(final String mode, final String properties) {
    database.command("sql",
        "CREATE GRAPH ANALYTICAL VIEW gav VERTEX TYPES (P) EDGE TYPES (K) " + properties + " UPDATE MODE " + mode).close();
    final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "gav");
    assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
    return view;
  }

  /** An ASYNCHRONOUS view merges a commit off the committing thread: wait until it has. */
  private void awaitMerged(final GraphAnalyticalView view) {
    assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
  }
}
