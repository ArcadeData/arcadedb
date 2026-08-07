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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Comparator;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for GitHub issue #5746.
 * <p>
 * With a Graph Analytical View active, an openCypher multi-hop pattern anchored by an inline
 * property map ({@code MATCH (p:Person {id: 0})-[:KNOWS]->(a)-[:KNOWS]->(b)}) returned nothing.
 * Two independent defects in the fused-chain path combined:
 * <ol>
 *   <li>The optimizer decided which variables to materialize from WHERE/WITH/RETURN only, so a
 *   filter it then pushed INTO the chain could read a variable no slot was allocated for. An inline
 *   property map becomes exactly such a filter, so the predicate never held and every row was
 *   dropped.</li>
 *   <li>The chain's output name array always reserved slot 0 for the source variable, while the
 *   value array only wrote that slot when the source was materialized, shifting every remaining
 *   variable one slot so the last one read back null.</li>
 * </ol>
 * <p>
 * A view is a performance feature, so the oracle here is that every query returns the same rows
 * with and without the view. Each scenario is asserted both ways.
 */
class CypherGAVFusedChainIssue5746Test {
  private Database database;
  private static final String DB_PATH = "./target/databases/cypher-issue-5746";

  /** 0 -&gt; 1, 0 -&gt; 2, 1 -&gt; 3, 2 -&gt; 4, 1 -&gt; 5, 3 -&gt; 0. Two hops out of 0 reach {3, 4, 5}. */
  private static final int[][] KNOWS = { { 0, 1 }, { 0, 2 }, { 1, 3 }, { 2, 4 }, { 1, 5 }, { 3, 0 } };

  /** A second edge type over the same topology, so mixed-type chains have the same expected answer. */
  private static final int[][] LIKES = KNOWS;

  @AfterEach
  void tearDown() {
    if (database != null && database.isOpen())
      database.drop();
  }

  private void setUp(final boolean withGav) {
    // Every test builds this database twice at the same path, so a run killed mid-test would
    // otherwise leave a directory that makes create() fail with already-exists.
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    if (factory.exists())
      factory.open().drop();
    database = factory.create();

    final Schema schema = database.getSchema();
    final VertexType person = schema.createVertexType("Person");
    person.createProperty("id", Integer.class);
    person.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "id");
    schema.createEdgeType("KNOWS");
    schema.createEdgeType("LIKES");

    database.transaction(() -> {
      final MutableVertex[] people = new MutableVertex[6];
      for (int i = 0; i < people.length; i++)
        people[i] = database.newVertex("Person").set("id", i).save();
      for (final int[] e : KNOWS)
        people[e[0]].newEdge("KNOWS", people[e[1]]).save();
      for (final int[] e : LIKES)
        people[e[0]].newEdge("LIKES", people[e[1]]).save();
    });

    if (withGav)
      database.command("sql", "CREATE GRAPH ANALYTICAL VIEW v VERTEX TYPES (Person) "
          + "EDGE TYPES (KNOWS, LIKES) PROPERTIES (id) UPDATE MODE SYNCHRONOUS");
  }

  /**
   * Sorted by string form so an unbound variable shows up as a {@code null} element in the assertion
   * diff instead of blowing up the comparator - the symptom under test is exactly a null binding.
   */
  private TreeSet<Object> collect(final String query, final String column) {
    final TreeSet<Object> out = sortedSet();
    try (final ResultSet rs = database.query("cypher", query)) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        out.add(r.getProperty(column));
      }
    }
    return out;
  }

  private static TreeSet<Object> sortedSet() {
    return new TreeSet<>(Comparator.comparing(String::valueOf));
  }

  private Object scalar(final String query, final String column) {
    try (final ResultSet rs = database.query("cypher", query)) {
      return rs.hasNext() ? rs.next().getProperty(column) : null;
    }
  }

  private static TreeSet<Object> ids(final int... values) {
    final TreeSet<Object> out = sortedSet();
    for (final int v : values)
      out.add(v);
    return out;
  }

  /** Runs {@code scenario} against a database built with and without the view, asserting both. */
  private void withAndWithoutView(final Runnable scenario) {
    setUp(false);
    scenario.run();
    tearDown();
    setUp(true);
    scenario.run();
  }

  // ---- the reported shape: inline start filter + 2-hop chain ---------------------------------

  @Test
  void inlineFilterTwoHopProjectsEndOfChain() {
    withAndWithoutView(() -> assertThat(collect(
        "MATCH (p:Person {id: 0})-[:KNOWS]->(a:Person)-[:KNOWS]->(b:Person) RETURN DISTINCT b.id AS id", "id"))
        .isEqualTo(ids(3, 4, 5)));
  }

  @Test
  void inlineFilterTwoHopProjectsMiddleOfChain() {
    withAndWithoutView(() -> assertThat(collect(
        "MATCH (p:Person {id: 0})-[:KNOWS]->(a:Person)-[:KNOWS]->(b:Person) RETURN DISTINCT a.id AS id", "id"))
        .isEqualTo(ids(1, 2)));
  }

  @Test
  void inlineFilterTwoHopProjectsStartOfChain() {
    withAndWithoutView(() -> assertThat(collect(
        "MATCH (p:Person {id: 0})-[:KNOWS]->(a:Person)-[:KNOWS]->(b:Person) RETURN DISTINCT p.id AS id", "id"))
        .isEqualTo(ids(0)));
  }

  @Test
  void inlineFilterTwoHopProjectsBothEnds() {
    withAndWithoutView(() -> {
      try (final ResultSet rs = database.query("cypher",
          "MATCH (p:Person {id: 0})-[:KNOWS]->(a:Person)-[:KNOWS]->(b:Person) RETURN p.id AS pid, b.id AS bid")) {
        final TreeSet<Object> pairs = sortedSet();
        while (rs.hasNext()) {
          final Result r = rs.next();
          pairs.add(r.getProperty("pid") + "->" + r.getProperty("bid"));
        }
        final TreeSet<Object> expected = sortedSet();
        expected.add("0->3");
        expected.add("0->4");
        expected.add("0->5");
        assertThat(pairs).isEqualTo(expected);
      }
    });
  }

  @Test
  void inlineFilterTwoHopCountStar() {
    withAndWithoutView(() -> assertThat(scalar(
        "MATCH (p:Person {id: 0})-[:KNOWS]->(a:Person)-[:KNOWS]->(b:Person) RETURN count(*) AS c", "c"))
        .isEqualTo(3L));
  }

  @Test
  void inlineFilterTwoHopCountDistinct() {
    withAndWithoutView(() -> assertThat(scalar(
        "MATCH (p:Person {id: 0})-[:KNOWS]->(a:Person)-[:KNOWS]->(b:Person) RETURN count(DISTINCT b) AS c", "c"))
        .isEqualTo(3L));
  }

  // ---- the same pattern split across two MATCH clauses ---------------------------------------

  @Test
  void inlineFilterSplitAcrossTwoMatchClauses() {
    withAndWithoutView(() -> assertThat(collect(
        "MATCH (p:Person {id: 0})-[:KNOWS]->(a:Person) MATCH (a)-[:KNOWS]->(b:Person) RETURN DISTINCT b.id AS id",
        "id")).isEqualTo(ids(3, 4, 5)));
  }

  // ---- mixed edge types ----------------------------------------------------------------------

  @Test
  void inlineFilterTwoHopMixedEdgeTypes() {
    withAndWithoutView(() -> assertThat(collect(
        "MATCH (p:Person {id: 0})-[:KNOWS]->(a:Person)-[:LIKES]->(b:Person) RETURN DISTINCT b.id AS id", "id"))
        .isEqualTo(ids(3, 4, 5)));
  }

  // ---- three hops ------------------------------------------------------------------------------

  @Test
  void inlineFilterThreeHopProjectsEndOfChain() {
    // 0 -> {1,2} -> {3,4,5} -> {0} (only 3 -> 0 exists among the second-hop targets).
    withAndWithoutView(() -> assertThat(collect(
        "MATCH (p:Person {id: 0})-[:KNOWS]->(a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) "
            + "RETURN DISTINCT c.id AS id", "id")).isEqualTo(ids(0)));
  }

  // ---- the documented workaround must keep working --------------------------------------------

  @Test
  void whereClauseFormTwoHop() {
    withAndWithoutView(() -> assertThat(collect(
        "MATCH (p:Person)-[:KNOWS]->(a:Person)-[:KNOWS]->(b:Person) WHERE p.id = 0 RETURN DISTINCT b.id AS id",
        "id")).isEqualTo(ids(3, 4, 5)));
  }

  // ---- unfiltered multi-hop was already correct; keep it that way -------------------------------

  @Test
  void unfilteredTwoHopProjectsEndOfChain() {
    // No filter anywhere, so nothing forces the source to be materialized: this shape isolates the
    // output-slot alignment inside the fused chain from the pushed-filter materialization.
    // 2-hop paths: 0->1->3, 0->1->5, 0->2->4, 1->3->0, 3->0->1, 3->0->2.
    withAndWithoutView(() -> assertThat(collect(
        "MATCH (p:Person)-[:KNOWS]->(a:Person)-[:KNOWS]->(b:Person) RETURN DISTINCT b.id AS id", "id"))
        .isEqualTo(ids(0, 1, 2, 3, 4, 5)));
  }

  @Test
  void unfilteredTwoHopCountStar() {
    // Every 2-hop path in the graph: 0->1->3, 0->1->5, 0->2->4, 1->3->0, 3->0->1, 3->0->2 = 6.
    withAndWithoutView(() -> assertThat(scalar(
        "MATCH (p:Person)-[:KNOWS]->(a:Person)-[:KNOWS]->(b:Person) RETURN count(*) AS c", "c"))
        .isEqualTo(6L));
  }
}
