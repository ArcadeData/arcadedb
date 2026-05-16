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
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.traversal.TraversalPath;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for named paths with variable-length relationships.
 * Note: Variable-length path queries currently have a duplication bug (pre-existing issue).
 * These tests verify that path variables ARE being stored correctly.
 */
public class OpenCypherVariableLengthPathTest {
  private Database database;
  private Vertex  alice, bob, charlie;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/testopencypher-varlen-path").create();

    // Create schema
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("KNOWS");

    // Use low-level API to avoid any Cypher CREATE issues
    database.transaction(() -> {
      alice = database.newVertex("Person").set("name", "Alice").save();
      bob = database.newVertex("Person").set("name", "Bob").save();
      charlie = database.newVertex("Person").set("name", "Charlie").save();

      ((MutableVertex) alice).newEdge("KNOWS", bob, true, (Object[]) null).save();
      ((MutableVertex) bob).newEdge("KNOWS", charlie, true, (Object[]) null).save();
    });
  }

  @AfterEach
  void cleanup() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void namedPathVariableLengthStoresPath() {
    // Test that path variable IS being stored (even if there are duplicates)
    // This verifies the fix: passing pathVariable instead of relVar to ExpandPathStep
    final ResultSet result = database.query("opencypher",
        """
        MATCH p = (a:Person {name: 'Alice'})-[:KNOWS*1..2]->(b:Person) \
        RETURN p AS path LIMIT 1""");

    assertThat(result.hasNext()).as("Should have at least one result").isTrue();
    final Result row = result.next();

    final Object pathObj = row.getProperty("path");
    assertThat(pathObj).as("Path variable should not be null - this was the bug!").isNotNull();
    assertThat(pathObj).as("Path should be a TraversalPath").isInstanceOf(TraversalPath.class);

    final TraversalPath path = (TraversalPath) pathObj;
    assertThat(path.length() >= 1 && path.length() <= 2).as("Path length should be 1 or 2").isTrue();
    assertThat(path.getStartVertex().get("name")).isEqualTo("Alice");

    result.close();
  }

  @Test
  void namedPathSingleHop() {
    // Single-hop paths should work correctly (no duplication bug)
    final ResultSet result = database.query("opencypher",
        """
        MATCH p = (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person) \
        RETURN p AS path""");

    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();

    final TraversalPath path = (TraversalPath) row.getProperty("path");
    assertThat(path).isNotNull();
    assertThat(path.length()).isEqualTo(1);
    assertThat(path.getStartVertex().get("name")).isEqualTo("Alice");
    assertThat(path.getEndVertex().get("name")).isEqualTo("Bob");

    assertThat(result.hasNext()).as("Single-hop should return exactly 1 result").isFalse();
    result.close();
  }

  @Test
  void vlpPropertyPredicate() {
    // Create a separate database for this test with the TCK scenario
    final Database db2 = new DatabaseFactory("./target/databases/testopencypher-vlp-prop").create();
    try {
      db2.transaction(() -> {
        db2.command("opencypher",
            """
            CREATE (a:Artist:A), (b:Artist:B), (c:Artist:C) \
            CREATE (a)-[:WORKED_WITH {year: 1987}]->(b), \
            (b)-[:WORKED_WITH {year: 1988}]->(c)""");
      });

      db2.transaction(() -> {
        // First: check all artists exist
        final ResultSet rs1 = db2.command("opencypher", "MATCH (n:Artist) RETURN n");
        int count = 0;
        while (rs1.hasNext()) {
          rs1.next();
          count++;
        }
        assertThat(count).as("Should have 3 Artist nodes").isEqualTo(3);

        // Second: VLP without property filter
        final ResultSet rs2 = db2.command("opencypher", "MATCH (a:Artist)-[:WORKED_WITH*]->(b) RETURN a, b");
        int count2 = 0;
        while (rs2.hasNext()) {
          rs2.next();
          count2++;
        }
        assertThat(count2).as("VLP without property filter should find paths").isGreaterThan(0);

        // Third: VLP WITHOUT target label filter to isolate the issue
        final ResultSet rs3a = db2.command("opencypher",
            "MATCH (a:Artist)-[:WORKED_WITH* {year: 1988}]->(b) RETURN a, b");
        int count3a = 0;
        while (rs3a.hasNext()) {
          rs3a.next();
          count3a++;
        }

        // Fourth: VLP WITH target label only (no property filter)
        final ResultSet rs3b = db2.command("opencypher",
            "MATCH (a:Artist)-[:WORKED_WITH*]->(b:Artist) RETURN a, b");
        int count3b = 0;
        while (rs3b.hasNext()) {
          rs3b.next();
          count3b++;
        }

        // Fifth: VLP WITH property filter AND target label
        final ResultSet rs3 = db2.command("opencypher",
            "MATCH (a:Artist)-[:WORKED_WITH* {year: 1988}]->(b:Artist) RETURN a, b");
        int count3 = 0;
        while (rs3.hasNext()) {
          final Result r = rs3.next();
          count3++;
        }
        assertThat(count3a).as("VLP with property filter {year: 1988} without target label should find results").isGreaterThan(0);
        assertThat(count3b).as("VLP with target label :Artist but no property filter should find results").isGreaterThan(0);
        assertThat(count3).as("VLP with property filter {year: 1988} AND target label should find 1 result").isEqualTo(1);
      });
    } finally {
      db2.drop();
    }
  }

  @Test
  void collectRelThenVlpReturnsRows() {
    // Issue #3997: collect(r) carried via WITH drops all rows when a later MATCH uses VLP
    final List<Result> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher",
        """
        MATCH (a:Person {name: 'Alice'})-[r:KNOWS]->(b:Person)
        WITH a, collect(r) AS rels
        MATCH path = (a)-[:KNOWS*1..2]->(c:Person)
        RETURN length(path) AS len, size(rels) AS relationCount
        ORDER BY len""")) {
      rs.forEachRemaining(rows::add);
    }

    assertThat(rows).as("collect(r) + VLP should return 2 rows (len=1 and len=2)").hasSize(2);
    assertThat(rows.get(0).<Long>getProperty("len")).isEqualTo(1L);
    assertThat(rows.get(0).<Long>getProperty("relationCount")).isEqualTo(1L);
    assertThat(rows.get(1).<Long>getProperty("len")).isEqualTo(2L);
    assertThat(rows.get(1).<Long>getProperty("relationCount")).isEqualTo(1L);
  }

  @Test
  void collectDistinctRelThenVlpReturnsRows() {
    // Issue #3997: collect(DISTINCT r) carried via WITH also drops all rows
    final List<Result> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher",
        """
        MATCH (a:Person {name: 'Alice'})
        OPTIONAL MATCH (a)-[r:KNOWS]->(b:Person)
        WITH a, collect(DISTINCT r) AS rels
        MATCH path = (a)-[:KNOWS*1..2]->(c:Person)
        RETURN length(path) AS len, size(rels) AS relationCount
        ORDER BY len""")) {
      rs.forEachRemaining(rows::add);
    }

    assertThat(rows).as("collect(DISTINCT r) + VLP should return 2 rows").hasSize(2);
    assertThat(rows.get(0).<Long>getProperty("len")).isEqualTo(1L);
    assertThat(rows.get(1).<Long>getProperty("len")).isEqualTo(2L);
  }

  @Test
  void collectScalarThenVlpReturnsRows() {
    // Issue #3997 control case: collect(type(r)) should work (and does)
    final List<Result> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher",
        """
        MATCH (a:Person {name: 'Alice'})-[r:KNOWS]->(b:Person)
        WITH a, collect(type(r)) AS relTypes
        MATCH path = (a)-[:KNOWS*1..2]->(c:Person)
        RETURN length(path) AS len, relTypes
        ORDER BY len""")) {
      rs.forEachRemaining(rows::add);
    }

    assertThat(rows).as("collect(type(r)) + VLP control case should also return 2 rows").hasSize(2);
  }

  // ---------------------------------------------------------------------------
  // Helpers (consolidated from merged Issue test classes).
  // ---------------------------------------------------------------------------

  private static List<Result> collect(final ResultSet rs) {
    final List<Result> list = new ArrayList<>();
    while (rs.hasNext())
      list.add(rs.next());
    return list;
  }

  // ---------------------------------------------------------------------------
  // Issue #3931: variable-length pattern comprehension must respect hop bounds.
  // ---------------------------------------------------------------------------

  // Issue #3931: variable-length pattern comprehension with exact length 2 returns only path of exact length 2
  @SuppressWarnings("unchecked")
  @Test
  void variableLengthPatternComprehensionTwoLength() {
    final Database db = new DatabaseFactory("./target/databases/testopencypher-varlen-comp-3931").create();
    try {
      db.getSchema().createVertexType("VarLengthTest3");
      db.getSchema().createEdgeType("KNOWS");
      db.transaction(() -> db.command("opencypher",
          """
          CREATE (alice:VarLengthTest3 {name:'Alice'}), \
          (bob:VarLengthTest3 {name:'Bob'}), \
          (charlie:VarLengthTest3 {name:'Charlie'}), \
          (alice)-[:KNOWS]->(bob), \
          (bob)-[:KNOWS]->(charlie), \
          (alice)-[:KNOWS]->(charlie)"""));

      // Only the path alice->bob->charlie has exact length 2, so f = Charlie
      final ResultSet resultSet = db.query("opencypher",
          """
          MATCH (a:VarLengthTest3 {name:'Alice'}) \
          RETURN [(a)-[:KNOWS*2..2]->(f:VarLengthTest3) | f.name] AS result""");

      assertThat(resultSet.hasNext()).isTrue();
      final Result r = resultSet.next();
      final List<Object> list = (List<Object>) r.getProperty("result");
      assertThat(list).containsExactly("Charlie");
      assertThat(resultSet.hasNext()).isFalse();
    } finally {
      db.drop();
    }
  }

  // Issue #3931: variable-length pattern comprehension with length 1..1 returns direct neighbors
  @SuppressWarnings("unchecked")
  @Test
  void variableLengthPatternComprehensionOneLength() {
    final Database db = new DatabaseFactory("./target/databases/testopencypher-varlen-comp-3931").create();
    try {
      db.getSchema().createVertexType("VarLengthTest3");
      db.getSchema().createEdgeType("KNOWS");
      db.transaction(() -> db.command("opencypher",
          """
          CREATE (alice:VarLengthTest3 {name:'Alice'}), \
          (bob:VarLengthTest3 {name:'Bob'}), \
          (charlie:VarLengthTest3 {name:'Charlie'}), \
          (alice)-[:KNOWS]->(bob), \
          (bob)-[:KNOWS]->(charlie), \
          (alice)-[:KNOWS]->(charlie)"""));

      // GitHub issue #3930: within a pattern comprehension, the Cypher spec does not
      // mandate a specific iteration order. Neo4j happens to return insertion order
      // (["Bob", "Charlie"]); ArcadeDB walks its edge linked-list from newest to oldest
      // so the order is reversed. The outer ORDER BY in the issue's query sorts rows
      // (there is only one here), not the elements inside the list. Both orders are
      // valid Cypher results - we only assert the set membership is correct.
      final ResultSet resultSet = db.query("opencypher",
          """
          MATCH (a:VarLengthTest3 {name:'Alice'}) \
          RETURN [(a)-[:KNOWS*1..1]->(f:VarLengthTest3) | f.name] AS result ORDER BY result""");

      assertThat(resultSet.hasNext()).isTrue();
      final Result r = resultSet.next();
      final List<Object> list = (List<Object>) r.getProperty("result");
      assertThat(list).containsExactlyInAnyOrder("Bob", "Charlie");
      assertThat(resultSet.hasNext()).isFalse();
    } finally {
      db.drop();
    }
  }

  // Issue #3931: variable-length pattern comprehension zero-length returns the anchor vertex itself
  @SuppressWarnings("unchecked")
  @Test
  void variableLengthPatternComprehensionZeroLength() {
    final Database db = new DatabaseFactory("./target/databases/testopencypher-varlen-comp-3931").create();
    try {
      db.getSchema().createVertexType("VarLengthTest3");
      db.getSchema().createEdgeType("KNOWS");
      db.transaction(() -> db.command("opencypher",
          """
          CREATE (alice:VarLengthTest3 {name:'Alice'}), \
          (bob:VarLengthTest3 {name:'Bob'}), \
          (charlie:VarLengthTest3 {name:'Charlie'}), \
          (alice)-[:KNOWS]->(bob), \
          (bob)-[:KNOWS]->(charlie), \
          (alice)-[:KNOWS]->(charlie)"""));

      // GitHub issue #3929: zero-length path returns the anchor node itself.
      final ResultSet resultSet = db.query("opencypher",
          """
          MATCH (a:VarLengthTest3 {name:'Alice'}) \
          RETURN [(a)-[:KNOWS*0..0]->(f:VarLengthTest3) | f.name] AS result""");

      assertThat(resultSet.hasNext()).isTrue();
      final Result r = resultSet.next();
      final List<Object> list = (List<Object>) r.getProperty("result");
      assertThat(list).containsExactly("Alice");
      assertThat(resultSet.hasNext()).isFalse();
    } finally {
      db.drop();
    }
  }

  // Issue #3931: variable-length pattern comprehension for non-existent hop length returns empty list
  @SuppressWarnings("unchecked")
  @Test
  void variableLengthPatternComprehensionNonExistentLength() {
    final Database db = new DatabaseFactory("./target/databases/testopencypher-varlen-comp-3931").create();
    try {
      db.getSchema().createVertexType("VarLengthTest3");
      db.getSchema().createEdgeType("KNOWS");
      db.transaction(() -> db.command("opencypher",
          """
          CREATE (alice:VarLengthTest3 {name:'Alice'}), \
          (bob:VarLengthTest3 {name:'Bob'}), \
          (charlie:VarLengthTest3 {name:'Charlie'}), \
          (alice)-[:KNOWS]->(bob), \
          (bob)-[:KNOWS]->(charlie), \
          (alice)-[:KNOWS]->(charlie)"""));

      // GitHub issue #3932: no path of length 10 exists, must return empty list.
      final ResultSet resultSet = db.query("opencypher",
          """
          MATCH (a:VarLengthTest3 {name:'Alice'}) \
          RETURN [(a)-[:KNOWS*10..10]->(f:VarLengthTest3) | f.name] AS result""");

      assertThat(resultSet.hasNext()).isTrue();
      final Result r = resultSet.next();
      final List<Object> list = (List<Object>) r.getProperty("result");
      assertThat(list).isEmpty();
      assertThat(resultSet.hasNext()).isFalse();
    } finally {
      db.drop();
    }
  }

  // Issue #3931: variable-length pattern comprehension across an inclusive range produces one element per matching path
  @SuppressWarnings("unchecked")
  @Test
  void variableLengthPatternComprehensionRange() {
    final Database db = new DatabaseFactory("./target/databases/testopencypher-varlen-comp-3931").create();
    try {
      db.getSchema().createVertexType("VarLengthTest3");
      db.getSchema().createEdgeType("KNOWS");
      db.transaction(() -> db.command("opencypher",
          """
          CREATE (alice:VarLengthTest3 {name:'Alice'}), \
          (bob:VarLengthTest3 {name:'Bob'}), \
          (charlie:VarLengthTest3 {name:'Charlie'}), \
          (alice)-[:KNOWS]->(bob), \
          (bob)-[:KNOWS]->(charlie), \
          (alice)-[:KNOWS]->(charlie)"""));

      // Length 1: alice->bob, alice->charlie. Length 2: alice->bob->charlie.
      final ResultSet resultSet = db.query("opencypher",
          """
          MATCH (a:VarLengthTest3 {name:'Alice'}) \
          RETURN [(a)-[:KNOWS*1..2]->(f:VarLengthTest3) | f.name] AS result""");

      assertThat(resultSet.hasNext()).isTrue();
      final Result r = resultSet.next();
      final List<Object> list = (List<Object>) r.getProperty("result");
      // Charlie appears twice: once at distance 1 (direct), once at distance 2 (via Bob)
      assertThat(list).containsExactlyInAnyOrder("Bob", "Charlie", "Charlie");
      assertThat(resultSet.hasNext()).isFalse();
    } finally {
      db.drop();
    }
  }

  // ---------------------------------------------------------------------------
  // Issue #3999: relationship variable stays bound after a later VLP MATCH.
  // ---------------------------------------------------------------------------

  // Issue #3999: count(r) bound before unbounded VLP MATCH still aggregates the rel
  @Test
  void countRelationshipBoundBeforeUnboundedVarLengthMatch() {
    final Database db = new DatabaseFactory("./target/databases/testopencypher-3999").create();
    try {
      db.getSchema().createVertexType("Person3999");
      db.getSchema().createEdgeType("KNOWS3999");
      db.transaction(() -> db.command("opencypher",
          """
          CREATE (a:Person3999 {name:'Alice'}),\
                 (b:Person3999 {name:'Bob'}),\
                 (a)-[:KNOWS3999]->(b)"""));

      final ResultSet result = db.query("opencypher",
          """
          MATCH (a:Person3999 {name:'Alice'})-[r:KNOWS3999]->(b:Person3999 {name:'Bob'}) \
          WITH a, b, r \
          MATCH path = (a)-[:KNOWS3999*]->(b) \
          RETURN count(r) AS rc""");

      final List<Result> rows = collect(result);
      assertThat(rows).hasSize(1);
      assertThat(((Number) rows.get(0).getProperty("rc")).longValue()).isEqualTo(1L);
    } finally {
      db.drop();
    }
  }

  // Issue #3999: count(r) bound before bounded *1..1 VLP MATCH still aggregates the rel
  @Test
  void countRelationshipBoundBeforeBoundedVarLengthMatch() {
    final Database db = new DatabaseFactory("./target/databases/testopencypher-3999").create();
    try {
      db.getSchema().createVertexType("Person3999");
      db.getSchema().createEdgeType("KNOWS3999");
      db.transaction(() -> db.command("opencypher",
          """
          CREATE (a:Person3999 {name:'Alice'}),\
                 (b:Person3999 {name:'Bob'}),\
                 (a)-[:KNOWS3999]->(b)"""));

      final ResultSet result = db.query("opencypher",
          """
          MATCH (a:Person3999 {name:'Alice'})-[r:KNOWS3999]->(b:Person3999 {name:'Bob'}) \
          WITH a, b, r \
          MATCH path = (a)-[:KNOWS3999*1..1]->(b) \
          RETURN count(r) AS rc""");

      final List<Result> rows = collect(result);
      assertThat(rows).hasSize(1);
      assertThat(((Number) rows.get(0).getProperty("rc")).longValue()).isEqualTo(1L);
    } finally {
      db.drop();
    }
  }

  // Issue #3999: collect(type(r)) carried through WITH survives a VLP MATCH
  @Test
  void collectTypeOfRelationshipBoundBeforeVarLengthMatch() {
    final Database db = new DatabaseFactory("./target/databases/testopencypher-3999").create();
    try {
      db.getSchema().createVertexType("Person3999");
      db.getSchema().createEdgeType("KNOWS3999");
      db.transaction(() -> db.command("opencypher",
          """
          CREATE (a:Person3999 {name:'Alice'}),\
                 (b:Person3999 {name:'Bob'}),\
                 (a)-[:KNOWS3999]->(b)"""));

      final ResultSet result = db.query("opencypher",
          """
          MATCH (a:Person3999 {name:'Alice'})-[r:KNOWS3999]->(b:Person3999 {name:'Bob'}) \
          WITH a, b, r \
          MATCH path = (a)-[:KNOWS3999*]->(b) \
          RETURN count(r) AS rc, collect(type(r)) AS rts""");

      final List<Result> rows = collect(result);
      assertThat(rows).hasSize(1);
      assertThat(((Number) rows.get(0).getProperty("rc")).longValue()).isEqualTo(1L);
      final List<Object> rts = rows.get(0).getProperty("rts");
      assertThat(rts).containsExactly("KNOWS3999");
    } finally {
      db.drop();
    }
  }

  // Issue #3999: relationship freshly CREATEd in the same statement is preserved across a later VLP MATCH
  @Test
  void countRelationshipCreatedAndCarriedThroughVarLengthMatch() {
    final Database db = new DatabaseFactory("./target/databases/testopencypher-3999").create();
    try {
      db.getSchema().createVertexType("Person3999");
      db.getSchema().createEdgeType("KNOWS3999");
      db.transaction(() -> db.command("opencypher",
          """
          CREATE (a:Person3999 {name:'Alice'}),\
                 (b:Person3999 {name:'Bob'}),\
                 (a)-[:KNOWS3999]->(b)"""));

      // Drop the KNOWS edge created in setUp so only the CREATE'd edge exists
      db.transaction(() -> db.command("opencypher", "MATCH ()-[r:KNOWS3999]->() DELETE r"));

      final ResultSet[] resultRef = new ResultSet[1];
      db.transaction(() -> resultRef[0] = db.command("opencypher",
          """
          MATCH (a:Person3999), (b:Person3999) \
          WHERE a.name = 'Alice' AND b.name = 'Bob' \
          CREATE (a)-[r:KNOWS3999 {label:'second'}]->(b) \
          WITH a, b, r \
          MATCH path = (a)-[:KNOWS3999*]->(b) \
          WITH count(r) AS rc \
          RETURN rc"""));

      final List<Result> rows = collect(resultRef[0]);
      assertThat(rows).hasSize(1);
      assertThat(((Number) rows.get(0).getProperty("rc")).longValue()).isEqualTo(1L);
    } finally {
      db.drop();
    }
  }

  // Issue #3999: MERGEd relationship survives a subsequent VLP MATCH consuming the same path
  @Test
  void mergeRelationshipAndConsumePath() {
    final Database db = new DatabaseFactory("./target/databases/testopencypher-3999").create();
    try {
      db.getSchema().createVertexType("Person3999");
      db.getSchema().createEdgeType("KNOWS3999");
      db.transaction(() -> db.command("opencypher",
          """
          CREATE (a:Person3999 {name:'Alice'}),\
                 (b:Person3999 {name:'Bob'}),\
                 (a)-[:KNOWS3999]->(b)"""));

      // Drop the KNOWS edge created in setUp so MERGE creates fresh state
      db.transaction(() -> db.command("opencypher", "MATCH ()-[r:KNOWS3999]->() DELETE r"));

      final ResultSet[] resultRef = new ResultSet[1];
      db.transaction(() -> resultRef[0] = db.command("opencypher",
          """
          MATCH (p1:Person3999), (p2:Person3999) \
          WHERE p1.name = 'Alice' AND p2.name = 'Bob' \
          MERGE (p1)-[r:KNOWS3999 {since: 2020}]->(p2) \
          WITH p1, p2, r \
          MATCH path = (p1)-[:KNOWS3999*]->(p2) \
          RETURN p1.name AS startName, \
                 p2.name AS endName, \
                 count(r) AS relationshipCount"""));

      final List<Result> rows = collect(resultRef[0]);
      assertThat(rows).hasSize(1);
      assertThat((String) rows.get(0).getProperty("startName")).isEqualTo("Alice");
      assertThat((String) rows.get(0).getProperty("endName")).isEqualTo("Bob");
      assertThat(((Number) rows.get(0).getProperty("relationshipCount")).longValue()).isEqualTo(1L);
    } finally {
      db.drop();
    }
  }

  // Issue #3999: fixed-length control case showing pre-existing behavior remains correct
  @Test
  void fixedLengthControlCaseAlreadyWorks() {
    final Database db = new DatabaseFactory("./target/databases/testopencypher-3999").create();
    try {
      db.getSchema().createVertexType("Person3999");
      db.getSchema().createEdgeType("KNOWS3999");
      db.transaction(() -> db.command("opencypher",
          """
          CREATE (a:Person3999 {name:'Alice'}),\
                 (b:Person3999 {name:'Bob'}),\
                 (a)-[:KNOWS3999]->(b)"""));

      final ResultSet result = db.query("opencypher",
          """
          MATCH (a:Person3999 {name:'Alice'})-[r:KNOWS3999]->(b:Person3999 {name:'Bob'}) \
          WITH a, b, r \
          MATCH path = (a)-[:KNOWS3999]->(b) \
          RETURN count(r) AS rc""");

      final List<Result> rows = collect(result);
      assertThat(rows).hasSize(1);
      assertThat(((Number) rows.get(0).getProperty("rc")).longValue()).isEqualTo(1L);
    } finally {
      db.drop();
    }
  }

  // ---------------------------------------------------------------------------
  // Issue #4006: VLP segments must obey path isomorphism w.r.t. a bound rel var.
  // ---------------------------------------------------------------------------

  // Issue #4006: VLP segments flanking a bound rel must not re-traverse it (TCK Match4 [7])
  @Test
  void vlpSegmentsDoNotReuseExplicitlyNamedBoundRelationship() {
    final Database db = new DatabaseFactory("./target/databases/testopencypher-4006").create();
    try {
      db.getSchema().createVertexType("Node4006");
      db.getSchema().createEdgeType("EDGE4006");
      db.transaction(() -> db.command("opencypher",
          "CREATE (n0:Node4006)-[:EDGE4006]->(n1:Node4006)-[:EDGE4006]->(n2:Node4006)-[:EDGE4006]->(n3:Node4006)"));

      final ResultSet result = db.query("opencypher",
          """
          MATCH ()-[r:EDGE4006]-()\
           MATCH p = (n)-[*0..1]-()-[r]-()-[*0..1]-(m)\
           RETURN count(p) AS c""");

      final List<Result> rows = collect(result);
      assertThat(rows).hasSize(1);
      assertThat(((Number) rows.get(0).getProperty("c")).longValue()).isEqualTo(32L);
    } finally {
      db.drop();
    }
  }

  // Issue #4006: unrelated previously-bound rel must not block a later VLP MATCH (regression guard for #3999)
  @Test
  void unboundedVlpIsNotBlockedByUnrelatedPreviouslyBoundRel() {
    final Database db = new DatabaseFactory("./target/databases/testopencypher-4006").create();
    try {
      db.getSchema().createVertexType("Node4006");
      db.getSchema().createEdgeType("EDGE4006");
      db.transaction(() -> db.command("opencypher",
          "CREATE (n0:Node4006)-[:EDGE4006]->(n1:Node4006)-[:EDGE4006]->(n2:Node4006)-[:EDGE4006]->(n3:Node4006)"));

      final ResultSet result = db.query("opencypher",
          """
          MATCH (a:Node4006)-[r:EDGE4006]->(b:Node4006)\
           WITH a, b, r\
           MATCH path = (a)-[:EDGE4006*1..2]->(b)\
           RETURN count(r) AS rc""");

      final List<Result> rows = collect(result);
      assertThat(rows).hasSize(1);
      assertThat(((Number) rows.get(0).getProperty("rc")).longValue()).isGreaterThan(0L);
    } finally {
      db.drop();
    }
  }

  // ---------------------------------------------------------------------------
  // Issue #4095 (consecutive directed): adjacent directed rel slots must not share an edge.
  // ---------------------------------------------------------------------------

  // Issue #4095: anonymous (s)<-[:KNOWS]-(f)-[:KNOWS]->(d) must not reuse the same edge for both slots
  @Test
  void consecutiveDirectedRelDoesNotReuseSameEdge() {
    final Database db = new DatabaseFactory("./target/databases/issue-4095-consecutive-directed-rel").create();
    try {
      db.transaction(() -> db.command("opencypher",
          "CREATE (:Person {name:'Alice'})<-[:KNOWS]-(:Person {name:'Bob'})-[:KNOWS]->(:Person {name:'Charlie'})"));

      final ResultSet rs = db.query("opencypher",
          "MATCH (s:Person)<-[:KNOWS]-(f:Person)-[:KNOWS]->(d:Person) "
              + "RETURN s.name AS s, f.name AS f, d.name AS d "
              + "ORDER BY s, f, d");
      final List<String> rows = new ArrayList<>();
      while (rs.hasNext()) {
        final Result r = rs.next();
        rows.add(r.<String>getProperty("s") + "," + r.<String>getProperty("f") + "," + r.<String>getProperty("d"));
      }
      assertThat(rows).containsExactly("Alice,Bob,Charlie", "Charlie,Bob,Alice");
    } finally {
      db.drop();
    }
  }

  // ---------------------------------------------------------------------------
  // Issue #4095 (directed rel reuse isomorphism, diverging/converging/3-hop/mixed/cross-clause).
  // ---------------------------------------------------------------------------

  // Issue #4095: diverging anonymous directed pattern must not bind the same edge twice
  @Test
  void divergingAnonymousDirectedDoesNotReuseRelationship() {
    final Database db = new DatabaseFactory("./target/databases/testopencypher-4095").create();
    try {
      db.getSchema().createVertexType("Person4095");
      db.getSchema().createEdgeType("KNOWS4095");
      db.transaction(() -> {
        db.command("opencypher",
            "CREATE (:Person4095 {name:'Alice'})<-[:KNOWS4095]-(:Person4095 {name:'Bob'})-[:KNOWS4095]->(:Person4095 {name:'Charlie'})");
        db.command("opencypher",
            "CREATE (:Person4095 {name:'Dave'})-[:KNOWS4095]->(:Person4095 {name:'Eve'})<-[:KNOWS4095]-(:Person4095 {name:'Frank'})");
      });

      final ResultSet result = db.query("opencypher",
          """
              MATCH (s:Person4095)<-[:KNOWS4095]-(f:Person4095)-[:KNOWS4095]->(d:Person4095)
              WHERE f.name = 'Bob'
              RETURN s.name AS s, f.name AS f, d.name AS d
              ORDER BY s, d""");

      final List<Result> rows = collect(result);
      assertThat(rows).hasSize(2);

      assertThat((String) rows.get(0).getProperty("s")).isEqualTo("Alice");
      assertThat((String) rows.get(0).getProperty("f")).isEqualTo("Bob");
      assertThat((String) rows.get(0).getProperty("d")).isEqualTo("Charlie");

      assertThat((String) rows.get(1).getProperty("s")).isEqualTo("Charlie");
      assertThat((String) rows.get(1).getProperty("f")).isEqualTo("Bob");
      assertThat((String) rows.get(1).getProperty("d")).isEqualTo("Alice");
    } finally {
      db.drop();
    }
  }

  // Issue #4095: converging anonymous directed pattern must not bind the same edge twice
  @Test
  void convergingAnonymousDirectedDoesNotReuseRelationship() {
    final Database db = new DatabaseFactory("./target/databases/testopencypher-4095").create();
    try {
      db.getSchema().createVertexType("Person4095");
      db.getSchema().createEdgeType("KNOWS4095");
      db.transaction(() -> {
        db.command("opencypher",
            "CREATE (:Person4095 {name:'Alice'})<-[:KNOWS4095]-(:Person4095 {name:'Bob'})-[:KNOWS4095]->(:Person4095 {name:'Charlie'})");
        db.command("opencypher",
            "CREATE (:Person4095 {name:'Dave'})-[:KNOWS4095]->(:Person4095 {name:'Eve'})<-[:KNOWS4095]-(:Person4095 {name:'Frank'})");
      });

      final ResultSet result = db.query("opencypher",
          """
              MATCH (p1:Person4095)-[:KNOWS4095]->(p2:Person4095)<-[:KNOWS4095]-(p3:Person4095)
              WHERE p2.name = 'Eve'
              RETURN p1.name AS p1, p2.name AS p2, p3.name AS p3
              ORDER BY p1, p3""");

      final List<Result> rows = collect(result);
      assertThat(rows).hasSize(2);

      assertThat((String) rows.get(0).getProperty("p1")).isEqualTo("Dave");
      assertThat((String) rows.get(0).getProperty("p2")).isEqualTo("Eve");
      assertThat((String) rows.get(0).getProperty("p3")).isEqualTo("Frank");

      assertThat((String) rows.get(1).getProperty("p1")).isEqualTo("Frank");
      assertThat((String) rows.get(1).getProperty("p2")).isEqualTo("Eve");
      assertThat((String) rows.get(1).getProperty("p3")).isEqualTo("Dave");
    } finally {
      db.drop();
    }
  }

  // Issue #4095: count over diverging anonymous directed pattern is 2 (no spurious self-reuse rows)
  @Test
  void divergingAnonymousDirectedCountIsCorrect() {
    final Database db = new DatabaseFactory("./target/databases/testopencypher-4095").create();
    try {
      db.getSchema().createVertexType("Person4095");
      db.getSchema().createEdgeType("KNOWS4095");
      db.transaction(() -> {
        db.command("opencypher",
            "CREATE (:Person4095 {name:'Alice'})<-[:KNOWS4095]-(:Person4095 {name:'Bob'})-[:KNOWS4095]->(:Person4095 {name:'Charlie'})");
        db.command("opencypher",
            "CREATE (:Person4095 {name:'Dave'})-[:KNOWS4095]->(:Person4095 {name:'Eve'})<-[:KNOWS4095]-(:Person4095 {name:'Frank'})");
      });

      final ResultSet result = db.query("opencypher",
          """
              MATCH (s:Person4095)<-[:KNOWS4095]-(f:Person4095)-[:KNOWS4095]->(d:Person4095)
              WHERE f.name = 'Bob'
              RETURN count(d) AS c""");

      final List<Result> rows = collect(result);
      assertThat(rows).hasSize(1);
      assertThat(((Number) rows.get(0).getProperty("c")).longValue()).isEqualTo(2L);
    } finally {
      db.drop();
    }
  }

  // Issue #4095: count over converging anonymous directed pattern is 2 (no spurious self-reuse rows)
  @Test
  void convergingAnonymousDirectedCountIsCorrect() {
    final Database db = new DatabaseFactory("./target/databases/testopencypher-4095").create();
    try {
      db.getSchema().createVertexType("Person4095");
      db.getSchema().createEdgeType("KNOWS4095");
      db.transaction(() -> {
        db.command("opencypher",
            "CREATE (:Person4095 {name:'Alice'})<-[:KNOWS4095]-(:Person4095 {name:'Bob'})-[:KNOWS4095]->(:Person4095 {name:'Charlie'})");
        db.command("opencypher",
            "CREATE (:Person4095 {name:'Dave'})-[:KNOWS4095]->(:Person4095 {name:'Eve'})<-[:KNOWS4095]-(:Person4095 {name:'Frank'})");
      });

      final ResultSet result = db.query("opencypher",
          """
              MATCH (p1:Person4095)-[:KNOWS4095]->(p2:Person4095)<-[:KNOWS4095]-(p3:Person4095)
              WHERE p2.name = 'Eve'
              RETURN count(p1) AS c""");

      final List<Result> rows = collect(result);
      assertThat(rows).hasSize(1);
      assertThat(((Number) rows.get(0).getProperty("c")).longValue()).isEqualTo(2L);
    } finally {
      db.drop();
    }
  }

  // Issue #4095: three-hop anonymous chain of the same type binds three distinct edges
  @Test
  void threeHopAnonymousSameTypeChainHasDistinctEdges() {
    final Database db = new DatabaseFactory("./target/databases/testopencypher-4095").create();
    try {
      db.getSchema().createVertexType("Person4095");
      db.getSchema().createEdgeType("KNOWS4095");
      db.transaction(() -> {
        db.command("opencypher",
            "CREATE (:Person4095 {name:'Alice'})<-[:KNOWS4095]-(:Person4095 {name:'Bob'})-[:KNOWS4095]->(:Person4095 {name:'Charlie'})");
        db.command("opencypher",
            "CREATE (:Person4095 {name:'Dave'})-[:KNOWS4095]->(:Person4095 {name:'Eve'})<-[:KNOWS4095]-(:Person4095 {name:'Frank'})");
      });

      db.transaction(() -> db.command("opencypher",
          """
              CREATE (:Person4095 {name:'C1'})-[:KNOWS4095]->(:Person4095 {name:'C2'})
                    -[:KNOWS4095]->(:Person4095 {name:'C3'})
                    -[:KNOWS4095]->(:Person4095 {name:'C4'})"""));

      final ResultSet result = db.query("opencypher",
          """
              MATCH (a:Person4095)-[:KNOWS4095]->(b:Person4095)-[:KNOWS4095]->(c:Person4095)-[:KNOWS4095]->(d:Person4095)
              WHERE a.name STARTS WITH 'C'
              RETURN a.name AS a, b.name AS b, c.name AS c, d.name AS d""");

      final List<Result> rows = collect(result);
      assertThat(rows).hasSize(1);
      assertThat((String) rows.get(0).getProperty("a")).isEqualTo("C1");
      assertThat((String) rows.get(0).getProperty("b")).isEqualTo("C2");
      assertThat((String) rows.get(0).getProperty("c")).isEqualTo("C3");
      assertThat((String) rows.get(0).getProperty("d")).isEqualTo("C4");
    } finally {
      db.drop();
    }
  }

  // Issue #4095: mixed named + anonymous rels in same MATCH clause must each track its own edge exclusion
  @Test
  void mixedNamedAndAnonymousSameClauseBindsDistinctEdges() {
    final Database db = new DatabaseFactory("./target/databases/testopencypher-4095").create();
    try {
      db.getSchema().createVertexType("Person4095");
      db.getSchema().createEdgeType("KNOWS4095");
      db.transaction(() -> {
        db.command("opencypher",
            "CREATE (:Person4095 {name:'Alice'})<-[:KNOWS4095]-(:Person4095 {name:'Bob'})-[:KNOWS4095]->(:Person4095 {name:'Charlie'})");
        db.command("opencypher",
            "CREATE (:Person4095 {name:'Dave'})-[:KNOWS4095]->(:Person4095 {name:'Eve'})<-[:KNOWS4095]-(:Person4095 {name:'Frank'})");
      });

      final ResultSet result = db.query("opencypher",
          """
              MATCH (s:Person4095)<-[r:KNOWS4095]-(f:Person4095)-[:KNOWS4095]->(d:Person4095)
              WHERE f.name = 'Bob'
              RETURN s.name AS s, type(r) AS rt, d.name AS d
              ORDER BY s, d""");

      final List<Result> rows = collect(result);
      assertThat(rows).hasSize(2);
      assertThat((String) rows.get(0).getProperty("s")).isEqualTo("Alice");
      assertThat((String) rows.get(0).getProperty("d")).isEqualTo("Charlie");
      assertThat((String) rows.get(1).getProperty("s")).isEqualTo("Charlie");
      assertThat((String) rows.get(1).getProperty("d")).isEqualTo("Alice");
    } finally {
      db.drop();
    }
  }

  // Issue #4095: cross-clause anonymous edge reuse is allowed (uniqueness is per MATCH clause)
  @Test
  void crossClauseAnonymousEdgeReuseIsAllowed() {
    final Database db = new DatabaseFactory("./target/databases/testopencypher-4095").create();
    try {
      db.getSchema().createVertexType("Person4095");
      db.getSchema().createEdgeType("KNOWS4095");
      db.transaction(() -> {
        db.command("opencypher",
            "CREATE (:Person4095 {name:'Alice'})<-[:KNOWS4095]-(:Person4095 {name:'Bob'})-[:KNOWS4095]->(:Person4095 {name:'Charlie'})");
        db.command("opencypher",
            "CREATE (:Person4095 {name:'Dave'})-[:KNOWS4095]->(:Person4095 {name:'Eve'})<-[:KNOWS4095]-(:Person4095 {name:'Frank'})");
      });

      final ResultSet result = db.query("opencypher",
          """
              MATCH (a:Person4095)-[:KNOWS4095]->(b:Person4095)
              MATCH (b:Person4095)<-[:KNOWS4095]-(c:Person4095)
              WHERE b.name = 'Eve'
              RETURN count(*) AS cnt""");

      final List<Result> rows = collect(result);
      assertThat(rows).hasSize(1);
      // Eve has 2 IN edges (Dave->Eve, Frank->Eve). First MATCH binds (a,b)=(Dave,Eve) or
      // (Frank,Eve). Second MATCH binds c independently from b's IN edges. Same edge can
      // legitimately fill the first MATCH's slot AND the second MATCH's slot, giving 4 rows.
      assertThat(((Number) rows.get(0).getProperty("cnt")).longValue()).isEqualTo(4L);
    } finally {
      db.drop();
    }
  }

  // ---------------------------------------------------------------------------
  // Issue #4096: undirected rel reuse isomorphism per MATCH clause.
  // ---------------------------------------------------------------------------

  // Issue #4096: named undirected multi-hop binds distinct edges to r1 and r2
  @Test
  void undirectedMultiHopDoesNotReuseRelationship() {
    final Database db = new DatabaseFactory("./target/databases/testopencypher-4096").create();
    try {
      db.getSchema().createVertexType("Person4096");
      db.getSchema().createEdgeType("KNOWS4096");
      db.transaction(() -> db.command("opencypher",
          "CREATE (a:Person4096 {name:'Alice'}), (b:Person4096 {name:'Bob'}), (c:Person4096 {name:'Charlie'}),"
              + " (a)-[:KNOWS4096]->(b), (b)-[:KNOWS4096]->(c)"));

      final ResultSet result = db.query("opencypher",
          """
              MATCH (a:Person4096)-[r1:KNOWS4096]-(b:Person4096)-[r2:KNOWS4096]-(c:Person4096)
              RETURN a.name AS a, b.name AS b, c.name AS c, r1 = r2 AS sameRel
              ORDER BY a, c""");

      final List<Result> rows = collect(result);
      assertThat(rows).hasSize(2);

      assertThat((String) rows.get(0).getProperty("a")).isEqualTo("Alice");
      assertThat((String) rows.get(0).getProperty("b")).isEqualTo("Bob");
      assertThat((String) rows.get(0).getProperty("c")).isEqualTo("Charlie");
      assertThat((Boolean) rows.get(0).getProperty("sameRel")).isFalse();

      assertThat((String) rows.get(1).getProperty("a")).isEqualTo("Charlie");
      assertThat((String) rows.get(1).getProperty("b")).isEqualTo("Bob");
      assertThat((String) rows.get(1).getProperty("c")).isEqualTo("Alice");
      assertThat((Boolean) rows.get(1).getProperty("sameRel")).isFalse();
    } finally {
      db.drop();
    }
  }

  // Issue #4096: named undirected multi-hop count of distinct-rel traversals is exactly 2
  @Test
  void undirectedMultiHopDistinctRelationshipCount() {
    final Database db = new DatabaseFactory("./target/databases/testopencypher-4096").create();
    try {
      db.getSchema().createVertexType("Person4096");
      db.getSchema().createEdgeType("KNOWS4096");
      db.transaction(() -> db.command("opencypher",
          "CREATE (a:Person4096 {name:'Alice'}), (b:Person4096 {name:'Bob'}), (c:Person4096 {name:'Charlie'}),"
              + " (a)-[:KNOWS4096]->(b), (b)-[:KNOWS4096]->(c)"));

      final ResultSet result = db.query("opencypher",
          """
              MATCH (a:Person4096)-[r1:KNOWS4096]-(b:Person4096)-[r2:KNOWS4096]-(c:Person4096)
              WHERE r1 <> r2 RETURN count(*) AS cnt""");

      final List<Result> rows = collect(result);
      assertThat(rows).hasSize(1);
      assertThat(((Number) rows.get(0).getProperty("cnt")).longValue()).isEqualTo(2L);
    } finally {
      db.drop();
    }
  }

  // Issue #4096: cross-clause edge reuse is allowed (isomorphism scoped per MATCH clause)
  @Test
  void crossClauseEdgeReuseIsAllowed() {
    final Database db = new DatabaseFactory("./target/databases/testopencypher-4096").create();
    try {
      db.getSchema().createVertexType("Person4096");
      db.getSchema().createEdgeType("KNOWS4096");
      db.transaction(() -> db.command("opencypher",
          "CREATE (a:Person4096 {name:'Alice'}), (b:Person4096 {name:'Bob'}), (c:Person4096 {name:'Charlie'}),"
              + " (a)-[:KNOWS4096]->(b), (b)-[:KNOWS4096]->(c)"));

      final ResultSet result = db.query("opencypher",
          """
              MATCH (a:Person4096)-[r1:KNOWS4096]-(b:Person4096)
              MATCH (b:Person4096)-[r2:KNOWS4096]-(c:Person4096)
              RETURN count(*) AS cnt""");

      final List<Result> rows = collect(result);
      assertThat(rows).hasSize(1);
      assertThat(((Number) rows.get(0).getProperty("cnt")).longValue()).isEqualTo(6L);
    } finally {
      db.drop();
    }
  }

  // Issue #4096: ExpandInto path also honors per-clause relationship-uniqueness
  @Test
  void expandIntoPathDoesNotReuseRelationship() {
    final Database db = new DatabaseFactory("./target/databases/testopencypher-4096").create();
    try {
      db.getSchema().createVertexType("Person4096");
      db.getSchema().createEdgeType("KNOWS4096");
      db.transaction(() -> db.command("opencypher",
          "CREATE (a:Person4096 {name:'Alice'}), (b:Person4096 {name:'Bob'}), (c:Person4096 {name:'Charlie'}),"
              + " (a)-[:KNOWS4096]->(b), (b)-[:KNOWS4096]->(c)"));

      final ResultSet result = db.query("opencypher",
          """
              MATCH (a:Person4096 {name:'Alice'}), (c:Person4096 {name:'Charlie'})
              MATCH (a)-[r1:KNOWS4096]-(b:Person4096), (b)-[r2:KNOWS4096]-(c)
              RETURN count(*) AS cnt""");

      final List<Result> rows = collect(result);
      assertThat(rows).hasSize(1);
      assertThat(((Number) rows.get(0).getProperty("cnt")).longValue()).isEqualTo(1L);
    } finally {
      db.drop();
    }
  }

  // ---------------------------------------------------------------------------
  // Issue #4106: pattern comprehension with unbound start iterates over graph.
  // ---------------------------------------------------------------------------

  // Issue #4106: pattern comprehension with unbound start vertex iterates over candidates and returns all matches
  @SuppressWarnings("unchecked")
  @Test
  void patternComprehensionUnboundStartReturnsAllMatches() {
    final Database db = new DatabaseFactory("./target/databases/issue-4106-pattern-comp").create();
    try {
      db.transaction(() -> db.command("opencypher",
          "CREATE (a:Person {name:'Alice', age:30}), "
              + "(b:Person {name:'Bob', age:25}), "
              + "(c:Person {name:'Charlie', age:35}), "
              + "(a)-[:KNOWS {since:2020}]->(b), "
              + "(b)-[:KNOWS {since:2019}]->(c), "
              + "(a)-[:KNOWS {since:2021}]->(c)"));

      final ResultSet rs = db.query("opencypher",
          "RETURN [(p:Person)-[r:KNOWS]->(q:Person) | q.name] AS xs");
      assertThat(rs.hasNext()).isTrue();
      final List<Object> list = (List<Object>) rs.next().getProperty("xs");
      assertThat(list).containsExactlyInAnyOrder("Bob", "Charlie", "Charlie");
    } finally {
      db.drop();
    }
  }

  // Issue #4106: pattern comprehension with unbound start and WHERE filter returns filtered results
  @SuppressWarnings("unchecked")
  @Test
  void patternComprehensionUnboundStartWithFilterReturnsFiltered() {
    final Database db = new DatabaseFactory("./target/databases/issue-4106-pattern-comp").create();
    try {
      db.transaction(() -> db.command("opencypher",
          "CREATE (a:Person {name:'Alice', age:30}), "
              + "(b:Person {name:'Bob', age:25}), "
              + "(c:Person {name:'Charlie', age:35}), "
              + "(a)-[:KNOWS {since:2020}]->(b), "
              + "(b)-[:KNOWS {since:2019}]->(c), "
              + "(a)-[:KNOWS {since:2021}]->(c)"));

      final ResultSet rs = db.query("opencypher",
          "RETURN [(p:Person)-[r:KNOWS]->(q:Person) WHERE r.since > 2019 | q.name] AS xs");
      assertThat(rs.hasNext()).isTrue();
      final List<Object> list = (List<Object>) rs.next().getProperty("xs");
      assertThat(list).containsExactlyInAnyOrder("Bob", "Charlie");
    } finally {
      db.drop();
    }
  }

  // Issue #4106: size() over a pattern comprehension with unbound start returns the count
  @Test
  void patternComprehensionSizeReturnsCount() {
    final Database db = new DatabaseFactory("./target/databases/issue-4106-pattern-comp").create();
    try {
      db.transaction(() -> db.command("opencypher",
          "CREATE (a:Person {name:'Alice', age:30}), "
              + "(b:Person {name:'Bob', age:25}), "
              + "(c:Person {name:'Charlie', age:35}), "
              + "(a)-[:KNOWS {since:2020}]->(b), "
              + "(b)-[:KNOWS {since:2019}]->(c), "
              + "(a)-[:KNOWS {since:2021}]->(c)"));

      final ResultSet rs = db.query("opencypher",
          "RETURN size([(p:Person)-[r:KNOWS]->(q:Person) | q.name]) AS s");
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Number>getProperty("s").longValue()).isEqualTo(3L);
    } finally {
      db.drop();
    }
  }

  // ---------------------------------------------------------------------------
  // Issue #4109: a relationship variable repeated within a WHERE pattern is unsatisfiable.
  // ---------------------------------------------------------------------------

  // Issue #4109: repeated rel var (p)-[r]->()<-[r]- is unsatisfiable, count is 0
  @Test
  void repeatedRelVarInWhereIsUnsatisfiable() {
    final Database db = new DatabaseFactory("./target/databases/issue-4109-repeated-rel-var").create();
    try {
      db.transaction(() -> db.command("opencypher",
          "CREATE (:Person {name:'Alice'})-[:KNOWS]->(:Person {name:'Bob'})-[:KNOWS]->(:Person {name:'Charlie'})"));

      final ResultSet rs = db.query("opencypher",
          "MATCH (p:Person)-[r:KNOWS]-() WHERE (p)-[r:KNOWS]->()<-[r:KNOWS]-() RETURN count(*) AS cnt");
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Number>getProperty("cnt").longValue()).isEqualTo(0L);
    } finally {
      db.drop();
    }
  }

  // Issue #4109: repeated rel var under EXISTS { ... } agrees and returns 0
  @Test
  void repeatedRelVarUnderExistsAgrees() {
    final Database db = new DatabaseFactory("./target/databases/issue-4109-repeated-rel-var").create();
    try {
      db.transaction(() -> db.command("opencypher",
          "CREATE (:Person {name:'Alice'})-[:KNOWS]->(:Person {name:'Bob'})-[:KNOWS]->(:Person {name:'Charlie'})"));

      final ResultSet rs = db.query("opencypher",
          "MATCH (p:Person)-[r:KNOWS]-() WHERE EXISTS { MATCH (p)-[r:KNOWS]->()<-[r:KNOWS]-() } RETURN count(*) AS cnt");
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Number>getProperty("cnt").longValue()).isEqualTo(0L);
    } finally {
      db.drop();
    }
  }

  // Issue #4109: baseline MATCH (p:Person)-[r:KNOWS]-() matches 4 (sanity check)
  @Test
  void baselineMatchPersonRelMatchesFour() {
    final Database db = new DatabaseFactory("./target/databases/issue-4109-repeated-rel-var").create();
    try {
      db.transaction(() -> db.command("opencypher",
          "CREATE (:Person {name:'Alice'})-[:KNOWS]->(:Person {name:'Bob'})-[:KNOWS]->(:Person {name:'Charlie'})"));

      final ResultSet rs = db.query("opencypher",
          "MATCH (p:Person)-[r:KNOWS]-() RETURN count(*) AS cnt");
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Number>getProperty("cnt").longValue()).isEqualTo(4L);
    } finally {
      db.drop();
    }
  }

  // ---------------------------------------------------------------------------
  // Issue #4111: VLP pattern comprehension with both endpoints bound must not duplicate.
  // ---------------------------------------------------------------------------

  // Issue #4111: VLP pattern comprehension with bound endpoints does not duplicate when matching path passes through end vertex
  @SuppressWarnings("unchecked")
  @Test
  void varLengthCompBoundEndpointsNoDuplicates() {
    final Database db = new DatabaseFactory("./target/databases/issue-4111-varlength-comp").create();
    try {
      db.transaction(() -> db.command("opencypher",
          "CREATE (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), (c:Person {name:'Charlie'}), "
              + "(a)-[:KNOWS]->(b), (b)-[:KNOWS]->(c)"));

      final ResultSet rs = db.query("opencypher",
          "MATCH (p1:Person {name:'Alice'}), (p2:Person {name:'Bob'}) "
              + "RETURN [(p1)-[:KNOWS*1..3]->(p2) | 'X'] AS vals");
      assertThat(rs.hasNext()).isTrue();
      final List<Object> list = (List<Object>) rs.next().getProperty("vals");
      assertThat(list).containsExactly("X");
    } finally {
      db.drop();
    }
  }

  // Issue #4111: fixed-length control case projects one element per matching path with bound endpoints
  @SuppressWarnings("unchecked")
  @Test
  void varLengthCompFixedLengthControl() {
    final Database db = new DatabaseFactory("./target/databases/issue-4111-varlength-comp").create();
    try {
      db.transaction(() -> db.command("opencypher",
          "CREATE (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), (c:Person {name:'Charlie'}), "
              + "(a)-[:KNOWS]->(b), (b)-[:KNOWS]->(c)"));

      final ResultSet rs = db.query("opencypher",
          "MATCH (p1:Person {name:'Alice'}), (p2:Person {name:'Bob'}) "
              + "RETURN [(p1)-[:KNOWS]->(p2) | 'X'] AS vals");
      assertThat(rs.hasNext()).isTrue();
      final List<Object> list = (List<Object>) rs.next().getProperty("vals");
      assertThat(list).containsExactly("X");
    } finally {
      db.drop();
    }
  }

  // Issue #4111: VLP pattern comprehension with no matching path returns empty list
  @SuppressWarnings("unchecked")
  @Test
  void varLengthCompNoMatchReturnsEmpty() {
    final Database db = new DatabaseFactory("./target/databases/issue-4111-varlength-comp").create();
    try {
      db.transaction(() -> db.command("opencypher",
          "CREATE (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), (c:Person {name:'Charlie'}), "
              + "(a)-[:KNOWS]->(b), (b)-[:KNOWS]->(c)"));

      final ResultSet rs = db.query("opencypher",
          "MATCH (p1:Person {name:'Charlie'}), (p2:Person {name:'Bob'}) "
              + "RETURN [(p1)-[:KNOWS*1..3]->(p2) | 'X'] AS vals");
      assertThat(rs.hasNext()).isTrue();
      final List<Object> list = (List<Object>) rs.next().getProperty("vals");
      assertThat(list).isEmpty();
    } finally {
      db.drop();
    }
  }

  // Issue #4111: VLP pattern comprehension can project a bound outer property
  @SuppressWarnings("unchecked")
  @Test
  void varLengthCompProjectsBoundProperty() {
    final Database db = new DatabaseFactory("./target/databases/issue-4111-varlength-comp").create();
    try {
      db.transaction(() -> db.command("opencypher",
          "CREATE (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), (c:Person {name:'Charlie'}), "
              + "(a)-[:KNOWS]->(b), (b)-[:KNOWS]->(c)"));

      final ResultSet rs = db.query("opencypher",
          "MATCH (p1:Person {name:'Alice'}), (p2:Person {name:'Bob'}) "
              + "RETURN [(p1)-[:KNOWS*1..3]->(p2) | p1.name] AS vals");
      assertThat(rs.hasNext()).isTrue();
      final List<Object> list = (List<Object>) rs.next().getProperty("vals");
      assertThat(list).containsExactly("Alice");
    } finally {
      db.drop();
    }
  }
}
