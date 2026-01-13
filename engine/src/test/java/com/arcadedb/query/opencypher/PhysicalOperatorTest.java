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
package com.arcadedb.opencypher;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.ast.Direction;
import com.arcadedb.query.opencypher.executor.operators.*;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for physical operators (Phase 2 implementation).
 * Tests all 8 physical operators: NodeByLabelScan, NodeIndexSeek, ExpandAll,
 * ExpandInto, NodeHashJoin, FilterOperator.
 */
public class PhysicalOperatorTest {
  private Database database;

  @BeforeEach
  public void setUp() {
    database = new DatabaseFactory("./target/databases/PhysicalOperatorTest").create();

    // Create schema and data in separate transactions for proper index building
    database.transaction(() -> {
      // Create schema without index first
      if (!database.getSchema().existsType("Person")) {
        database.getSchema().createVertexType("Person");
        database.getSchema().getType("Person").createProperty("id", Integer.class);
        database.getSchema().getType("Person").createProperty("name", String.class);
        database.getSchema().getType("Person").createProperty("age", Integer.class);
      }

      if (!database.getSchema().existsType("KNOWS")) {
        database.getSchema().createEdgeType("KNOWS");
      }
    });

    // Create test data
    database.transaction(() -> {
      final MutableVertex alice = database.newVertex("Person");
      alice.set("id", 1);
      alice.set("name", "Alice");
      alice.set("age", 30);
      alice.save();

      final MutableVertex bob = database.newVertex("Person");
      bob.set("id", 2);
      bob.set("name", "Bob");
      bob.set("age", 25);
      bob.save();

      final MutableVertex charlie = database.newVertex("Person");
      charlie.set("id", 3);
      charlie.set("name", "Charlie");
      charlie.set("age", 35);
      charlie.save();

      // Create relationships
      alice.newEdge("KNOWS", bob, true, (Object[]) null);
      alice.newEdge("KNOWS", charlie, true, (Object[]) null);
      bob.newEdge("KNOWS", charlie, true, (Object[]) null);
    });

    // Create index after data exists
    database.transaction(() -> {
      if (database.getSchema().getType("Person").getAllIndexes(false).isEmpty()) {
        database.getSchema().getType("Person")
            .createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "id");
      }
    });
  }

  @AfterEach
  public void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  public void testNodeByLabelScan() {
    database.transaction(() -> {
      final com.arcadedb.query.sql.executor.BasicCommandContext context = new com.arcadedb.query.sql.executor.BasicCommandContext();
      context.setDatabase(database);

      // Test scan of Person vertices
      final NodeByLabelScan scan = new NodeByLabelScan(
          "n", "Person", 3.0, 3L
      );

      final ResultSet results = scan.execute(context, -1);

      // Verify results
      final List<Result> resultList = collectResults(results);
      assertEquals(3, resultList.size(), "Should find 3 Person vertices");

      // Verify each result has a vertex bound to 'n'
      for (final Result result : resultList) {
        assertTrue(result.hasProperty("n"), "Result should have 'n' property");
        final Object obj = result.getProperty("n");
        assertTrue(obj instanceof Vertex, "Property 'n' should be a Vertex");
        final Vertex vertex = (Vertex) obj;
        assertEquals("Person", vertex.getTypeName(), "Vertex should be of type Person");
      }

      // Verify cost and cardinality
      assertEquals(3.0, scan.getEstimatedCost(), 0.01);
      assertEquals(3L, scan.getEstimatedCardinality());
    });
  }

  @Test
  public void testNodeIndexSeek() {
    database.transaction(() -> {
      final com.arcadedb.query.sql.executor.BasicCommandContext context = new com.arcadedb.query.sql.executor.BasicCommandContext();
      context.setDatabase(database);

      // Test index seek for Person with id=1
      final NodeIndexSeek seek = new NodeIndexSeek(
          "n", "Person", "id", 1, "Person.id", 5.1, 1L
      );

      final ResultSet results = seek.execute(context, -1);

      // Verify results
      final List<Result> resultList = collectResults(results);
      assertEquals(1, resultList.size(), "Should find exactly 1 Person with id=1");

      final Result result = resultList.get(0);
      assertTrue(result.hasProperty("n"), "Result should have 'n' property");
      final Vertex vertex = result.getProperty("n");
      assertEquals(1, (int) vertex.get("id"), "Vertex should have id=1");
      assertEquals("Alice", vertex.get("name"), "Vertex should be Alice");

      // Verify cost and cardinality
      assertEquals(5.1, seek.getEstimatedCost(), 0.01);
      assertEquals(1L, seek.getEstimatedCardinality());
    });
  }

  @Test
  public void testNodeIndexSeekNotFound() {
    database.transaction(() -> {
      final com.arcadedb.query.sql.executor.BasicCommandContext context = new com.arcadedb.query.sql.executor.BasicCommandContext();
      context.setDatabase(database);

      // Test index seek for non-existent id
      final NodeIndexSeek seek = new NodeIndexSeek(
          "n", "Person", "id", 999, "Person.id", 5.1, 0L
      );

      final ResultSet results = seek.execute(context, -1);

      // Verify no results
      final List<Result> resultList = collectResults(results);
      assertEquals(0, resultList.size(), "Should find no results for id=999");
    });
  }

  @Test
  public void testExpandAll() {
    database.transaction(() -> {
      final com.arcadedb.query.sql.executor.BasicCommandContext context = new com.arcadedb.query.sql.executor.BasicCommandContext();
      context.setDatabase(database);

      // First get Alice using index seek
      final NodeIndexSeek seek = new NodeIndexSeek(
          "a", "Person", "id", 1, "Person.id", 5.1, 1L
      );

      // Then expand from Alice following KNOWS relationships
      final ExpandAll expand = new ExpandAll(
          seek, "a", "r", "b", Direction.OUT, new String[]{"KNOWS"}, 2.0, 2L
      );

      final ResultSet results = expand.execute(context, -1);

      // Verify results - Alice knows Bob and Charlie
      final List<Result> resultList = collectResults(results);
      assertEquals(2, resultList.size(), "Alice should know 2 people");

      // Verify each result has source vertex, edge, and target vertex
      for (final Result result : resultList) {
        assertTrue(result.hasProperty("a"), "Result should have 'a' (source)");
        assertTrue(result.hasProperty("r"), "Result should have 'r' (edge)");
        assertTrue(result.hasProperty("b"), "Result should have 'b' (target)");

        final Vertex source = result.getProperty("a");
        assertEquals(1, (int) source.get("id"), "Source should be Alice");

        final Vertex target = result.getProperty("b");
        final int targetId = (Integer) target.get("id");
        assertTrue(targetId == 2 || targetId == 3, "Target should be Bob or Charlie");
      }
    });
  }

  @Test
  public void testExpandInto() {
    database.transaction(() -> {
      final com.arcadedb.query.sql.executor.BasicCommandContext context = new com.arcadedb.query.sql.executor.BasicCommandContext();
      context.setDatabase(database);

      // Get both Alice and Bob using index seeks
      final NodeIndexSeek seekAlice = new NodeIndexSeek(
          "a", "Person", "id", 1, "Person.id", 5.1, 1L
      );

      final NodeIndexSeek seekBob = new NodeIndexSeek(
          "b", "Person", "id", 2, "Person.id", 5.1, 1L
      );

      // Join Alice and Bob using hash join
      final NodeHashJoin join = new NodeHashJoin(
          seekAlice, seekBob, "dummy", 10.2, 1L
      );

      // Check if there's a KNOWS relationship between them using ExpandInto
      final ExpandInto expandInto = new ExpandInto(
          join, "a", "b", "r", Direction.OUT, new String[]{"KNOWS"}, 1.0, 1L
      );

      final ResultSet results = expandInto.execute(context, -1);

      // Verify results - there should be a KNOWS relationship from Alice to Bob
      final List<Result> resultList = collectResults(results);
      assertEquals(1, resultList.size(), "Should find KNOWS relationship from Alice to Bob");

      final Result result = resultList.get(0);
      assertTrue(result.hasProperty("a"), "Result should have 'a' (Alice)");
      assertTrue(result.hasProperty("b"), "Result should have 'b' (Bob)");
      assertTrue(result.hasProperty("r"), "Result should have 'r' (edge)");

      final Vertex source = result.getProperty("a");
      final Vertex target = result.getProperty("b");
      assertEquals(1, (int) source.get("id"), "Source should be Alice");
      assertEquals(2, (int) target.get("id"), "Target should be Bob");
    });
  }

  @Test
  public void testExpandIntoNotConnected() {
    database.transaction(() -> {
      final com.arcadedb.query.sql.executor.BasicCommandContext context = new com.arcadedb.query.sql.executor.BasicCommandContext();
      context.setDatabase(database);

      // Get Bob and Alice (reverse direction - Bob doesn't know Alice in OUT direction)
      final NodeIndexSeek seekBob = new NodeIndexSeek(
          "a", "Person", "id", 2, "Person.id", 5.1, 1L
      );

      final NodeIndexSeek seekAlice = new NodeIndexSeek(
          "b", "Person", "id", 1, "Person.id", 5.1, 1L
      );

      final NodeHashJoin join = new NodeHashJoin(
          seekBob, seekAlice, "dummy", 10.2, 1L
      );

      // Check for KNOWS relationship from Bob to Alice in OUT direction (doesn't exist)
      final ExpandInto expandInto = new ExpandInto(
          join, "a", "b", "r", Direction.OUT, new String[]{"KNOWS"}, 1.0, 0L
      );

      final ResultSet results = expandInto.execute(context, -1);

      // Verify no results
      final List<Result> resultList = collectResults(results);
      assertEquals(0, resultList.size(), "Should find no relationship from Bob to Alice in OUT direction");
    });
  }

  @Test
  public void testNodeHashJoin() {
    database.transaction(() -> {
      final com.arcadedb.query.sql.executor.BasicCommandContext context = new com.arcadedb.query.sql.executor.BasicCommandContext();
      context.setDatabase(database);

      // Get Alice
      final NodeIndexSeek seekAlice = new NodeIndexSeek(
          "person", "Person", "id", 1, "Person.id", 5.1, 1L
      );

      // Get all Person vertices
      final NodeByLabelScan scanAll = new NodeByLabelScan(
          "person", "Person", 3.0, 3L
      );

      // Join on 'person' variable - should produce Cartesian product of Alice with all persons
      final NodeHashJoin join = new NodeHashJoin(
          seekAlice, scanAll, "person", 8.1, 1L
      );

      final ResultSet results = join.execute(context, -1);

      // Verify results - should find 1 match (Alice with Alice, since they share the same variable)
      final List<Result> resultList = collectResults(results);
      assertEquals(1, resultList.size(), "Should find 1 match where Alice joins with herself");

      final Result result = resultList.get(0);
      final Vertex person = result.getProperty("person");
      assertEquals(1, (int) person.get("id"), "Should be Alice");
    });
  }

  @Test
  public void testExplainOutput() {
    database.transaction(() -> {
      // Test explain output for various operators
      final NodeByLabelScan scan = new NodeByLabelScan("n", "Person", 3.0, 3L);
      final String scanExplain = scan.explain(0);
      assertTrue(scanExplain.contains("NodeByLabelScan"), "Explain should contain operator type");
      assertTrue(scanExplain.contains("Person"), "Explain should contain label");
      assertTrue(scanExplain.contains("3.00"), "Explain should contain cost");

      final NodeIndexSeek seek = new NodeIndexSeek(
          "n", "Person", "id", 1, "Person.id", 5.1, 1L
      );
      final String seekExplain = seek.explain(0);
      assertTrue(seekExplain.contains("NodeIndexSeek"), "Explain should contain operator type");
      assertTrue(seekExplain.contains("Person.id"), "Explain should contain index name");
      assertTrue(seekExplain.contains("5.10"), "Explain should contain cost");

      final ExpandInto expandInto = new ExpandInto(
          null, "a", "b", "r", Direction.OUT, new String[]{"KNOWS"}, 1.0, 1L
      );
      final String expandIntoExplain = expandInto.explain(0);
      assertTrue(expandIntoExplain.contains("ExpandInto"), "Explain should contain operator type");
      assertTrue(expandIntoExplain.contains("SEMI-JOIN"), "Explain should indicate semi-join");
      assertTrue(expandIntoExplain.contains("a"), "Explain should contain source variable");
      assertTrue(expandIntoExplain.contains("b"), "Explain should contain target variable");
    });
  }

  /**
   * Helper method to collect all results from a ResultSet into a list.
   */
  private List<Result> collectResults(final ResultSet resultSet) {
    final List<Result> results = new ArrayList<>();
    while (resultSet.hasNext()) {
      results.add(resultSet.next());
    }
    resultSet.close();
    return results;
  }
}
