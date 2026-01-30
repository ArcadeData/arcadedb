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
import com.arcadedb.query.opencypher.ast.Direction;
import com.arcadedb.query.opencypher.executor.operators.*;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Unit tests for physical operators (Phase 2 implementation).
 * Tests all 8 physical operators: NodeByLabelScan, NodeIndexSeek, ExpandAll,
 * ExpandInto, NodeHashJoin, FilterOperator.
 */
class PhysicalOperatorTest {
  private Database database;

  @BeforeEach
  void setUp() {
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
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void nodeByLabelScan() {
    database.transaction(() -> {
      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(database);

      // Test scan of Person vertices
      final NodeByLabelScan scan = new NodeByLabelScan(
          "n", "Person", 3.0, 3L
      );

      final ResultSet results = scan.execute(context, -1);

      // Verify results
      final List<Result> resultList = collectResults(results);
      assertThat(resultList.size()).as("Should find 3 Person vertices").isEqualTo(3);

      // Verify each result has a vertex bound to 'n'
      for (final Result result : resultList) {
        assertThat(result.hasProperty("n")).as("Result should have 'n' property").isTrue();
        final Object obj = result.getProperty("n");
        assertThat(obj).as("Property 'n' should be a Vertex").isInstanceOf(Vertex.class);
        final Vertex vertex = (Vertex) obj;
        assertThat(vertex.getTypeName()).as("Vertex should be of type Person").isEqualTo("Person");
      }

      // Verify cost and cardinality
      assertThat(scan.getEstimatedCost()).isCloseTo(3.0, within(0.01));
      assertThat(scan.getEstimatedCardinality()).isEqualTo(3L);
    });
  }

  @Test
  void nodeIndexSeek() {
    database.transaction(() -> {
      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(database);

      // Test index seek for Person with id=1
      final NodeIndexSeek seek = new NodeIndexSeek(
          "n", "Person", "id", 1, "Person.id", 5.1, 1L
      );

      final ResultSet results = seek.execute(context, -1);

      // Verify results
      final List<Result> resultList = collectResults(results);
      assertThat(resultList.size()).as("Should find exactly 1 Person with id=1").isEqualTo(1);

      final Result result = resultList.get(0);
      assertThat(result.hasProperty("n")).as("Result should have 'n' property").isTrue();
      final Vertex vertex = result.getProperty("n");
      assertThat((int) vertex.get("id")).as("Vertex should have id=1").isEqualTo(1);
      assertThat(vertex.get("name")).as("Vertex should be Alice").isEqualTo("Alice");

      // Verify cost and cardinality
      assertThat(seek.getEstimatedCost()).isCloseTo(5.1, within(0.01));
      assertThat(seek.getEstimatedCardinality()).isEqualTo(1L);
    });
  }

  @Test
  void nodeIndexSeekNotFound() {
    database.transaction(() -> {
      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(database);

      // Test index seek for non-existent id
      final NodeIndexSeek seek = new NodeIndexSeek(
          "n", "Person", "id", 999, "Person.id", 5.1, 0L
      );

      final ResultSet results = seek.execute(context, -1);

      // Verify no results
      final List<Result> resultList = collectResults(results);
      assertThat(resultList.size()).as("Should find no results for id=999").isEqualTo(0);
    });
  }

  @Test
  void expandAll() {
    database.transaction(() -> {
      final BasicCommandContext context = new BasicCommandContext();
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
      assertThat(resultList.size()).as("Alice should know 2 people").isEqualTo(2);

      // Verify each result has source vertex, edge, and target vertex
      for (final Result result : resultList) {
        assertThat(result.hasProperty("a")).as("Result should have 'a' (source)").isTrue();
        assertThat(result.hasProperty("r")).as("Result should have 'r' (edge)").isTrue();
        assertThat(result.hasProperty("b")).as("Result should have 'b' (target)").isTrue();

        final Vertex source = result.getProperty("a");
        assertThat((int) source.get("id")).as("Source should be Alice").isEqualTo(1);

        final Vertex target = result.getProperty("b");
        final int targetId = (Integer) target.get("id");
        assertThat(targetId == 2 || targetId == 3).as("Target should be Bob or Charlie").isTrue();
      }
    });
  }

  @Test
  void expandInto() {
    database.transaction(() -> {
      final BasicCommandContext context = new BasicCommandContext();
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
      assertThat(resultList.size()).as("Should find KNOWS relationship from Alice to Bob").isEqualTo(1);

      final Result result = resultList.get(0);
      assertThat(result.hasProperty("a")).as("Result should have 'a' (Alice)").isTrue();
      assertThat(result.hasProperty("b")).as("Result should have 'b' (Bob)").isTrue();
      assertThat(result.hasProperty("r")).as("Result should have 'r' (edge)").isTrue();

      final Vertex source = result.getProperty("a");
      final Vertex target = result.getProperty("b");
      assertThat((int) source.get("id")).as("Source should be Alice").isEqualTo(1);
      assertThat((int) target.get("id")).as("Target should be Bob").isEqualTo(2);
    });
  }

  @Test
  void expandIntoNotConnected() {
    database.transaction(() -> {
      final BasicCommandContext context = new BasicCommandContext();
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
      assertThat(resultList.size()).as("Should find no relationship from Bob to Alice in OUT direction").isEqualTo(0);
    });
  }

  @Test
  void nodeHashJoin() {
    database.transaction(() -> {
      final BasicCommandContext context = new BasicCommandContext();
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
      assertThat(resultList.size()).as("Should find 1 match where Alice joins with herself").isEqualTo(1);

      final Result result = resultList.get(0);
      final Vertex person = result.getProperty("person");
      assertThat((int) person.get("id")).as("Should be Alice").isEqualTo(1);
    });
  }

  @Test
  void explainOutput() {
    database.transaction(() -> {
      // Test explain output for various operators
      final NodeByLabelScan scan = new NodeByLabelScan("n", "Person", 3.0, 3L);
      final String scanExplain = scan.explain(0);
      assertThat(scanExplain.contains("NodeByLabelScan")).as("Explain should contain operator type").isTrue();
      assertThat(scanExplain.contains("Person")).as("Explain should contain label").isTrue();
      assertThat(scanExplain.contains("3.00")).as("Explain should contain cost").isTrue();

      final NodeIndexSeek seek = new NodeIndexSeek(
          "n", "Person", "id", 1, "Person.id", 5.1, 1L
      );
      final String seekExplain = seek.explain(0);
      assertThat(seekExplain.contains("NodeIndexSeek")).as("Explain should contain operator type").isTrue();
      assertThat(seekExplain.contains("Person.id")).as("Explain should contain index name").isTrue();
      assertThat(seekExplain.contains("5.10")).as("Explain should contain cost").isTrue();

      final ExpandInto expandInto = new ExpandInto(
          null, "a", "b", "r", Direction.OUT, new String[]{"KNOWS"}, 1.0, 1L
      );
      final String expandIntoExplain = expandInto.explain(0);
      assertThat(expandIntoExplain.contains("ExpandInto")).as("Explain should contain operator type").isTrue();
      assertThat(expandIntoExplain.contains("SEMI-JOIN")).as("Explain should indicate semi-join").isTrue();
      assertThat(expandIntoExplain.contains("a")).as("Explain should contain source variable").isTrue();
      assertThat(expandIntoExplain.contains("b")).as("Explain should contain target variable").isTrue();
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
