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
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Nested;

/**
 * Regression tests for issue #4909: a nested parameter field access ($param.field) used as a property
 * value inside a MATCH pattern must resolve, so the pattern binds. Previously the pattern matched
 * nothing (no error), which broke single edge saves in the graphiti driver.
 */
class NestedParameterInMatchTest {

  @Test
  void nestedParameterFieldInMatchPatternResolves() {
    withDatabase(database -> {
      database.transaction(() -> database.command("opencypher", "CREATE (n:Entity {uuid: 'u1', name: 'Alice'})"));

      final ResultSet rs = database.query("opencypher",
          "MATCH (n:Entity {uuid: $data.uuid}) RETURN n.name AS name",
          Map.of("data", Map.of("uuid", "u1")));

      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<String>getProperty("name")).isEqualTo("Alice");
    });
  }

  @Test
  void nestedParameterFieldResolvesWhenIndexExists() {
    withDatabase(database -> {
      // Force the index-resolution path (tryFindAndUseIndex) rather than the full scan.
      database.command("sql", "CREATE PROPERTY Entity.uuid STRING");
      database.command("sql", "CREATE INDEX ON Entity (uuid) UNIQUE");
      database.transaction(() -> database.command("opencypher", "CREATE (n:Entity {uuid: 'u1', name: 'Bob'})"));

      final ResultSet rs = database.query("opencypher",
          "MATCH (n:Entity {uuid: $data.uuid}) RETURN n.name AS name",
          Map.of("data", Map.of("uuid", "u1")));

      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<String>getProperty("name")).isEqualTo("Bob");
    });
  }

  @Test
  void nestedParameterEdgeSaveCreatesRelationship() {
    withDatabase(database -> {
      database.transaction(() -> {
        database.command("opencypher", "CREATE (n:Entity {uuid: 'src'})");
        database.command("opencypher", "CREATE (n:Entity {uuid: 'dst'})");
      });

      // The exact graphiti single-edge-save shape, without the UNWIND workaround.
      database.transaction(() -> database.command("opencypher",
          """
              MATCH (source:Entity {uuid: $edge_data.source_uuid})
              MATCH (target:Entity {uuid: $edge_data.target_uuid})
              MERGE (source)-[e:RELATES_TO {uuid: $edge_data.uuid}]->(target)
              RETURN e.uuid AS uuid""",
          Map.of("edge_data", Map.of("source_uuid", "src", "target_uuid", "dst", "uuid", "edge-1"))));

      final ResultSet rs = database.query("opencypher",
          "MATCH (:Entity {uuid: 'src'})-[e:RELATES_TO]->(:Entity {uuid: 'dst'}) RETURN e.uuid AS uuid");
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<String>getProperty("uuid")).isEqualTo("edge-1");
    });
  }

  private static void withDatabase(final Consumer<Database> test) {
    final String path = "./target/testnestedparam_" + System.nanoTime();
    final Database database = new DatabaseFactory(path).create();
    try {
      database.getSchema().getOrCreateVertexType("Entity");
      test.accept(database);
    } finally {
      database.drop();
    }
  }

  /**
   * Issue #11480: a field of a parameter used as the value of an equality on an indexed property, {@code MATCH (n:Entity {uuid: $d.u})}
   * or {@code WHERE n.uuid = $d.u}, was planned as a scan of the whole label with the equality as a filter, while the same lookup with a
   * flat parameter sought the index. The cost-based planner accepted only a literal or a bare parameter as a seek value; it now takes
   * any value that reads no variable and is the same on every row, the rule the relationship seek already followed, and the seek
   * evaluates it once per execution. The plan is asserted, not only the rows: a label scan returns the same rows, which is how the
   * regression of 26.8.1 went unnoticed by the test of #4909.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue11480NestedParameterIndexSeek extends TestHelper {
    private static final int VERTICES = 1_000;

    @Override
    protected void beginTest() {
      database.transaction(() -> {
        final VertexType entity = database.getSchema().createVertexType("Entity");
        entity.createProperty("uuid", Type.STRING);
        entity.createProperty("rank", Type.INTEGER);
        database.getSchema().buildTypeIndex("Entity", new String[] { "uuid" }).withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true)
            .withIndexName("entity_uuid").create();
        database.getSchema().buildTypeIndex("Entity", new String[] { "rank" }).withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false)
            .withIndexName("entity_rank").create();
        database.getSchema().createEdgeType("RELATES_TO");
      });
      database.transaction(() -> database.command("opencypher",
          "UNWIND range(1, $n) AS i CREATE (:Entity {uuid: 'u' + toString(i), name: 'n' + toString(i), rank: i})",
          Map.of("n", VERTICES)));
    }

    @Test
    void inlineMapWithAFieldOfAParameterSeeksTheIndex() {
      assertSeeks("MATCH (n:Entity {uuid: $d.u}) RETURN n.name AS name", Map.of("d", Map.of("u", "u500")), "n500");
    }

    @Test
    void whereEqualityWithAFieldOfAParameterSeeksTheIndex() {
      assertSeeks("MATCH (n:Entity) WHERE n.uuid = $d.u RETURN n.name AS name", Map.of("d", Map.of("u", "u500")), "n500");
    }

    @Test
    void reversedEqualitySeeksTheIndex() {
      assertSeeks("MATCH (n:Entity) WHERE $d.u = n.uuid RETURN n.name AS name", Map.of("d", Map.of("u", "u7")), "n7");
    }

    @Test
    void deeperFieldsAndSubscriptsSeekTheIndex() {
      assertSeeks("MATCH (n:Entity {uuid: $d.edge.source}) RETURN n.name AS name",
          Map.of("d", Map.of("edge", Map.of("source", "u3"))), "n3");
      assertSeeks("MATCH (n:Entity) WHERE n.uuid = $d['u'] RETURN n.name AS name", Map.of("d", Map.of("u", "u4")), "n4");
      assertSeeks("MATCH (n:Entity) WHERE n.uuid = $ids[1] RETURN n.name AS name", Map.of("ids", List.of("u1", "u5")), "n5");
    }

    @Test
    void aComputedValueStaysAFilter() {
      // A computed value reaches an index as a Cypher value, whose key conversion is not Cypher's equality for every temporal
      // implementation and declared type: the node seek is built from what the caller passed, a parameter or a part of one
      final List<String> names = new ArrayList<>();
      final String plan = run("MATCH (n:Entity) WHERE n.uuid = toLower($d.u) RETURN n.name AS name", Map.of("d", Map.of("u", "U9")),
          names);
      assertThat(plan).doesNotContain("NodeIndexSeek");
      assertThat(names).containsExactly("n9");
    }

    @Test
    void inListOfFieldsOfAParameterSeeksTheIndex() {
      assertSeeks("MATCH (n:Entity) WHERE n.uuid IN [$d.a, $d.b] RETURN n.name AS name ORDER BY name",
          Map.of("d", Map.of("a", "u10", "b", "u11")), "n10", "n11");
      // A field holding the whole list: one key per item, as IN $ids seeks
      assertSeeks("MATCH (n:Entity) WHERE n.uuid IN $d.ids RETURN n.name AS name ORDER BY name",
          Map.of("d", Map.of("ids", List.of("u12", "u13", "missing"))), "n12", "n13");
    }

    @Test
    void rangeBoundFromAFieldOfAParameterScansTheIndexRange() {
      final List<String> names = new ArrayList<>();
      final String plan = run("MATCH (n:Entity) WHERE n.rank >= $d.from AND n.rank < $d.to RETURN n.name AS name ORDER BY name",
          Map.of("d", Map.of("from", 20, "to", 23)), names);
      assertThat(plan).contains("NodeIndexRangeScan").doesNotContain("NodeByLabelScan");
      assertThat(names).containsExactly("n20", "n21", "n22");
    }

    @Test
    void aMissingFieldMatchesNothing() {
      assertSeeks("MATCH (n:Entity {uuid: $d.missing}) RETURN n.name AS name", Map.of("d", Map.of("u", "u1")));
    }

    @Test
    void aNonDeterministicValueIsNotASeekKey() {
      // rand() answers differently on every row, so the equality cannot be decided once for the whole label
      final List<String> names = new ArrayList<>();
      final String plan = run("MATCH (n:Entity) WHERE n.uuid = 'u' + toString(toInteger(rand() * 0) + 2) RETURN n.name AS name",
          Map.of(), names);
      assertThat(plan).doesNotContain("NodeIndexSeek");
      assertThat(names).containsExactly("n2");
    }

    @Test
    void theGraphitiEdgeSaveFindsBothEndpointsByIndexAndMergesOnce() {
      final Map<String, Object> edgeData = Map.of("source_uuid", "u1", "target_uuid", "u2", "uuid", "e1", "fact", "f");
      final String save = "MATCH (source:Entity {uuid: $edge_data.source_uuid}) MATCH (target:Entity {uuid: $edge_data.target_uuid}) "
          + "MERGE (source)-[e:RELATES_TO {uuid: $edge_data.uuid}]->(target) SET e = $edge_data RETURN e.uuid AS uuid";
      for (int i = 0; i < 2; i++)
        database.transaction(() -> {
          try (final ResultSet rs = database.command("opencypher", save, Map.of("edge_data", edgeData))) {
            assertThat(rs.next().<String>getProperty("uuid")).isEqualTo("e1");
          }
        });

      try (final ResultSet rs = database.query("opencypher",
          "EXPLAIN MATCH (source:Entity {uuid: $edge_data.source_uuid}) MATCH (target:Entity {uuid: $edge_data.target_uuid}) RETURN source, target",
          Map.of("edge_data", edgeData))) {
        assertThat(rs.getExecutionPlan().get().prettyPrint(0, 2)).contains("NodeIndexSeek").doesNotContain("NodeByLabelScan");
      }
      try (final ResultSet rs = database.query("opencypher", "MATCH ()-[e:RELATES_TO]->() RETURN count(e) AS c")) {
        assertThat(rs.next().<Long>getProperty("c")).isEqualTo(1L);
      }
    }

    private void assertSeeks(final String query, final Map<String, Object> parameters, final String... expectedNames) {
      final List<String> names = new ArrayList<>();
      final String plan = run(query, parameters, names);
      assertThat(plan).contains("NodeIndexSeek").doesNotContain("NodeByLabelScan");
      assertThat(names).containsExactly(expectedNames);
    }

    private String run(final String query, final Map<String, Object> parameters, final List<String> names) {
      try (final ResultSet rs = database.query("opencypher", "PROFILE " + query, parameters)) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          names.add(row.getProperty("name"));
        }
        return rs.getExecutionPlan().get().prettyPrint(0, 2);
      }
    }
  }
}
