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
}
