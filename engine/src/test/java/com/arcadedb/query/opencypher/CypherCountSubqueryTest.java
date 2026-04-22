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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test COUNT { ... } pattern / subquery expression in Cypher queries.
 * See <a href="https://github.com/ArcadeData/arcadedb/issues/3956">issue #3956</a>.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherCountSubqueryTest {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/cyphercount").create();

    database.transaction(() -> {
      database.getSchema().createVertexType("Person");
      database.getSchema().createVertexType("Dog");
      database.getSchema().createEdgeType("OWNS");

      final MutableVertex alice = database.newVertex("Person").set("name", "Alice").save();
      final MutableVertex bob = database.newVertex("Person").set("name", "Bob").save();
      database.newVertex("Person").set("name", "Charlie").save();

      final MutableVertex rex = database.newVertex("Dog").set("name", "Rex").save();
      final MutableVertex fido = database.newVertex("Dog").set("name", "Fido").save();

      alice.newEdge("OWNS", rex, new Object[0]).save();
      bob.newEdge("OWNS", fido, new Object[0]).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  /**
   * Issue #3956: COUNT { (p:Person {name: person.name})-[:OWNS]->(:Dog) } must return the
   * integer number of matches per outer row, not the inner matched node.
   */
  @Test
  void countPatternWithFreshInnerVariable() {
    final ResultSet results = database.query("opencypher",
        "MATCH (person:Person) "
            + "RETURN person.name AS name, "
            + "       COUNT { (p:Person {name: person.name})-[:OWNS]->(:Dog) } AS dogCount "
            + "ORDER BY name");

    final List<Result> rows = collect(results);

    assertThat(rows).hasSize(3);
    assertThat((String) rows.get(0).getProperty("name")).isEqualTo("Alice");
    assertThat(((Number) rows.get(0).getProperty("dogCount")).longValue()).isEqualTo(1L);
    assertThat((String) rows.get(1).getProperty("name")).isEqualTo("Bob");
    assertThat(((Number) rows.get(1).getProperty("dogCount")).longValue()).isEqualTo(1L);
    assertThat((String) rows.get(2).getProperty("name")).isEqualTo("Charlie");
    assertThat(((Number) rows.get(2).getProperty("dogCount")).longValue()).isEqualTo(0L);
  }

  /**
   * Issue #3956 control: COUNT { (person)-[:OWNS]->(:Dog) } reusing the outer variable must
   * produce the same integer counts.
   */
  @Test
  void countPatternReusingOuterVariable() {
    final ResultSet results = database.query("opencypher",
        "MATCH (person:Person) "
            + "RETURN person.name AS name, "
            + "       COUNT { (person)-[:OWNS]->(:Dog) } AS dogCount "
            + "ORDER BY name");

    final List<Result> rows = collect(results);

    assertThat(rows).hasSize(3);
    assertThat(((Number) rows.get(0).getProperty("dogCount")).longValue()).isEqualTo(1L);
    assertThat(((Number) rows.get(1).getProperty("dogCount")).longValue()).isEqualTo(1L);
    assertThat(((Number) rows.get(2).getProperty("dogCount")).longValue()).isEqualTo(0L);
  }

  /**
   * Issue #3955: COUNT { (person)-[:HAS_DOG]->(dog:Dog) WHERE dog.breed = 'Labrador' }
   * pattern with inline WHERE must count only matches satisfying the filter.
   */
  @Test
  void countPatternWithInlineWhere() {
    database.transaction(() -> {
      final MutableVertex labrador = database.newVertex("Dog").set("name", "Rex2").set("breed", "Labrador").save();
      database.newVertex("Dog").set("name", "Fido2").set("breed", "Poodle").save();
      database.newVertex("Dog").set("name", "Spot2").set("breed", "Dalmatian").save();
    });

    final ResultSet results = database.query("opencypher",
        "MATCH (person:Person) "
            + "OPTIONAL MATCH (person)-[:OWNS]->(:Dog) "
            + "RETURN person.name AS name, "
            + "       COUNT { (person)-[:OWNS]->(dog:Dog) WHERE dog.name = 'Rex' } AS rexCount "
            + "ORDER BY name");

    final List<Result> rows = collect(results);

    assertThat(rows).hasSize(3);
    assertThat((String) rows.get(0).getProperty("name")).isEqualTo("Alice");
    assertThat(((Number) rows.get(0).getProperty("rexCount")).longValue()).isEqualTo(1L);
    assertThat((String) rows.get(1).getProperty("name")).isEqualTo("Bob");
    assertThat(((Number) rows.get(1).getProperty("rexCount")).longValue()).isEqualTo(0L);
    assertThat((String) rows.get(2).getProperty("name")).isEqualTo("Charlie");
    assertThat(((Number) rows.get(2).getProperty("rexCount")).longValue()).isEqualTo(0L);
  }

  /**
   * COUNT { ... } with a full inner MATCH clause and WHERE filter.
   */
  @Test
  void countFullMatchSubquery() {
    final ResultSet results = database.query("opencypher",
        "MATCH (person:Person) "
            + "RETURN person.name AS name, "
            + "       COUNT { MATCH (person)-[:OWNS]->(d:Dog) WHERE d.name = 'Rex' } AS rexCount "
            + "ORDER BY name");

    final List<Result> rows = collect(results);

    assertThat(rows).hasSize(3);
    assertThat(((Number) rows.get(0).getProperty("rexCount")).longValue()).isEqualTo(1L);
    assertThat(((Number) rows.get(1).getProperty("rexCount")).longValue()).isEqualTo(0L);
    assertThat(((Number) rows.get(2).getProperty("rexCount")).longValue()).isEqualTo(0L);
  }

  private static List<Result> collect(final ResultSet rs) {
    final List<Result> list = new ArrayList<>();
    while (rs.hasNext())
      list.add(rs.next());
    return list;
  }
}
