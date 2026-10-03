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
import com.arcadedb.schema.Property;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issues #8999 and #8993: an index on a Cypher property that holds both integers and floats, or that is created before any
 * record has the property, must not change any answer (openCypher compares {@code 3 = 3.0} as true and orders numbers by value).
 * Neo4j returns the same rows with and without an index.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherIndexMixedNumericIssue8999Test {
  private static final String[] RANGE_QUERIES = {
      "MATCH (n:A) WHERE n.x > 2.2 RETURN n.id AS id ORDER BY id",
      "MATCH (n:A) WHERE n.x < 2.7 RETURN n.id AS id ORDER BY id",
      "MATCH (n:A) WHERE n.x > 2 AND n.x < 3 RETURN n.id AS id ORDER BY id",
      "MATCH (n:A) WHERE n.x >= 2.5 RETURN n.id AS id ORDER BY id" };

  private static final String[] EQUALITY_QUERIES = {
      "MATCH (n:A {x: 3}) RETURN n.id AS id ORDER BY id",
      "MATCH (n:A) WHERE n.x = 3 RETURN n.id AS id ORDER BY id",
      "MATCH (n:A) WHERE n.x IN [3] RETURN n.id AS id ORDER BY id",
      "MATCH (n:A) WHERE n.x = 7.0 RETURN n.id AS id ORDER BY id" };

  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/issue8999");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
  }

  @AfterEach
  void teardown() {
    if (database != null && database.isOpen())
      database.drop();
  }

  @Test
  void rangeOverIntegersAndFloatsIndexedAfterTheData() {
    database.transaction(() -> database.command("opencypher", "CREATE (:A {id: 1, x: 3}), (:A {id: 2, x: 2.5}), (:A {id: 3, x: 2})"));
    final List<List<String>> without = run(RANGE_QUERIES);
    assertThat(without.get(0)).containsExactly("1", "2");
    assertThat(without.get(1)).containsExactly("2", "3");
    assertThat(without.get(2)).containsExactly("2");

    database.command("opencypher", "CREATE INDEX FOR (n:A) ON (n.x)");
    assertThat(run(RANGE_QUERIES)).isEqualTo(without);
    // the record still holds the float it was written with
    assertThat(run("MATCH (n:A) WHERE n.id = 2 RETURN n.x AS x")).containsExactly("2.5");
  }

  @Test
  void equalityOverIntegersAndFloatsIndexedBeforeTheData() {
    database.command("opencypher", "CREATE INDEX FOR (n:A) ON (n.x)");
    database.transaction(() -> database.command("opencypher", "CREATE (:A {id: 1, x: 3.0}), (:A {id: 2, x: 7})"));

    for (final String q : EQUALITY_QUERIES)
      assertThat(run(q)).as(q).hasSize(1);
    assertThat(run(EQUALITY_QUERIES[0])).containsExactly("1");
    assertThat(run(EQUALITY_QUERIES[3])).containsExactly("2");
    assertThat(run("MERGE (n:A {x: 3}) ON CREATE SET n.id = 99 RETURN n.id AS id")).containsExactly("1");
    assertThat(run("MATCH (n:A) RETURN count(n) AS c")).containsExactly("2");
  }

  @Test
  void rangeOverNumbersIndexedBeforeTheData() {
    database.command("opencypher", "CREATE INDEX FOR (n:A) ON (n.x)");
    database.transaction(() -> database.command("opencypher", "CREATE (:A {id: 1, x: 3}), (:A {id: 2, x: 2.5}), (:A {id: 3, x: 2}), (:A {id: 4, x: 10})"));
    assertThat(run("MATCH (n:A) WHERE n.x > 2.2 RETURN n.id AS id ORDER BY id")).containsExactly("1", "2", "4");
    assertThat(run("MATCH (n:A) WHERE n.x < 2.7 RETURN n.id AS id ORDER BY id")).containsExactly("2", "3");
    assertThat(run("MATCH (n:A) WHERE n.x > 9 RETURN n.id AS id ORDER BY id")).containsExactly("4");
  }

  @Test
  void stringsStayStringsWhenTheIndexIsCreatedFirst() {
    database.command("opencypher", "CREATE INDEX FOR (n:A) ON (n.x)");
    database.transaction(() -> database.command("opencypher", "CREATE (:A {id: 1, x: 'abc'}), (:A {id: 2, x: 3})"));
    assertThat(run("MATCH (n:A) WHERE n.x = 'abc' RETURN n.id AS id")).containsExactly("1");
    assertThat(run("MATCH (n:A) WHERE n.x = 3 RETURN n.id AS id")).containsExactly("2");
    // Cypher: 3 = '3' is false
    assertThat(run("MATCH (n:A) WHERE n.x = '3' RETURN n.id AS id")).isEmpty();
  }

  @Test
  void failedIndexBuildLeavesNoDeclaredProperty() {
    database.transaction(() -> database.command("opencypher", "CREATE (:B {id: 1, x: 3}), (:B {id: 2, x: 'abc'})"));
    // Neo4j builds the index over integers and strings alike
    database.command("opencypher", "CREATE INDEX FOR (n:B) ON (n.x)");
    assertThat(database.getSchema().getType("B").getAllIndexes(false)).isNotEmpty();
    assertThat(run("MATCH (n:B) WHERE n.x = 3 RETURN n.id AS id")).containsExactly("1");
    assertThat(run("MATCH (n:B) WHERE n.x = 'abc' RETURN n.id AS id")).containsExactly("2");

    database.transaction(() -> database.command("opencypher", "CREATE (:B {id: 3, x: 4.5}), (:B {id: 4, x: 'def'})"));
    assertThat(run("MATCH (n:B) WHERE n.id = 3 RETURN n.x AS x")).containsExactly("4.5");
    assertThat(run("MATCH (n:B) WHERE n.id = 4 RETURN n.x AS x")).containsExactly("def");
    final Property declared = database.getSchema().getType("B").getPropertyIfExists("x");
    assertThat(declared == null || declared.getType().name().equals("STRING")).isTrue();
  }

  private List<List<String>> run(final String[] queries) {
    final List<List<String>> out = new ArrayList<>();
    for (final String q : queries)
      out.add(run(q));
    return out;
  }

  private List<String> run(final String query) {
    final List<String> out = new ArrayList<>();
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", query)) {
        while (rs.hasNext()) {
          final Object value = rs.next().getProperty(firstColumn(query));
          out.add(String.valueOf(value));
        }
      }
    });
    return out;
  }

  private static String firstColumn(final String query) {
    return query.contains("AS c") ? "c" : query.contains("AS x") ? "x" : "id";
  }
}
