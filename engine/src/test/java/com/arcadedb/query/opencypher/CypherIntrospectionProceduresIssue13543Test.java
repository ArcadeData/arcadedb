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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issues #13542 and #13543, the introspection procedures Neo4j clients use to learn about a database. {@code db.propertyKeys()}
 * listed only the properties a schema declares, so it answered nothing for the data Cypher writes, which declares nothing;
 * {@code db.info}, {@code db.schema.nodeTypeProperties} and {@code db.schema.relTypeProperties} were not defined. The expected
 * answers are what Neo4j 5.26 answers for the same data.
 */
class CypherIntrospectionProceduresIssue13543Test {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/cypher-introspection-13543").create();
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void propertyKeysListsThePropertiesTheDataCarriesWithoutADeclaration() {
    database.command("opencypher", "CREATE (:Person {pk_name: 'a', pk_age: 1})-[:KNOWS {pk_since: 2020}]->(:Person {pk_name: 'b'})");

    assertThat(column("CALL db.propertyKeys() YIELD propertyKey RETURN propertyKey", "propertyKey"))
        .containsExactlyInAnyOrder("pk_name", "pk_age", "pk_since");
  }

  @Test
  void propertyKeysListsDeclaredAndUndeclaredPropertiesOnceEach() {
    database.command("sql", "CREATE VERTEX TYPE Person");
    database.command("sql", "CREATE PROPERTY Person.declared STRING");
    database.command("opencypher", "CREATE (:Person {declared: 'x', undeclared: 'y'})");

    final List<String> keys = column("CALL db.propertyKeys() YIELD propertyKey RETURN propertyKey", "propertyKey");
    assertThat(keys).containsExactly("declared", "undeclared");
  }

  @Test
  void propertyKeysReadsTheNodesOfAMultiLabelType() {
    database.command("opencypher", "CREATE (:A:B {only_on_a_and_b: 1})");

    assertThat(column("CALL db.propertyKeys() YIELD propertyKey RETURN propertyKey", "propertyKey")).containsExactly("only_on_a_and_b");
  }

  @Test
  void propertyKeysOfAnEmptyDatabaseIsEmpty() {
    assertThat(column("CALL db.propertyKeys() YIELD propertyKey RETURN propertyKey", "propertyKey")).isEmpty();
  }

  @Test
  void dbInfoNamesTheDatabase() {
    try (final ResultSet rows = database.query("opencypher", "CALL db.info() YIELD id, name, creationDate RETURN id, name, creationDate")) {
      final Result row = rows.next();
      assertThat(row.<String>getProperty("name")).isEqualTo(database.getName());
      assertThat(row.<String>getProperty("id")).isNotBlank();
      assertThat(row.<String>getProperty("creationDate")).isNotNull();
      assertThat(rows.hasNext()).isFalse();
    }
  }

  @Test
  void nodeTypePropertiesReportsWhatTheNodesCarry() {
    database.command("opencypher", """
        CREATE (:Person {name: 'a', age: 1, tags: ['x'], born: date('2000-01-01')}), (:Person {name: 'b'}), (:Empty), (:A:B {x: 1.5})""");

    final List<Map<String, Object>> rows = rows("""
        CALL db.schema.nodeTypeProperties() YIELD nodeType, nodeLabels, propertyName, propertyTypes, mandatory
        RETURN nodeType, nodeLabels, propertyName, propertyTypes, mandatory ORDER BY nodeType, propertyName""");

    assertThat(rows).containsExactly(
        row("nodeType", ":`A`:`B`", "nodeLabels", List.of("A", "B"), "propertyName", "x", "propertyTypes", List.of("Double"), "mandatory", true),
        row("nodeType", ":`Empty`", "nodeLabels", List.of("Empty"), "propertyName", null, "propertyTypes", null, "mandatory", false),
        row("nodeType", ":`Person`", "nodeLabels", List.of("Person"), "propertyName", "age", "propertyTypes", List.of("Long"), "mandatory", false),
        row("nodeType", ":`Person`", "nodeLabels", List.of("Person"), "propertyName", "born", "propertyTypes", List.of("Date"), "mandatory", false),
        row("nodeType", ":`Person`", "nodeLabels", List.of("Person"), "propertyName", "name", "propertyTypes", List.of("String"), "mandatory", true),
        row("nodeType", ":`Person`", "nodeLabels", List.of("Person"), "propertyName", "tags", "propertyTypes", List.of("StringArray"), "mandatory", false));
  }

  @Test
  void nodeTypePropertiesDoesNotListADeclaredPropertyNoNodeHolds() {
    database.command("sql", "CREATE VERTEX TYPE Person");
    database.command("sql", "CREATE PROPERTY Person.declared STRING");
    database.command("opencypher", "CREATE (:Person {name: 'a'})");

    assertThat(column("CALL db.schema.nodeTypeProperties() YIELD propertyName RETURN propertyName", "propertyName"))
        .containsExactly("name");
  }

  @Test
  void relTypePropertiesReportsWhatTheRelationshipsCarryWhateverTheyJoin() {
    database.command("opencypher", """
        CREATE (a:Person)-[:KNOWS {since: 2020, w: 1}]->(b:Person), (a)-[:KNOWS {since: 2021}]->(:Company), (a)-[:LIKES]->(b)""");

    final List<Map<String, Object>> rows = rows("""
        CALL db.schema.relTypeProperties() YIELD relType, propertyName, propertyTypes, mandatory
        RETURN relType, propertyName, propertyTypes, mandatory ORDER BY relType, propertyName""");

    assertThat(rows).containsExactly(
        row("relType", ":`KNOWS`", "propertyName", "since", "propertyTypes", List.of("Long"), "mandatory", true),
        row("relType", ":`KNOWS`", "propertyName", "w", "propertyTypes", List.of("Long"), "mandatory", false),
        row("relType", ":`LIKES`", "propertyName", null, "propertyTypes", null, "mandatory", false));
  }

  private List<String> column(final String statement, final String name) {
    final List<String> values = new ArrayList<>();
    try (final ResultSet rows = database.query("opencypher", statement)) {
      while (rows.hasNext())
        values.add(rows.next().getProperty(name));
    }
    return values;
  }

  private List<Map<String, Object>> rows(final String statement) {
    final List<Map<String, Object>> rows = new ArrayList<>();
    try (final ResultSet result = database.query("opencypher", statement)) {
      while (result.hasNext()) {
        final Result next = result.next();
        final Map<String, Object> row = new LinkedHashMap<>();
        for (final String name : next.getPropertyNames())
          row.put(name, next.getProperty(name));
        rows.add(row);
      }
    }
    return rows;
  }

  private static Map<String, Object> row(final Object... keyValues) {
    final Map<String, Object> row = new LinkedHashMap<>();
    for (int i = 0; i < keyValues.length; i += 2)
      row.put((String) keyValues[i], keyValues[i + 1]);
    return row;
  }
}
