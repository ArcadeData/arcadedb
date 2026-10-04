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
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9205: an {@code OPTIONAL MATCH ... WHERE false WITH *} barrier in front of a {@code CALL (*)} subquery
 * changed the number of rows the query returned.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9205OptionalMatchBarrierCallStarTest {
  private static final String PATH = "./target/databases/issue-9205";
  private Database database;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory(PATH);
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Seed");
    database.getSchema().createVertexType("Person");
    database.getSchema().createVertexType("NoMatch");
    database.transaction(() -> database.command("opencypher", "CREATE (:Seed {id: 1})"));
  }

  @AfterEach
  void tearDown() {
    if (database != null && database.isOpen())
      database.drop();
  }

  private int count(final String query) {
    int rows = 0;
    database.begin();
    try (final ResultSet rs = database.command("opencypher", query)) {
      while (rs.hasNext()) {
        rs.next();
        rows++;
      }
    }
    database.commit();
    return rows;
  }

  private static final String CALL = """
      CALL (*) {
        OPTIONAL MATCH () WHERE EXISTS { MATCH (m) }
        RETURN 0 AS marker
      }
      RETURN alias0""";

  /** Control: this shape already passed before the fix. */
  @Test
  void callStarWithoutOptionalMatchBarrier() {
    assertThat(count("CREATE (alias0:Person) " + CALL)).isEqualTo(2);
  }

  @Test
  void callStarAfterOptionalMatchBarrier() {
    assertThat(count("CREATE (alias0:Person) OPTIONAL MATCH (:NoMatch) WHERE false WITH * " + CALL)).isEqualTo(2);
  }

  @Test
  void returnsTheCreatedNodeAndKeepsUserVariablesVisibleInsideTheBody() {
    database.begin();
    final List<Object[]> rows = new ArrayList<>();
    try (final ResultSet rs = database.command("opencypher", """
        CREATE (alias0:Person {name: 'p'})
        OPTIONAL MATCH (:NoMatch) WHERE false
        WITH *
        CALL (*) {
          OPTIONAL MATCH () WHERE EXISTS { MATCH (m) }
          RETURN alias0.name AS seen
        }
        RETURN alias0.name AS name, seen""")) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        rows.add(new Object[] { r.getProperty("name"), r.getProperty("seen") });
      }
    }
    database.commit();
    assertThat(rows).hasSize(2);
    for (final Object[] row : rows) {
      assertThat(row[0]).isEqualTo("p");
      assertThat(row[1]).isEqualTo("p");
    }
  }

  @Test
  void generatedBindingsOfTheBodyDoNotReplaceTheOuterOnes() {
    // the outer MATCH () and the inner MATCH () both use generated names; the outer rows must survive the call
    assertThat(count("MATCH () WITH * CALL (*) { MATCH () RETURN 1 AS x } RETURN x")).isEqualTo(1);
    assertThat(count("MATCH () OPTIONAL MATCH (:NoMatch) WHERE false WITH * CALL (*) { MATCH () RETURN 1 AS x } MATCH () RETURN x")).isEqualTo(1);
  }

  @Test
  void explicitImportAfterTheBarrierIsUnchanged() {
    assertThat(count("""
        CREATE (alias0:Person) OPTIONAL MATCH (:NoMatch) WHERE false WITH *
        CALL (alias0) { OPTIONAL MATCH () WHERE EXISTS { MATCH (m) } RETURN 0 AS marker }
        RETURN alias0""")).isEqualTo(2);
  }

  @Test
  void theGraphIsLeftUntouched() {
    count("CREATE (alias0:Person) OPTIONAL MATCH (:NoMatch) WHERE false WITH * " + CALL);
    assertThat(database.countType("Person", true) + database.countType("Seed", true)).isEqualTo(2);
  }
}
