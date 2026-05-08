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
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the GQL Quantified Path Patterns (issue #3365 sections 1.4 and 1.5), Phase A.
 * Phase A handles single-relationship grouped patterns lowered to existing variable-length
 * traversal. Multi-relationship grouped patterns and inner-WHERE remain Phase B.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue3365QPPPhaseATest {
  private Database database;
  private String   databasePath;

  @BeforeEach
  void setUp(final TestInfo testInfo) {
    databasePath = "./target/databases/testopencypher-qpp-" + testInfo.getTestMethod().get().getName();
    final DatabaseFactory factory = new DatabaseFactory(databasePath);
    if (factory.exists())
      factory.open().drop();
    database = factory.create();

    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("KNOWS");

    database.transaction(() -> {
      database.command("opencypher",
          "CREATE (a:Person {name:'A'})-[:KNOWS]->(b:Person {name:'B'})-[:KNOWS]->(c:Person {name:'C'})-[:KNOWS]->(d:Person {name:'D'})");
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
  void groupedPlusEquivalentToVarLength() {
    final ResultSet qpp = database.query("opencypher",
        "MATCH (a:Person {name:'A'})-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person)-[:KNOWS]->(d:Person {name:'D'}) RETURN d.name AS name");
    assertThat(qpp.hasNext()).isTrue();

    final ResultSet grouped = database.query("opencypher",
        "MATCH (a:Person {name:'A'})((:Person)-[:KNOWS]->(:Person))+(d:Person {name:'D'}) RETURN d.name AS name");
    assertThat(grouped.hasNext()).isTrue();
    assertThat((String) grouped.next().getProperty("name")).isEqualTo("D");
  }

  @Test
  void groupedRangeQuantifier() {
    final ResultSet result = database.query("opencypher",
        "MATCH (a:Person {name:'A'})((:Person)-[:KNOWS]->(:Person)){1,2}(end:Person) RETURN end.name AS name ORDER BY name");
    int count = 0;
    while (result.hasNext()) {
      result.next();
      count++;
    }
    assertThat(count).isEqualTo(2);
  }

  @Test
  void groupedExactQuantifier() {
    final ResultSet result = database.query("opencypher",
        "MATCH (a:Person {name:'A'})((:Person)-[:KNOWS]->(:Person)){2}(end:Person) RETURN end.name AS name");
    assertThat(result.hasNext()).isTrue();
    assertThat((String) result.next().getProperty("name")).isEqualTo("C");
  }

  @Test
  void groupedRejectsZeroQuantifier() {
    assertThatThrownBy(() -> database.query("opencypher",
        "MATCH (a:Person {name:'A'})((:Person)-[:KNOWS]->(:Person)){0}(end:Person) RETURN end"))
        .isInstanceOf(CommandParsingException.class);
  }

  @Test
  void groupedRejectsInnerWhere() {
    assertThatThrownBy(() -> database.query("opencypher",
        "MATCH (a:Person {name:'A'})((:Person)-[:KNOWS]->(b:Person) WHERE b.name <> 'X')+(end:Person) RETURN end"))
        .isInstanceOf(CommandParsingException.class);
  }

  @Test
  void groupedRejectsMultiRelInner() {
    assertThatThrownBy(() -> database.query("opencypher",
        "MATCH (a:Person {name:'A'})((:Person)-[:KNOWS]->(:Person)-[:KNOWS]->(:Person))+(end:Person) RETURN end"))
        .isInstanceOf(CommandParsingException.class);
  }
}
