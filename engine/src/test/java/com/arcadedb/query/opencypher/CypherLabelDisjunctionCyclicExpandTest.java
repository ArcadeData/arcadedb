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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A hop that ends on the node it started from, where that node carries a label disjunction, answers every alternative: the node is the anchor of the
 * plan, the disjunction scan has already filtered it, and the expand operator's single {@code targetLabel} must not filter it again on the first
 * alternative only. Before, {@code (n:A|C)-[*0..1]->(n)} and {@code (n:A|C)-[:R*1..1]->(n)} dropped the nodes labelled {@code C} only. The expected rows are what
 * Neo4j 5.26 answers for the same graph.
 */
class CypherLabelDisjunctionCyclicExpandTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/CypherLabelDisjunctionCyclicExpandTest");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.transaction(() -> database.command("opencypher",
        "CREATE (a:A {id: 1}), (c:C {id: 2}), (:B {id: 3}), (d:B:C {id: 4}), (a)-[:R]->(a), (c)-[:R]->(c), (d)-[:R]->(d)"));
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  private List<Object> ids(final String query) {
    final List<Object> ids = new ArrayList<>();
    try (final ResultSet resultSet = database.query("opencypher", query)) {
      while (resultSet.hasNext())
        ids.add(((Number) resultSet.next().getProperty("id")).longValue());
    }
    return ids;
  }

  @Test
  void zeroLengthHopOverTheSameDisjunctionNodeKeepsEveryAlternative() {
    assertThat(ids("MATCH (n:A|C)-[*0..0]->(n) RETURN n.id AS id ORDER BY id")).containsExactly(1L, 2L, 4L);
    assertThat(ids("MATCH (n)-[*0..0]->(n:A|C) RETURN n.id AS id ORDER BY id")).containsExactly(1L, 2L, 4L);
    assertThat(ids("MATCH (n:A|C)-[:R*0..1]->(n) RETURN n.id AS id ORDER BY id")).containsExactly(1L, 1L, 2L, 2L, 4L, 4L);
  }

  @Test
  void cyclicVariableLengthHopOverTheSameDisjunctionNodeKeepsEveryAlternative() {
    assertThat(ids("MATCH (n:A|C)-[:R*1..1]->(n) RETURN n.id AS id ORDER BY id")).containsExactly(1L, 2L, 4L);
    assertThat(ids("MATCH (n:A|C)-[:R*1..2]->(n) RETURN n.id AS id ORDER BY id")).containsExactly(1L, 2L, 4L);
  }

  @Test
  void controlsThatAlreadyAnsweredEveryAlternative() {
    assertThat(ids("MATCH (n:A|C)-[:R]->(n) RETURN n.id AS id ORDER BY id")).containsExactly(1L, 2L, 4L);
    assertThat(ids("MATCH (n:A|C)-[*0..0]->(m) RETURN n.id AS id ORDER BY id")).containsExactly(1L, 2L, 4L);
    assertThat(ids("MATCH (n:A|C) RETURN n.id AS id ORDER BY id")).containsExactly(1L, 2L, 4L);
  }
}
