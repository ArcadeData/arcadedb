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
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Reproduces issue #5094: count(*) after OPTIONAL MATCH drops the null-preserving rows,
 * returning count(m) instead of the total number of rows.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5094OptionalMatchCountStarTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/testissue5094").create();
    database.transaction(() -> {
      database.getSchema().createVertexType("BugNode");
      database.getSchema().createEdgeType("LINK");

      final MutableVertex n1 = database.newVertex("BugNode").set("id", 1).save();
      final MutableVertex n2 = database.newVertex("BugNode").set("id", 2).save();
      final MutableVertex n3 = database.newVertex("BugNode").set("id", 3).save();
      final MutableVertex n4 = database.newVertex("BugNode").set("id", 4).save();
      final MutableVertex n5 = database.newVertex("BugNode").set("id", 5).save();

      // n1 has 2 outgoing LINK edges, n2 and n4 have 1 each, n3 and n5 have none
      n1.newEdge("LINK", n2);
      n1.newEdge("LINK", n3);
      n4.newEdge("LINK", n5);
      n2.newEdge("LINK", n4);
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
  void countStarAfterOptionalMatchCountsNullRows() {
    // 4 matched rows (n1->n2, n1->n3, n2->n4, n4->n5) + 2 null rows (n3, n5) = 6
    final ResultSet rs = database.query("opencypher",
        "MATCH (n:BugNode) OPTIONAL MATCH (n)-[:LINK]->(m:BugNode) RETURN count(*) AS cnt");

    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Long>getProperty("cnt")).isEqualTo(6L);
    assertThat(rs.hasNext()).isFalse();
    rs.close();
  }

  @Test
  void countStarAndCountVariableTogether() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (n:BugNode) OPTIONAL MATCH (n)-[:LINK]->(m:BugNode) RETURN count(*) AS rowCount, count(m) AS matchedCount");

    assertThat(rs.hasNext()).isTrue();
    final var row = rs.next();
    assertThat(row.<Long>getProperty("rowCount")).isEqualTo(6L);
    assertThat(row.<Long>getProperty("matchedCount")).isEqualTo(4L);
    rs.close();
  }

  @Test
  void countOptionalVariableExcludesNulls() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (n:BugNode) OPTIONAL MATCH (n)-[:LINK]->(m:BugNode) RETURN count(m) AS cnt");

    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Long>getProperty("cnt")).isEqualTo(4L);
    rs.close();
  }

  @Test
  void rowMaterializationPreservesNullRows() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (n:BugNode) OPTIONAL MATCH (n)-[:LINK]->(m:BugNode) RETURN n.id AS nId, m.id AS mId");

    int rows = 0;
    int nullRows = 0;
    while (rs.hasNext()) {
      final var row = rs.next();
      rows++;
      if (row.getProperty("mId") == null)
        nullRows++;
    }
    assertThat(rows).isEqualTo(6);
    assertThat(nullRows).isEqualTo(2);
    rs.close();
  }

  @Test
  void countStarWithoutOptionalMatchStillWorks() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (n:BugNode) RETURN count(*) AS cnt");

    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Long>getProperty("cnt")).isEqualTo(5L);
    rs.close();
  }
}
