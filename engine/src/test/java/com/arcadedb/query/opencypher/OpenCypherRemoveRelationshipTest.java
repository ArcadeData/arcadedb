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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #5013: Cypher REMOVE on a relationship property is ignored unless the
 * relationship is projected through a WITH clause.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class OpenCypherRemoveRelationshipTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/testopencypher-remove-rel").create();
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  private void seed() {
    database.command("opencypher", """
        CREATE
          (a1:BugRemUser {gid: 1, active: true}),
          (b1:BugRemUser {gid: 11, active: false}),
          (a2:BugRemUser {gid: 2, active: true}),
          (b2:BugRemUser {gid: 12, active: false}),
          (a3:BugRemUser {gid: 3, active: true}),
          (b3:BugRemUser {gid: 13, active: false}),
          (a1)-[:BUG_REM {marked: true}]->(b1),
          (a2)-[:BUG_REM {marked: true}]->(b2),
          (a3)-[:BUG_REM {marked: true}]->(b3),
          (b1)-[:BUG_REM {marked: true}]->(a1)""");
  }

  private long[] countMarked() {
    final ResultSet rs = database.query("opencypher",
        "MATCH ()-[r:BUG_REM]->() RETURN count(r) AS total, count(r.marked) AS marked");
    assertThat(rs.hasNext()).isTrue();
    final Result row = rs.next();
    return new long[] { ((Number) row.getProperty("total")).longValue(), ((Number) row.getProperty("marked")).longValue() };
  }

  @Test
  void directRemoveOnRelationshipProperty() {
    seed();

    database.command("opencypher", """
        MATCH (a:BugRemUser {active: true})-[r:BUG_REM]->(b:BugRemUser {active: false})
        REMOVE r.marked""");

    final long[] counts = countMarked();
    assertThat(counts[0]).isEqualTo(4);  // total relationships
    assertThat(counts[1]).isEqualTo(1);  // only the control relationship b1->a1 keeps marked
  }

  @Test
  void removeOnRelationshipPropertyWithProjection() {
    seed();

    database.command("opencypher", """
        MATCH (a:BugRemUser {active: true})-[r:BUG_REM]->(b:BugRemUser {active: false})
        WITH r
        REMOVE r.marked""");

    final long[] counts = countMarked();
    assertThat(counts[0]).isEqualTo(4);
    assertThat(counts[1]).isEqualTo(1);
  }
}
