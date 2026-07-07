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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Reproduces issue #5084: {@code count(n)} on a multi-label node pattern
 * {@code (n:BugA:BugB)} returns an inflated count because the O(1) type-count
 * optimization only considered the first label. Row materialization for the
 * same pattern was already correct.
 * <p>
 * Label distribution:
 * <ul>
 *   <li>id = 1: BugA + BugB</li>
 *   <li>id = 2: BugA only</li>
 *   <li>id = 3: BugB only</li>
 *   <li>id = 4: BugA + BugB</li>
 * </ul>
 * Only nodes id = 1 and id = 4 have both labels, so the conjunction count must be 2.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5084MultiLabelCountTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/testissue5084").create();
    database.command("opencypher", "CREATE "
        + "(n1:BugNode:BugA:BugB {id: 1}), "
        + "(n2:BugNode:BugA      {id: 2}), "
        + "(n3:BugNode:BugB      {id: 3}), "
        + "(n4:BugNode:BugA:BugB {id: 4})");
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void multiLabelConjunctionCount() {
    // Only id = 1 and id = 4 have both BugA and BugB.
    final ResultSet rs = database.query("opencypher",
        "MATCH (n:BugA:BugB) RETURN count(n) AS cnt");

    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Long>getProperty("cnt")).isEqualTo(2L);
    assertThat(rs.hasNext()).isFalse();
    rs.close();
  }

  @Test
  void multiLabelConjunctionRowMaterializationStillCorrect() {
    // Control: row materialization was already returning the correct set.
    final ResultSet rs = database.query("opencypher",
        "MATCH (n:BugA:BugB) RETURN n.id AS id ORDER BY id");

    assertThat(rs.next().<Number>getProperty("id").intValue()).isEqualTo(1);
    assertThat(rs.next().<Number>getProperty("id").intValue()).isEqualTo(4);
    assertThat(rs.hasNext()).isFalse();
    rs.close();
  }

  @Test
  void singleLabelPlusWherePredicateCount() {
    // Control: equivalent single-label pattern + WHERE predicate was already correct.
    final ResultSet rs = database.query("opencypher",
        "MATCH (n:BugA) WHERE n:BugB RETURN count(n) AS cnt");

    assertThat(rs.next().<Long>getProperty("cnt")).isEqualTo(2L);
    rs.close();
  }

  @Test
  void singleLabelCountStillUsesOptimization() {
    // (n:BugA) matches all nodes carrying BugA (id 1,2,4) as in Neo4j.
    final ResultSet rs = database.query("opencypher",
        "MATCH (n:BugA) RETURN count(n) AS cnt");

    assertThat(rs.next().<Long>getProperty("cnt")).isEqualTo(3L);
    rs.close();
  }

  @Test
  void singleLabelBugBCount() {
    // (n:BugB) matches all nodes carrying BugB (id 1,3,4).
    final ResultSet rs = database.query("opencypher",
        "MATCH (n:BugB) RETURN count(n) AS cnt");

    assertThat(rs.next().<Long>getProperty("cnt")).isEqualTo(3L);
    rs.close();
  }
}
