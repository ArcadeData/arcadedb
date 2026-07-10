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
 * Regression test for issue #5190: multiple property assignments in a single SET clause must be
 * evaluated against the graph state before the SET clause runs (snapshot / simultaneous
 * assignment), matching openCypher / Neo4j semantics, rather than sequentially reading earlier
 * updates.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class CypherSetSnapshotIssue5190Test {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/testopencypher-set5190").create();
    database.getSchema().createVertexType("N");
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  private int prop(final String name) {
    final ResultSet rs = database.query("opencypher", "MATCH (a:N) RETURN a." + name + " AS v");
    return ((Number) rs.next().getProperty("v")).intValue();
  }

  @Test
  void swapValues() {
    database.transaction(() -> database.command("opencypher", "CREATE (a:N {x: 1, y: 2})"));
    database.transaction(() -> database.command("opencypher", "MATCH (a:N) SET a.x = a.y, a.y = a.x"));
    // Expected: swap -> x=2, y=1
    assertThat(prop("x")).isEqualTo(2);
    assertThat(prop("y")).isEqualTo(1);
  }

  @Test
  void crossReference() {
    database.transaction(() -> database.command("opencypher", "CREATE (a:N {x: 5, y: 0})"));
    database.transaction(() -> database.command("opencypher", "MATCH (a:N) SET a.x = a.x + 1, a.y = a.x"));
    // a.y must read the pre-SET x (5), not the just-updated 6
    assertThat(prop("x")).isEqualTo(6);
    assertThat(prop("y")).isEqualTo(5);
  }

  @Test
  void orderIndependence() {
    // Same two assignments in both orders must give the same result.
    database.transaction(() -> database.command("opencypher", "CREATE (a:N {x: 100, y: 0})"));
    database.transaction(() -> database.command("opencypher", "MATCH (a:N) SET a.x = a.x + 5, a.y = a.x + 6"));
    final int x1 = prop("x"), y1 = prop("y");

    database.transaction(() -> database.command("opencypher", "MATCH (a:N) SET a.x = 100, a.y = 0"));
    database.transaction(() -> database.command("opencypher", "MATCH (a:N) SET a.y = a.x + 6, a.x = a.x + 5"));
    final int x2 = prop("x"), y2 = prop("y");

    assertThat(x1).isEqualTo(105);
    assertThat(y1).isEqualTo(106);
    assertThat(x2).isEqualTo(x1);
    assertThat(y2).isEqualTo(y1);
  }
}
