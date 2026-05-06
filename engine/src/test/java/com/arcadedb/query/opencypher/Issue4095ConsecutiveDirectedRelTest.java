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
 * Regression test for GitHub issue #4095.
 * <p>
 * Two directed relationship slots in the same MATCH pattern must bind to
 * distinct edges (Cypher relationship-uniqueness rule), so the pattern
 * {@code (s)<-[:KNOWS]-(f)-[:KNOWS]->(d)} cannot reuse the same edge for
 * both slots.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue4095ConsecutiveDirectedRelTest {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/issue-4095-consecutive-directed-rel").create();
    database.transaction(() -> database.command("opencypher",
        "CREATE (:Person {name:'Alice'})<-[:KNOWS]-(:Person {name:'Bob'})-[:KNOWS]->(:Person {name:'Charlie'})"));
  }

  @AfterEach
  void cleanup() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void consecutiveDirectedRelDoesNotReuseSameEdge() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (s:Person)<-[:KNOWS]-(f:Person)-[:KNOWS]->(d:Person) "
            + "RETURN s.name AS s, f.name AS f, d.name AS d "
            + "ORDER BY s, f, d");
    final List<String> rows = new ArrayList<>();
    while (rs.hasNext()) {
      final Result r = rs.next();
      rows.add(r.<String>getProperty("s") + "," + r.<String>getProperty("f") + "," + r.<String>getProperty("d"));
    }
    assertThat(rows).containsExactly("Alice,Bob,Charlie", "Charlie,Bob,Alice");
  }
}
