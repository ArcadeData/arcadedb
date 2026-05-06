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

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for GitHub issue #4106.
 * <p>
 * A fixed-length pattern comprehension whose start variable is not bound by an
 * outer clause must iterate over candidate vertices in the graph, matching
 * Neo4j's behavior.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue4106PatternComprehensionTest {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/issue-4106-pattern-comp").create();
    database.transaction(() -> database.command("opencypher",
        "CREATE (a:Person {name:'Alice', age:30}), "
            + "(b:Person {name:'Bob', age:25}), "
            + "(c:Person {name:'Charlie', age:35}), "
            + "(a)-[:KNOWS {since:2020}]->(b), "
            + "(b)-[:KNOWS {since:2019}]->(c), "
            + "(a)-[:KNOWS {since:2021}]->(c)"));
  }

  @AfterEach
  void cleanup() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  void patternComprehensionUnboundStartReturnsAllMatches() {
    final ResultSet rs = database.query("opencypher",
        "RETURN [(p:Person)-[r:KNOWS]->(q:Person) | q.name] AS xs");
    assertThat(rs.hasNext()).isTrue();
    final List<Object> list = (List<Object>) rs.next().getProperty("xs");
    assertThat(list).containsExactlyInAnyOrder("Bob", "Charlie", "Charlie");
  }

  @SuppressWarnings("unchecked")
  @Test
  void patternComprehensionUnboundStartWithFilterReturnsFiltered() {
    final ResultSet rs = database.query("opencypher",
        "RETURN [(p:Person)-[r:KNOWS]->(q:Person) WHERE r.since > 2019 | q.name] AS xs");
    assertThat(rs.hasNext()).isTrue();
    final List<Object> list = (List<Object>) rs.next().getProperty("xs");
    assertThat(list).containsExactlyInAnyOrder("Bob", "Charlie");
  }

  @Test
  void patternComprehensionSizeReturnsCount() {
    final ResultSet rs = database.query("opencypher",
        "RETURN size([(p:Person)-[r:KNOWS]->(q:Person) | q.name]) AS s");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Number>getProperty("s").longValue()).isEqualTo(3L);
  }
}
