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

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #5140: {@code COUNT { ... }} block subqueries inside a
 * pattern-comprehension {@code WHERE} clause must filter the comprehension results.
 * ArcadeDB previously ignored the predicate, returning all rows.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5140CountInPatternComprehensionWhereTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./databases/test-5140-count-pc-where").create();
    database.getSchema().createVertexType("CB");
    database.getSchema().createEdgeType("E");

    // a -> b (weight 1), a -> c (weight 2), b -> d (weight 3)
    // b has 1 outgoing edge; c has 0 outgoing edges.
    database.command("opencypher",
        """
        CREATE (a:CB {name: 'a'}), \
        (b:CB {name: 'b'}), \
        (c:CB {name: 'c'}), \
        (d:CB {name: 'd'}), \
        (a)-[:E {weight: 1}]->(b), \
        (a)-[:E {weight: 2}]->(c), \
        (b)-[:E {weight: 3}]->(d)""");
  }

  @AfterEach
  void tearDown() {
    if (database != null)
      database.drop();
  }

  @Test
  void countZeroInPatternComprehensionWhere() {
    // Only c has zero outgoing edges.
    final ResultSet rs = database.query("opencypher",
        """
        MATCH (a:CB {name: 'a'})
        RETURN [
          (a)-[:E]->(b:CB)
          WHERE COUNT { MATCH (b)-[:E]->(:CB) } = 0
          | b.name
        ] AS names""");

    assertThat(rs.hasNext()).isTrue();
    final Result result = rs.next();
    final List<Object> names = result.getProperty("names");
    assertThat(names).containsExactly("c");
  }

  @Test
  void countGreaterThanZeroInPatternComprehensionWhere() {
    // Only b has outgoing edges.
    final ResultSet rs = database.query("opencypher",
        """
        MATCH (a:CB {name: 'a'})
        RETURN [
          (a)-[:E]->(b:CB)
          WHERE COUNT { MATCH (b)-[:E]->(:CB) } > 0
          | b.name
        ] AS names""");

    assertThat(rs.hasNext()).isTrue();
    final Result result = rs.next();
    final List<Object> names = result.getProperty("names");
    assertThat(names).containsExactly("b");
  }

  @Test
  void ordinaryPredicateInPatternComprehensionWhereStillWorks() {
    // Control: an ordinary predicate must keep filtering correctly.
    final ResultSet rs = database.query("opencypher",
        """
        MATCH (a:CB {name: 'a'})
        RETURN [
          (a)-[:E]->(b:CB)
          WHERE b.name = 'c'
          | b.name
        ] AS names""");

    assertThat(rs.hasNext()).isTrue();
    final Result result = rs.next();
    final List<Object> names = result.getProperty("names");
    assertThat(names).containsExactly("c");
  }
}
