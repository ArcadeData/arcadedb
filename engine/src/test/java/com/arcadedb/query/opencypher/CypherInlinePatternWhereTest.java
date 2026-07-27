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
 * Covers the openCypher inline {@code WHERE} predicate inside node and relationship patterns,
 * e.g. {@code MATCH (n:Person WHERE n.age > 18)-[r:KNOWS WHERE r.since < 2000]->(m)}.
 * <p>
 * Two defects were found while fixing issue #5464:
 * <ul>
 *   <li>the node inline predicate was parsed but never reached any execution step, so it matched
 *       everything;</li>
 *   <li>the relationship inline predicate was dropped together with its edge binding whenever
 *       nothing else in the query referenced the relationship variable, so it matched nothing
 *       ({@code count(*)} returned 0 while {@code count(r)} returned 1 - issue #5466).</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherInlinePatternWhereTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/cypher-inline-pattern-where").create();
    database.transaction(() -> {
      database.command("opencypher", "CREATE (a:P {name: 'Alice', age: 30}), (b:P {name: 'Bob', age: 17}), "
          + "(c:P {name: 'Carol', age: 45})");
      database.command("opencypher", "MATCH (a:P {name: 'Alice'}), (b:P {name: 'Bob'}), (c:P {name: 'Carol'}) "
          + "CREATE (a)-[:KNOWS {since: 1995}]->(b), (a)-[:KNOWS {since: 2010}]->(c)");
    });
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  private List<String> names(final String query) {
    final List<String> names = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        names.add(r.getProperty("name"));
      }
    }
    return names;
  }

  private long count(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      assertThat(rs.hasNext()).isTrue();
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  @Test
  void nodeInlineWhereFiltersLikeClauseWhere() {
    assertThat(names("MATCH (n:P WHERE n.age > 18) RETURN n.name AS name ORDER BY name"))
        .containsExactly("Alice", "Carol");
    assertThat(names("MATCH (n:P) WHERE n.age > 18 RETURN n.name AS name ORDER BY name"))
        .containsExactly("Alice", "Carol");
    assertThat(names("MATCH (n:P WHERE n.age > 200) RETURN n.name AS name")).isEmpty();
  }

  @Test
  void nodeInlineWhereCombinesWithClauseWhere() {
    assertThat(names("MATCH (n:P WHERE n.age > 18) WHERE n.name STARTS WITH 'A' RETURN n.name AS name"))
        .containsExactly("Alice");
    assertThat(names("MATCH (n:P WHERE n.age > 18) WHERE n.name STARTS WITH 'B' RETURN n.name AS name")).isEmpty();
  }

  @Test
  void nodeInlineWhereOnTraversalTarget() {
    assertThat(names("MATCH (:P {name: 'Alice'})-[:KNOWS]->(m:P WHERE m.age > 18) RETURN m.name AS name"))
        .containsExactly("Carol");
    assertThat(count("MATCH (:P {name: 'Alice'})-[:KNOWS]->(m:P WHERE m.age > 200) RETURN count(*) AS c")).isZero();
  }

  @Test
  void nodeInlineWhereInOptionalMatchKeepsTheRow() {
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (n:P {name: 'Alice'}) OPTIONAL MATCH (m:P WHERE m.age > 200) RETURN n.name AS name, m AS other")) {
      assertThat(rs.hasNext()).isTrue();
      final Result r = rs.next();
      assertThat(r.<String>getProperty("name")).isEqualTo("Alice");
      assertThat(r.<Object>getProperty("other")).isNull();
      assertThat(rs.hasNext()).isFalse();
    }
  }

  @Test
  void relationshipInlineWhereFiltersWhenTheVariableIsNotProjected() {
    // count(*) does not mention r: the edge binding must survive anyway, otherwise the inline
    // predicate evaluates against an unbound variable and drops every row (issue #5466)
    assertThat(count("MATCH (:P {name: 'Alice'})-[r:KNOWS WHERE r.since < 2000]->(m) RETURN count(*) AS c"))
        .isEqualTo(1);
    assertThat(count("MATCH (:P {name: 'Alice'})-[r:KNOWS WHERE r.since < 2000]->(m) RETURN count(r) AS c"))
        .isEqualTo(1);
    assertThat(names("MATCH (:P {name: 'Alice'})-[r:KNOWS WHERE r.since < 2000]->(m) RETURN m.name AS name"))
        .containsExactly("Bob");
    assertThat(names("MATCH (:P {name: 'Alice'})-[r:KNOWS WHERE r.since > 3000]->(m) RETURN m.name AS name")).isEmpty();
  }

  @Test
  void relationshipInlineWhereCanReferenceOuterVariables() {
    // Alice is 30, so the bound is 2000: only the 1995 edge qualifies
    assertThat(names("MATCH (n:P {name: 'Alice'})-[r:KNOWS WHERE r.since < n.age + 1970]->(m) RETURN m.name AS name"))
        .containsExactly("Bob");
  }

  @Test
  void inlineWhereOnBothEndsOfTheSamePattern() {
    assertThat(names("MATCH (n:P WHERE n.age > 18)-[r:KNOWS WHERE r.since > 2000]->(m:P WHERE m.age > 18) "
        + "RETURN m.name AS name")).containsExactly("Carol");
  }
}
