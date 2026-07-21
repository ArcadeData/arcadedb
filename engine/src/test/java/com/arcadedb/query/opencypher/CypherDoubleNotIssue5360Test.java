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
 * Regression test for issue #5360: a bare chain of NOT operators in a WHERE clause was collapsed to a
 * single NOT, so {@code WHERE NOT NOT n.flag} behaved like {@code WHERE NOT n.flag}. The grammar rule is
 * {@code expression9: NOT* expression8}, and the boolean branch of the AST builder applied the operator
 * once regardless of how many NOT tokens were matched. The projection branch already looped over the
 * tokens, which is why {@code RETURN NOT NOT n.flag} returned the right value while the filter did not.
 * Neo4j evaluates each NOT, so an even number of them is the identity on true/false and keeps null null.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class CypherDoubleNotIssue5360Test {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/testopencypher-doublenot5360").create();
    database.command("sql", "CREATE VERTEX TYPE BoolCase");
    database.command("sql", "CREATE PROPERTY BoolCase.id LONG");
    database.command("sql", "CREATE PROPERTY BoolCase.flag BOOLEAN");
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:BoolCase {id: 1, flag: true})");
      database.command("opencypher", "CREATE (:BoolCase {id: 2, flag: false})");
      database.command("opencypher", "CREATE (:BoolCase {id: 3})");
    });
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  private List<Long> ids(final String cypher) {
    final List<Long> result = new ArrayList<>();
    final ResultSet rs = database.query("opencypher", cypher);
    while (rs.hasNext())
      result.add(((Number) rs.next().getProperty("id")).longValue());
    return result;
  }

  @Test
  void bareDoubleNotInWhereMustNotCollapseToSingleNot() {
    // Query A: the baseline, only the true flag passes.
    assertThat(ids("MATCH (n:BoolCase) WHERE n.flag RETURN n.id AS id ORDER BY id")).containsExactly(1L);

    // Query B: double negation is the identity, so the same row must come back. It used to return id=2.
    assertThat(ids("MATCH (n:BoolCase) WHERE NOT NOT n.flag RETURN n.id AS id ORDER BY id")).containsExactly(1L);

    // Control: the parenthesized form always worked.
    assertThat(ids("MATCH (n:BoolCase) WHERE NOT (NOT n.flag) RETURN n.id AS id ORDER BY id")).containsExactly(1L);
  }

  @Test
  void oddAndEvenNotChainsInWhere() {
    assertThat(ids("MATCH (n:BoolCase) WHERE NOT n.flag RETURN n.id AS id ORDER BY id")).containsExactly(2L);
    assertThat(ids("MATCH (n:BoolCase) WHERE NOT NOT NOT n.flag RETURN n.id AS id ORDER BY id")).containsExactly(2L);
    assertThat(ids("MATCH (n:BoolCase) WHERE NOT NOT NOT NOT n.flag RETURN n.id AS id ORDER BY id")).containsExactly(1L);
  }

  @Test
  void doubleNotOnComparisonInWhere() {
    assertThat(ids("MATCH (n:BoolCase) WHERE NOT NOT n.id = 2 RETURN n.id AS id ORDER BY id")).containsExactly(2L);
    assertThat(ids("MATCH (n:BoolCase) WHERE NOT NOT n.id > 1 RETURN n.id AS id ORDER BY id")).containsExactly(2L, 3L);
  }

  @Test
  void doubleNotCombinedWithAndOr() {
    assertThat(ids("MATCH (n:BoolCase) WHERE NOT NOT n.flag AND n.id = 1 RETURN n.id AS id ORDER BY id")).containsExactly(1L);
    assertThat(ids("MATCH (n:BoolCase) WHERE n.id = 3 OR NOT NOT n.flag RETURN n.id AS id ORDER BY id")).containsExactly(1L, 3L);
  }

  @Test
  void doubleNotOnConstantWithoutGraphData() {
    // The reproducer without graph data: NOT NOT false is false, so no row must be returned.
    final ResultSet rs = database.query("opencypher", "WITH false AS p WHERE NOT NOT p RETURN p");
    assertThat(rs.hasNext()).isFalse();

    final ResultSet rs2 = database.query("opencypher", "WITH true AS p WHERE NOT NOT p RETURN p");
    assertThat(rs2.hasNext()).isTrue();
    assertThat((Boolean) rs2.next().getProperty("p")).isTrue();
  }

  @Test
  void doubleNotOnNullStaysNullAndFiltersOut() {
    // n.flag is null on id=3: NOT NOT null is null, which is not true, so the row is filtered out.
    assertThat(ids("MATCH (n:BoolCase) WHERE NOT NOT n.flag RETURN n.id AS id ORDER BY id")).doesNotContain(3L);

    final ResultSet rs = database.query("opencypher",
        "MATCH (n:BoolCase) WHERE n.id = 3 RETURN NOT NOT n.flag AS doubleNot");
    assertThat(rs.hasNext()).isTrue();
    final Result row = rs.next();
    assertThat((Object) row.getProperty("doubleNot")).isNull();
  }

  @Test
  void doubleNotOnIndexedPropertyGoesThroughThePlanner() {
    database.command("sql", "CREATE INDEX ON BoolCase (id) UNIQUE");
    assertThat(ids("MATCH (n:BoolCase) WHERE NOT NOT n.id = 2 RETURN n.id AS id ORDER BY id")).containsExactly(2L);
    assertThat(ids("MATCH (n:BoolCase) WHERE NOT n.id = 2 RETURN n.id AS id ORDER BY id")).containsExactly(1L, 3L);
  }

  @Test
  void doubleNotOnPatternPredicate() {
    database.command("sql", "CREATE EDGE TYPE Knows");
    database.transaction(() -> database.command("opencypher",
        "MATCH (a:BoolCase {id: 1}), (b:BoolCase {id: 2}) CREATE (a)-[:Knows]->(b)"));

    assertThat(ids("MATCH (n:BoolCase) WHERE (n)-[:Knows]->() RETURN n.id AS id ORDER BY id")).containsExactly(1L);
    assertThat(ids("MATCH (n:BoolCase) WHERE NOT (n)-[:Knows]->() RETURN n.id AS id ORDER BY id")).containsExactly(2L, 3L);
    assertThat(ids("MATCH (n:BoolCase) WHERE NOT NOT (n)-[:Knows]->() RETURN n.id AS id ORDER BY id")).containsExactly(1L);
  }

  @Test
  void doubleNotInProjectionKeepsWorking() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (n:BoolCase) RETURN n.id AS id, n.flag AS flag, NOT NOT n.flag AS doubleNot ORDER BY id");
    final List<Object> doubleNots = new ArrayList<>();
    while (rs.hasNext())
      doubleNots.add(rs.next().getProperty("doubleNot"));
    assertThat(doubleNots).containsExactly(true, false, null);
  }
}
