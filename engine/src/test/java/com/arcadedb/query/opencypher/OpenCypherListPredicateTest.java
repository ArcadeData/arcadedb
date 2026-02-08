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
 * Tests for Cypher list predicate functions: all(), any(), none(), single().
 * Regression test for https://github.com/ArcadeData/arcadedb/issues/3334
 */
class OpenCypherListPredicateTest {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./databases/test-list-predicates").create();
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void testAllPredicates() {
    // Exact query from the issue
    final ResultSet rs = database.query("opencypher",
        "WITH [1, 2, 3, 4] AS list " +
            "RETURN " +
            "  all(x IN list WHERE x > 0) AS is_all_pos, " +
            "  any(x IN list WHERE x = 4) AS has_four, " +
            "  none(x IN list WHERE x < 0) AS no_neg, " +
            "  single(x IN list WHERE x = 2) AS just_one_two");

    assertThat(rs.hasNext()).isTrue();
    final Result row = rs.next();
    assertThat((Boolean) row.getProperty("is_all_pos")).isTrue();
    assertThat((Boolean) row.getProperty("has_four")).isTrue();
    assertThat((Boolean) row.getProperty("no_neg")).isTrue();
    assertThat((Boolean) row.getProperty("just_one_two")).isTrue();
    assertThat(rs.hasNext()).isFalse();
  }

  @Test
  void testAllPredicateTrue() {
    final ResultSet rs = database.query("opencypher",
        "RETURN all(x IN [2, 4, 6] WHERE x > 0) AS result");
    assertThat(rs.hasNext()).isTrue();
    assertThat((Boolean) rs.next().getProperty("result")).isTrue();
  }

  @Test
  void testAllPredicateFalse() {
    final ResultSet rs = database.query("opencypher",
        "RETURN all(x IN [1, 2, 3] WHERE x > 2) AS result");
    assertThat(rs.hasNext()).isTrue();
    assertThat((Boolean) rs.next().getProperty("result")).isFalse();
  }

  @Test
  void testAnyPredicateTrue() {
    final ResultSet rs = database.query("opencypher",
        "RETURN any(x IN [1, 2, 3] WHERE x = 2) AS result");
    assertThat(rs.hasNext()).isTrue();
    assertThat((Boolean) rs.next().getProperty("result")).isTrue();
  }

  @Test
  void testAnyPredicateFalse() {
    final ResultSet rs = database.query("opencypher",
        "RETURN any(x IN [1, 2, 3] WHERE x > 10) AS result");
    assertThat(rs.hasNext()).isTrue();
    assertThat((Boolean) rs.next().getProperty("result")).isFalse();
  }

  @Test
  void testNonePredicateTrue() {
    final ResultSet rs = database.query("opencypher",
        "RETURN none(x IN [1, 2, 3] WHERE x > 5) AS result");
    assertThat(rs.hasNext()).isTrue();
    assertThat((Boolean) rs.next().getProperty("result")).isTrue();
  }

  @Test
  void testNonePredicateFalse() {
    final ResultSet rs = database.query("opencypher",
        "RETURN none(x IN [1, 2, 3] WHERE x = 2) AS result");
    assertThat(rs.hasNext()).isTrue();
    assertThat((Boolean) rs.next().getProperty("result")).isFalse();
  }

  @Test
  void testSinglePredicateTrue() {
    final ResultSet rs = database.query("opencypher",
        "RETURN single(x IN [1, 2, 3] WHERE x = 2) AS result");
    assertThat(rs.hasNext()).isTrue();
    assertThat((Boolean) rs.next().getProperty("result")).isTrue();
  }

  @Test
  void testSinglePredicateFalse() {
    final ResultSet rs = database.query("opencypher",
        "RETURN single(x IN [1, 2, 3] WHERE x > 1) AS result");
    assertThat(rs.hasNext()).isTrue();
    assertThat((Boolean) rs.next().getProperty("result")).isFalse();
  }

  @Test
  void testEmptyList() {
    final ResultSet rs = database.query("opencypher",
        "RETURN all(x IN [] WHERE x > 0) AS a, " +
            "any(x IN [] WHERE x > 0) AS b, " +
            "none(x IN [] WHERE x > 0) AS c, " +
            "single(x IN [] WHERE x > 0) AS d");
    assertThat(rs.hasNext()).isTrue();
    final Result row = rs.next();
    // all() on empty list is true (vacuous truth)
    assertThat((Boolean) row.getProperty("a")).isTrue();
    // any() on empty list is false
    assertThat((Boolean) row.getProperty("b")).isFalse();
    // none() on empty list is true
    assertThat((Boolean) row.getProperty("c")).isTrue();
    // single() on empty list is false
    assertThat((Boolean) row.getProperty("d")).isFalse();
  }

  @Test
  void testNestedQuantifiers() {
    // TCK Quantifier5 [1]: none(x IN list WHERE none(y IN x WHERE y = 'abc'))
    final ResultSet rs = database.query("opencypher",
        "RETURN none(x IN [['abc'], ['abc', 'def']] WHERE none(y IN x WHERE y = 'abc')) AS result");
    assertThat(rs.hasNext()).isTrue();
    assertThat((Boolean) rs.next().getProperty("result")).isTrue();
  }

  @Test
  void testQuantifierWithSharedList() {
    // TCK Quantifier5 [2]: none(x IN list WHERE none(y IN list WHERE x <= y))
    final ResultSet rs = database.query("opencypher",
        "WITH [1, 2, 3, 4, 5, 6, 7, 8, 9] AS list " +
            "RETURN none(x IN list WHERE none(y IN list WHERE x <= y)) AS result");
    assertThat(rs.hasNext()).isTrue();
    assertThat((Boolean) rs.next().getProperty("result")).isTrue();
  }

  @Test
  void testNoneEqualsNotAny() {
    // TCK Quantifier5 [3]: none(...) = (NOT any(...))
    final ResultSet rs = database.query("opencypher",
        "RETURN none(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x = 2) = " +
            "(NOT any(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x = 2)) AS result");
    assertThat(rs.hasNext()).isTrue();
    assertThat((Boolean) rs.next().getProperty("result")).isTrue();
  }

  @Test
  void testAllWithNot() {
    // Test all() with NOT inside WHERE
    ResultSet rs = database.query("opencypher",
        "RETURN all(x IN [1, 2, 3] WHERE NOT (x = 2)) AS result");
    assertThat(rs.hasNext()).isTrue();
    assertThat((Boolean) rs.next().getProperty("result")).isFalse();
  }

  @Test
  void testQuantifierEqualsQuantifier() {
    // Compare two quantifier results
    final ResultSet rs = database.query("opencypher",
        "RETURN none(x IN [1, 2, 3] WHERE x = 2) = all(x IN [1, 2, 3] WHERE NOT (x = 2)) AS result");
    assertThat(rs.hasNext()).isTrue();
    assertThat((Boolean) rs.next().getProperty("result")).isTrue();
  }

  @Test
  void testSizeOfListComprehension() {
    // Test size() on list comprehension
    ResultSet rs1 = database.query("opencypher",
        "RETURN size([x IN [1, 2, 3, 4, 5] WHERE x > 3 | x]) AS result");
    assertThat(rs1.hasNext()).isTrue();
    Object r1 = rs1.next().getProperty("result");
    assertThat(r1).isEqualTo(2L);
  }

  @Test
  void testSizeComprehensionInComparison() {
    // Test size() on list comprehension compared to number without parens
    final ResultSet rs = database.query("opencypher",
        "RETURN size([x IN [1, 2, 3, 4, 5] WHERE x > 3 | x]) = 2 AS result");
    assertThat(rs.hasNext()).isTrue();
    assertThat((Boolean) rs.next().getProperty("result")).isTrue();
  }

  @Test
  void testNoneEqualsSizeFilter() {
    // TCK Quantifier5 [5]
    final ResultSet rs = database.query("opencypher",
        "RETURN none(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x = 2) = " +
            "(size([x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x = 2 | x]) = 0) AS result");
    assertThat(rs.hasNext()).isTrue();
    assertThat((Boolean) rs.next().getProperty("result")).isTrue();
  }

  @Test
  void testWithMatchedNodes() {
    database.getSchema().createVertexType("Item");
    database.transaction(() -> {
      database.newVertex("Item").set("val", 10).save();
      database.newVertex("Item").set("val", 20).save();
      database.newVertex("Item").set("val", 30).save();
    });

    final ResultSet rs = database.query("opencypher",
        "MATCH (n:Item) WITH collect(n.val) AS vals " +
            "RETURN all(v IN vals WHERE v > 0) AS result");
    assertThat(rs.hasNext()).isTrue();
    assertThat((Boolean) rs.next().getProperty("result")).isTrue();
  }
}
