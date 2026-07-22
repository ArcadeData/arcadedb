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
 * Regression for issue #5384: a parenthesized function comparison in a projection, e.g.
 * {@code (abs(n.val) = 10)}, returned the raw function value instead of the BOOLEAN
 * result of the comparison.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5384ParenthesizedFunctionComparisonTest {
  private Database database;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/testIssue5384");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();

    database.command("opencypher", "CREATE (:Node {id:1, val:10})");
    database.command("opencypher", "CREATE (:Node {id:2, val:-10})");
    database.command("opencypher", "CREATE (:Node {id:3, val:5})");
    database.command("opencypher", "CREATE (:Node {id:4})");
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void parenthesizedAbsComparison() {
    assertThat(project("MATCH (n:Node) RETURN n.id AS id, (abs(n.val) = 10) AS r ORDER BY id"))
        .containsExactly(true, true, false, null);
  }

  @Test
  void unparenthesizedAbsComparisonControl() {
    assertThat(project("MATCH (n:Node) RETURN n.id AS id, abs(n.val) = 10 AS r ORDER BY id"))
        .containsExactly(true, true, false, null);
  }

  @Test
  void parenthesizedCoalesceComparison() {
    assertThat(project("MATCH (n:Node) RETURN n.id AS id, (coalesce(n.val, 0) = 10) AS r ORDER BY id"))
        .containsExactly(true, false, false, false);
  }

  @Test
  void parenthesizedToStringComparison() {
    assertThat(project("MATCH (n:Node) RETURN n.id AS id, (toString(n.val) = '10') AS r ORDER BY id"))
        .containsExactly(true, false, false, null);
  }

  @Test
  void parenthesizedToIntegerComparison() {
    assertThat(project("MATCH (n:Node) RETURN n.id AS id, (toInteger(n.val) = 10) AS r ORDER BY id"))
        .containsExactly(true, false, false, null);
  }

  @Test
  void parenthesizedToFloatGreaterThan() {
    assertThat(project("MATCH (n:Node) RETURN n.id AS id, (toFloat(n.val) > 0.0) AS r ORDER BY id"))
        .containsExactly(true, false, true, null);
  }

  @Test
  void parenthesizedCoalesceGreaterThan() {
    assertThat(project("MATCH (n:Node) RETURN n.id AS id, (coalesce(n.val, 0) > 0) AS r ORDER BY id"))
        .containsExactly(true, false, true, false);
  }

  @Test
  void parenthesizedComparisonInWithProjection() {
    assertThat(project("MATCH (n:Node) WITH n.id AS id, (abs(n.val) = 10) AS r RETURN id, r ORDER BY id"))
        .containsExactly(true, true, false, null);
  }

  private List<Object> project(final String cypher) {
    final List<Object> values = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", cypher)) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        values.add(r.getProperty("r"));
      }
    }
    return values;
  }
}
