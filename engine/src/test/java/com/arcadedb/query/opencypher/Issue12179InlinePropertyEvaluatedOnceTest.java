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

import com.arcadedb.TestHelper;
import com.arcadedb.database.Identifiable;
import com.arcadedb.function.sql.SQLFunctionAbstract;
import com.arcadedb.query.sql.SQLQueryEngine;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #12179: the inline property map of {@code MATCH (a:N {k: expr})} was evaluated once to look the
 * vertex up (index seek or scan hash) and again to check every candidate, so a non-deterministic expression drew two values
 * and the vertex found with the first was rejected by the second: {@code UNWIND range(1, 2000) AS j MATCH (a:N {k:
 * toInteger(rand() * 10)})} over keys 0..9 counted about 200 instead of 2000.
 */
class Issue12179InlinePropertyEvaluatedOnceTest extends TestHelper {
  private static final String FUNCTION = "issue12179Counted";
  // an undotted name must be a Cypher function: a SQL function is called through the sql. prefix
  private static final String CALL     = "sql." + FUNCTION;

  private final AtomicInteger calls = new AtomicInteger();

  @BeforeEach
  void registerCountingFunction() {
    database.command("sql", "CREATE VERTEX TYPE Indexed");
    database.command("sql", "CREATE PROPERTY Indexed.k INTEGER");
    database.command("sql", "CREATE INDEX ON Indexed (k) UNIQUE");
    database.command("sql", "CREATE VERTEX TYPE Plain");
    database.transaction(() -> {
      database.command("opencypher", "UNWIND range(0, 9) AS i CREATE (:Indexed {k: i})");
      database.command("opencypher", "UNWIND range(0, 9) AS i CREATE (:Plain {k: i})");
    });

    // returns its argument and counts the calls: the number of times the inline expression is evaluated
    ((SQLQueryEngine) database.getQueryEngine("sql")).getFunctionFactory().register(new SQLFunctionAbstract(FUNCTION) {
      @Override
      public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
          final CommandContext context) {
        calls.incrementAndGet();
        return params[0];
      }

      @Override
      public String getSyntax() {
        return FUNCTION + "(<value>)";
      }
    });
  }

  @AfterEach
  void unregisterCountingFunction() {
    ((SQLQueryEngine) database.getQueryEngine("sql")).getFunctionFactory().unregister(FUNCTION);
  }

  private long count(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      return ((Number) rs.next().getProperty("n")).longValue();
    }
  }

  @Test
  void randomKeyFindsAVertexOnEveryRow() {
    for (final String type : new String[] { "Indexed", "Plain" })
      assertThat(count("UNWIND range(1, 2000) AS j MATCH (a:" + type + " {k: toInteger(rand() * 10)}) RETURN count(a) AS n"))
          .as(type).isEqualTo(2000L);
  }

  @Test
  void inlineExpressionIsEvaluatedOncePerRow() {
    for (final String type : new String[] { "Indexed", "Plain" }) {
      calls.set(0);
      assertThat(count("UNWIND range(1, 100) AS j MATCH (a:" + type + " {k: " + CALL + "(j % 10)}) RETURN count(a) AS n"))
          .as(type).isEqualTo(100L);
      assertThat(calls.get()).as(type).isEqualTo(100);
    }
  }

  /**
   * The WHERE form evaluates the value once per candidate, as written (Neo4j filters the same way): ten candidates per row, each
   * kept with probability 1/10, so about one match per row. Seeking the index with one draw and filtering with another kept
   * about one in ten (198 of 2000). The bounds are seven standard deviations of the binomial count either side of 2000.
   */
  @Test
  void randomKeyInWhereKeepsOneMatchPerRow() {
    for (final String type : new String[] { "Indexed", "Plain" })
      assertThat(count("UNWIND range(1, 2000) AS j MATCH (a:" + type + ") WHERE a.k = toInteger(rand() * 10) RETURN count(a) AS n"))
          .as(type).isBetween(1700L, 2300L);
  }
}
