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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Oracle tests for the three-valued logic of AND / OR / XOR / NOT in a projection and for an aggregate nested in a boolean, CASE or
 * list-predicate expression. A WHERE turns null into false, so only a projection tells them apart. The operands are stored
 * properties (a stored true, false and an absent one) so that nothing is folded at plan time. Expected rows are what Neo4j 5.26
 * answers for the same statements.
 */
class CypherLogicAndNestedAggregationOracleTest {
  private static final String LOGIC_SETUP = "CREATE (:T {t: true, f: false})";

  /** Each case is the statement followed by the rows Neo4j 5.26 answers for it, one entry per row, columns sorted. */
  private static final String[][] LOGIC = {
        { "MATCH (t:T) RETURN t.t AND t.t AS v", "{v=true}" },
        { "MATCH (t:T) RETURN t.t AND t.f AS v", "{v=false}" },
        { "MATCH (t:T) RETURN t.t AND t.n AS v", "{v=null}" },
        { "MATCH (t:T) RETURN t.f AND t.t AS v", "{v=false}" },
        { "MATCH (t:T) RETURN t.f AND t.f AS v", "{v=false}" },
        { "MATCH (t:T) RETURN t.f AND t.n AS v", "{v=false}" },
        { "MATCH (t:T) RETURN t.n AND t.t AS v", "{v=null}" },
        { "MATCH (t:T) RETURN t.n AND t.f AS v", "{v=false}" },
        { "MATCH (t:T) RETURN t.n AND t.n AS v", "{v=null}" },
        { "MATCH (t:T) RETURN t.t OR t.t AS v", "{v=true}" },
        { "MATCH (t:T) RETURN t.t OR t.f AS v", "{v=true}" },
        { "MATCH (t:T) RETURN t.t OR t.n AS v", "{v=true}" },
        { "MATCH (t:T) RETURN t.f OR t.t AS v", "{v=true}" },
        { "MATCH (t:T) RETURN t.f OR t.f AS v", "{v=false}" },
        { "MATCH (t:T) RETURN t.f OR t.n AS v", "{v=null}" },
        { "MATCH (t:T) RETURN t.n OR t.t AS v", "{v=true}" },
        { "MATCH (t:T) RETURN t.n OR t.f AS v", "{v=null}" },
        { "MATCH (t:T) RETURN t.n OR t.n AS v", "{v=null}" },
        { "MATCH (t:T) RETURN t.t XOR t.t AS v", "{v=false}" },
        { "MATCH (t:T) RETURN t.t XOR t.f AS v", "{v=true}" },
        { "MATCH (t:T) RETURN t.t XOR t.n AS v", "{v=null}" },
        { "MATCH (t:T) RETURN t.f XOR t.t AS v", "{v=true}" },
        { "MATCH (t:T) RETURN t.f XOR t.f AS v", "{v=false}" },
        { "MATCH (t:T) RETURN t.f XOR t.n AS v", "{v=null}" },
        { "MATCH (t:T) RETURN t.n XOR t.t AS v", "{v=null}" },
        { "MATCH (t:T) RETURN t.n XOR t.f AS v", "{v=null}" },
        { "MATCH (t:T) RETURN t.n XOR t.n AS v", "{v=null}" },
        { "MATCH (t:T) RETURN NOT t.t AS v", "{v=false}" },
        { "MATCH (t:T) RETURN NOT t.f AS v", "{v=true}" },
        { "MATCH (t:T) RETURN NOT t.n AS v", "{v=null}" }
  };

  private static final String AGG_SETUP = "CREATE (:P {g: 'a', k: 1, b: true}), (:P {g: 'a', k: 2, b: false}), (:P {g: 'b', b: true}), (:P {g: 'b', k: 3}), (:P {g: 'c', k: 5, b: false}), (:P {k: 7})";

  /** Each case is the statement followed by the rows Neo4j 5.26 answers for it, one entry per row, columns sorted. */
  private static final String[][] AGG = {
        { "MATCH (p:P) RETURN p.g AS g, count(*) > 1 AS x ORDER BY g", "{g=a, x=true}", "{g=b, x=true}", "{g=c, x=false}", "{g=null, x=false}" },
        { "MATCH (p:P) RETURN p.g AS g, NOT (count(p.k) > 1) AS x ORDER BY g", "{g=a, x=false}", "{g=b, x=true}", "{g=c, x=true}", "{g=null, x=true}" },
        { "MATCH (p:P) RETURN p.g AS g, (count(*) > 1) AND (sum(p.k) > 2) AS x ORDER BY g", "{g=a, x=true}", "{g=b, x=true}", "{g=c, x=false}", "{g=null, x=false}" },
        { "MATCH (p:P) RETURN p.g AS g, (sum(p.k) > 5) OR (max(p.k) IS NULL) AS x ORDER BY g", "{g=a, x=false}", "{g=b, x=false}", "{g=c, x=false}", "{g=null, x=true}" },
        { "MATCH (p:P) RETURN p.g AS g, (count(*) > 1) XOR (sum(p.k) > 2) AS x ORDER BY g", "{g=a, x=false}", "{g=b, x=false}", "{g=c, x=true}", "{g=null, x=true}" },
        { "MATCH (p:P) RETURN p.g AS g, count(*) IN [1, 3] AS x ORDER BY g", "{g=a, x=false}", "{g=b, x=false}", "{g=c, x=true}", "{g=null, x=true}" },
        { "MATCH (p:P) RETURN p.g AS g, max(p.k) IS NULL AS x ORDER BY g", "{g=a, x=false}", "{g=b, x=false}", "{g=c, x=false}", "{g=null, x=false}" },
        { "MATCH (p:P) RETURN p.g AS g, (count(*) > 1) IS NULL AS x ORDER BY g", "{g=a, x=false}", "{g=b, x=false}", "{g=c, x=false}", "{g=null, x=false}" },
        { "MATCH (p:P) RETURN p.g AS g, CASE WHEN count(*) > 1 THEN 1 ELSE 0 END AS x ORDER BY g", "{g=a, x=1}", "{g=b, x=1}", "{g=c, x=0}", "{g=null, x=0}" },
        { "MATCH (p:P) RETURN p.g AS g, CASE count(*) WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'many' END AS x ORDER BY g", "{g=a, x=two}", "{g=b, x=two}", "{g=c, x=one}", "{g=null, x=one}" },
        { "MATCH (p:P) RETURN p.g AS g, CASE WHEN max(p.k) IS NULL THEN 'n' END AS x ORDER BY g", "{g=a, x=null}", "{g=b, x=null}", "{g=c, x=null}", "{g=null, x=null}" },
        { "MATCH (p:P) RETURN p.g AS g, CASE WHEN false THEN 0 ELSE count(*) END AS x ORDER BY g", "{g=a, x=2}", "{g=b, x=2}", "{g=c, x=1}", "{g=null, x=1}" },
        { "MATCH (p:P) RETURN p.g AS g, any(x IN collect(p.k) WHERE x > 1) AS x ORDER BY g", "{g=a, x=true}", "{g=b, x=true}", "{g=c, x=true}", "{g=null, x=true}" },
        { "MATCH (p:P) RETURN p.g AS g, all(x IN collect(p.k) WHERE x > 0) AS x ORDER BY g", "{g=a, x=true}", "{g=b, x=true}", "{g=c, x=true}", "{g=null, x=true}" },
        { "MATCH (p:P) RETURN p.g AS g, none(x IN collect(p.k) WHERE x > 5) AS x ORDER BY g", "{g=a, x=true}", "{g=b, x=true}", "{g=c, x=true}", "{g=null, x=false}" },
        { "MATCH (p:P) RETURN p.g AS g, single(x IN collect(p.k) WHERE x > 2) AS x ORDER BY g", "{g=a, x=false}", "{g=b, x=true}", "{g=c, x=true}", "{g=null, x=true}" },
        { "MATCH (p:P) RETURN p.g AS g, any(x IN collect(p.b) WHERE NOT x) AS x ORDER BY g", "{g=a, x=true}", "{g=b, x=false}", "{g=c, x=true}", "{g=null, x=false}" },
        { "MATCH (p:P) RETURN p.g AS g, NOT any(x IN collect(p.k) WHERE x > 1) AS x ORDER BY g", "{g=a, x=false}", "{g=b, x=false}", "{g=c, x=false}", "{g=null, x=false}" },
        { "MATCH (p:P) RETURN p.g AS g, any(x IN collect(p.k) WHERE x > 1) XOR count(*) > 3 AS x ORDER BY g", "{g=a, x=true}", "{g=b, x=true}", "{g=c, x=true}", "{g=null, x=true}" },
        { "MATCH (p:P) RETURN p.g AS g, CASE WHEN any(x IN collect(p.k) WHERE x > 1) THEN 'y' ELSE 'n' END AS x ORDER BY g", "{g=a, x=y}", "{g=b, x=y}", "{g=c, x=y}", "{g=null, x=y}" },
        { "MATCH (p:P) RETURN p.g AS g, count(*) > 1 AND any(x IN collect(p.k) WHERE x = 2) AS x ORDER BY g", "{g=a, x=true}", "{g=b, x=false}", "{g=c, x=false}", "{g=null, x=false}" },
        { "MATCH (p:P) RETURN count(*) > 1 AS x", "{x=true}" },
        { "MATCH (p:P) RETURN NOT (count(p.k) > 1) AS x", "{x=false}" },
        { "MATCH (p:P) RETURN (count(*) > 1) AND (sum(p.k) > 2) AS x", "{x=true}" },
        { "MATCH (p:P) RETURN CASE WHEN count(*) > 1 THEN 1 ELSE 0 END AS x", "{x=1}" },
        { "MATCH (p:P) RETURN any(x IN collect(p.k) WHERE x > 1) AS x", "{x=true}" },
        { "MATCH (p:P) RETURN any(x IN collect(p.k) WHERE x > 1) XOR count(*) > 3 AS x", "{x=false}" },
        { "MATCH (p:P) WITH p.g AS g, count(*) > 1 AS x RETURN g, x ORDER BY g", "{g=a, x=true}", "{g=b, x=true}", "{g=c, x=false}", "{g=null, x=false}" },
        { "MATCH (p:P) WITH p.g AS g, NOT (count(p.k) > 1) AS x RETURN g, x ORDER BY g", "{g=a, x=false}", "{g=b, x=true}", "{g=c, x=true}", "{g=null, x=true}" },
        { "MATCH (p:P) WITH p.g AS g, any(x IN collect(p.k) WHERE x > 1) AS x RETURN g, x ORDER BY g", "{g=a, x=true}", "{g=b, x=true}", "{g=c, x=true}", "{g=null, x=true}" }
  };

  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/cypherLogicAggOracle").create();
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  private void check(final String setup, final String[][] cases) {
    database.transaction(() -> database.command("opencypher", setup));
    for (final String[] testCase : cases) {
      final List<String> rows = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher", testCase[0])) {
        while (rs.hasNext())
          rows.add(render(rs.next()));
      }
      assertThat(rows).as(testCase[0]).containsExactly(Arrays.copyOfRange(testCase, 1, testCase.length));
    }
  }

  private static String render(final Result row) {
    final Map<String, Object> sorted = new TreeMap<>();
    for (final String name : row.getPropertyNames())
      sorted.put(name, row.getProperty(name));
    return sorted.toString();
  }

  @Test
  void threeValuedLogicOfAndOrXorNotInAProjection() {
    check(LOGIC_SETUP, LOGIC);
  }

  @Test
  void aggregateNestedInBooleanCaseAndListPredicateExpressions() {
    check(AGG_SETUP, AGG);
  }
}
