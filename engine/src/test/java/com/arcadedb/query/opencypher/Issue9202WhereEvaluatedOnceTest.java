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
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #9202: the WHERE of a MATCH planned by the cost-based optimizer was evaluated twice, once by the
 * Filter of the physical plan and once by the FilterPropertiesStep added after the optimized match. A predicate that draws
 * rand() keeps a row with probability 0.5 when it is evaluated once and 0.25 when it is evaluated twice.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9202WhereEvaluatedOnceTest {
  private static final String DB_PATH = "./target/databases/issue9202";

  private Database database;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.command("sql", "CREATE VERTEX TYPE P");
    database.command("sql", "CREATE EDGE TYPE K");
    database.begin();
    final RID[] v = new RID[1000];
    for (int i = 0; i < v.length; i++) {
      final MutableVertex mv = database.newVertex("P").set("id", (long) i);
      mv.save();
      v[i] = mv.getIdentity();
    }
    final Random r = new Random(3);
    for (int i = 0; i < 10_000; i++)
      v[r.nextInt(1000)].asVertex().newEdge("K", v[r.nextInt(1000)]);
    database.commit();
  }

  @AfterEach
  void tearDown() {
    if (database != null && database.isOpen())
      database.drop();
  }

  private long count(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      return ((Number) rs.next().getProperty("n")).longValue();
    }
  }

  private String plan(final String query) {
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query)) {
      return rs.getExecutionPlan().map(p -> p.prettyPrint(0, 2)).orElse("");
    }
  }

  @Test
  void randPredicateKeepsHalfTheRows() {
    for (final String where : new String[] { "rand() < 0.5", "b.id >= 0 AND rand() < 0.5" }) {
      final String query = "MATCH (a:P)-[:K]->(b:P) WHERE " + where + " RETURN count(*) AS n";
      double sum = 0;
      for (int i = 0; i < 5; i++)
        sum += count(query) / 10_000.0;
      assertThat(sum / 5).as(where).isBetween(0.45, 0.55);
    }
  }

  @Test
  void planCarriesTheFilterOnce() {
    final String text = plan("MATCH (a:P)-[:K]->(b:P) WHERE b.id >= 0 AND rand() < 0.5 RETURN count(*) AS n");
    assertThat(text).contains("Filter [predicate=");
    assertThat(text).doesNotContain("FILTER WHERE");
  }

  @Test
  void secondMatchWhereIsAppliedOnce() {
    final String query = "MATCH (a:P) WHERE a.id < 100 MATCH (a)-[:K]->(b:P) WHERE rand() < 0.5 RETURN count(*) AS n";
    final long all = count("MATCH (a:P) WHERE a.id < 100 MATCH (a)-[:K]->(b:P) RETURN count(*) AS n");
    double sum = 0;
    for (int i = 0; i < 10; i++)
      sum += count(query) / (double) all;
    assertThat(sum / 10).isBetween(0.4, 0.6);
  }
}
