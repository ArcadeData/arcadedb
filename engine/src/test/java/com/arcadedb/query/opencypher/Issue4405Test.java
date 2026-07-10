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
 * Regression test for <a href="https://github.com/ArcadeData/arcadedb/issues/4405">issue #4405</a>:
 * Missed optimization - redundant AND-true OR-false branch raises ArcadeDB PROFILE rows.
 *
 * The predicate {@code base AND true OR false AND expr} should be simplified to {@code base}
 * at parse time before reaching the optimizer, so both the base and transformed queries
 * produce identical execution plans.
 */
class Issue4405Test {
  private Database database;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/issue4405");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();

    database.getSchema().createVertexType("L1");

    database.transaction(() -> {
      // Create vertices with k5 values: one matching -1 (used as a proxy for -1862396709),
      // several non-matching; plus k6 and k10 properties for the UNWIND.
      database.command("opencypher", "CREATE (:L1 {k5: -1, k6: 10, k10: 20})");
      database.command("opencypher", "CREATE (:L1 {k5: -1, k6: 11, k10: 21})");
      database.command("opencypher", "CREATE (:L1 {k5: 0,  k6: 12, k10: 22})");
      database.command("opencypher", "CREATE (:L1 {k5: 1,  k6: 13, k10: 23})");
      database.command("opencypher", "CREATE (:L1 {k5: 2,  k6: 14, k10: 24})");
    });
  }

  @AfterEach
  void tearDown() {
    if (database != null && database.isOpen())
      database.drop();
  }

  /**
   * Both the base query and the semantically-equivalent transformed query
   * ({@code base AND true OR false AND expr}) must return identical result sets.
   */
  @Test
  void baseAndTransformedQueryReturnSameResults() {
    final String baseQuery = """
        MATCH (n0 :L1)
        WHERE (n0.k5) = -1
        UNWIND [(n0.k6), (n0.k10)] AS a0
        RETURN a0
        """;

    final String transformedQuery = """
        MATCH (n0 :L1)
        WHERE ((n0.k5) = -1) AND true OR false AND (n0.k5 IS NULL OR n0.k5 IS NOT NULL)
        UNWIND [(n0.k6), (n0.k10)] AS a0
        RETURN a0
        """;

    final List<Object> baseResults = collectResults(baseQuery, "a0");
    final List<Object> transformedResults = collectResults(transformedQuery, "a0");

    assertThat(transformedResults)
        .as("transformed query (base AND true OR false AND expr) should return same rows as base query")
        .containsExactlyInAnyOrderElementsOf(baseResults);
  }

  /**
   * WHERE clause with only redundant boolean constants that simplify to false
   * (false OR false) should return no results.
   */
  @Test
  void whereAlwaysFalseReturnsEmpty() {
    final String query = "MATCH (n0 :L1) WHERE false OR false RETURN n0";

    try (final ResultSet rs = database.query("opencypher", query)) {
      assertThat(rs.hasNext()).as("WHERE false OR false should return no rows").isFalse();
    }
  }

  /**
   * WHERE clause with only redundant boolean constants that simplify to true
   * (true AND true) should return all vertices.
   */
  @Test
  void whereAlwaysTrueReturnsAll() {
    final String query = "MATCH (n0 :L1) WHERE true AND true RETURN n0";
    final List<Object> results = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext()) {
        results.add(rs.next().getProperty("n0"));
      }
    }
    assertThat(results).as("WHERE true AND true should return all 5 vertices").hasSize(5);
  }

  /**
   * WHERE predicate AND true should behave identically to the predicate alone.
   */
  @Test
  void predicateAndTrueEquivalentToPredicateAlone() {
    final String baseQuery = "MATCH (n0 :L1) WHERE n0.k5 = -1 RETURN n0";
    final String withTrueQuery = "MATCH (n0 :L1) WHERE n0.k5 = -1 AND true RETURN n0";

    final List<Object> base = collectResults(baseQuery, "n0");
    final List<Object> withTrue = collectResults(withTrueQuery, "n0");

    assertThat(withTrue)
        .as("predicate AND true must return same results as predicate alone")
        .containsExactlyInAnyOrderElementsOf(base);
    assertThat(withTrue).hasSize(2);
  }

  /**
   * WHERE predicate OR false should behave identically to the predicate alone.
   */
  @Test
  void predicateOrFalseEquivalentToPredicateAlone() {
    final String baseQuery = "MATCH (n0 :L1) WHERE n0.k5 = -1 RETURN n0";
    final String withFalseQuery = "MATCH (n0 :L1) WHERE n0.k5 = -1 OR false RETURN n0";

    final List<Object> base = collectResults(baseQuery, "n0");
    final List<Object> withFalse = collectResults(withFalseQuery, "n0");

    assertThat(withFalse)
        .as("predicate OR false must return same results as predicate alone")
        .containsExactlyInAnyOrderElementsOf(base);
    assertThat(withFalse).hasSize(2);
  }

  private List<Object> collectResults(final String query, final String propertyName) {
    final List<Object> results = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        results.add(row.getProperty(propertyName));
      }
    }
    return results;
  }
}
