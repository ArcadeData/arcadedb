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
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #13267: what the planner reads of the index of a pinned target to decide where a hop starts. A key only a row knows (a
 * {@code WITH} variable) is estimated from the ends of the index, and planning never walks the index: 800 live entries and 200
 * tombstones in the middle of its key order are the witness, since a walk of every entry skips the tombstones and counts them in the
 * {@code deadEntriesSkipped} statistic of the index, and a sample of its two ends and a seek of one key do not reach them.
 */
class CypherLabelledSourceReversePlanningIssue13267Test {
  private static final String PATH = "./target/databases/cypher-labelled-source-reverse-planning-13267";

  private static final String ROW_ONLY_KEY = "WITH 'k1010' AS s MATCH (a:Src)-[r:E]->(n:Key {k: s}) RETURN count(*) AS c";

  private Database database;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory(PATH);
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.command("sql", "CREATE VERTEX TYPE Key BUCKETS 1");
    database.command("sql", "CREATE PROPERTY Key.k STRING");
    database.command("sql", "CREATE INDEX ON Key (k) NOTUNIQUE");
    database.command("sql", "CREATE VERTEX TYPE Src");
    database.command("sql", "CREATE EDGE TYPE E");
    database.transaction(() -> {
      database.command("opencypher", "UNWIND range(0, 999) AS i CREATE (:Key {k: 'k' + toString(1000 + i)})");
      database.command("opencypher", "UNWIND range(0, 4) AS i CREATE (:Src {i: i})");
      database.command("opencypher", "MATCH (a:Src), (n:Key {k: 'k1010'}) CREATE (a)-[:E]->(n)");
    });
    // the middle of the key order, left as tombstones in the index pages
    database.transaction(() -> database.command("opencypher", "MATCH (n:Key) WHERE n.k >= 'k1400' AND n.k < 'k1600' DELETE n"));
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void aKeyOnlyARowKnowsIsEstimatedFromTheIndexAndStartsFromTheTarget() {
    // 5 Src vertices against one entry per key and next to no incoming relationships per Key vertex
    final String plan = profile(ROW_ONLY_KEY);
    assertThat(plan).as("the test is about the step-based plan").doesNotContain("Cost-Based");
    assertThat(lineOf(plan, "MATCH NODE")).as(plan).contains("(n:Key");
    assertThat(count(ROW_ONLY_KEY)).isEqualTo(5L);
  }

  @Test
  void planningDoesNotWalkTheEntriesOfTheIndex() {
    final long before = deadEntriesSkipped();
    for (int i = 0; i < 3; i++)
      assertThat(count(ROW_ONLY_KEY)).isEqualTo(5L);
    assertThat(profile(ROW_ONLY_KEY)).contains("(n:Key");
    assertThat(deadEntriesSkipped()).as("a walk of every entry of the index skips its 200 tombstones").isEqualTo(before);
  }

  private long deadEntriesSkipped() {
    final TypeIndex index = database.getSchema().getType("Key").getAllIndexes(true).iterator().next();
    return index.getStats().getOrDefault("deadEntriesSkipped", 0L);
  }

  private long count(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }

  private String profile(final String query) {
    try (final ResultSet rs = database.query("opencypher", "PROFILE " + query)) {
      while (rs.hasNext())
        rs.next();
      return rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
    }
  }

  private static String lineOf(final String plan, final String text) {
    for (final String line : plan.split("\n"))
      if (line.contains(text))
        return line;
    return "";
  }
}
