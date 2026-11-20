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
import com.arcadedb.query.opencypher.ast.ComparisonExpression;
import com.arcadedb.query.opencypher.executor.operators.NodeIndexRangeScan;
import com.arcadedb.query.opencypher.executor.operators.NodeIndexSeek;
import com.arcadedb.query.opencypher.optimizer.RangePredicate;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #11889: a {@code count(*)} whose ordinary plan starts from an index range seek was taken by a CSR
 * count push-down ({@code COUNT CHAIN PATHS}, {@code COUNT STAR JOIN}) however narrow the range, so it walked the whole label
 * while the same statement with {@code sum(1)} cost the range and the expansion from it. The optimizer estimates any range at a
 * fixed share of the label, so the range's index entries are now counted, up to a quarter of the label: a range holding fewer is
 * left to the ordinary plan, a wider one keeps the push-down.
 * <p>
 * An equality on a NOTUNIQUE key had the same problem: its estimate is an average over the keys, which was never under the
 * hundredth of the label an equality seek has to find to decline the push-down, however few vertices the key held. Its entries
 * are now counted too, up to that hundredth.
 * <p>
 * The probe is bounded by the index entries it READS, not by the ones it counts: an index declared on a supertype also holds the
 * siblings' entries, which the scan reads and drops, so a range or a key that makes it read as many as the cap keeps the
 * push-down however few of them are the label's. And a whole key is looked up with the cap as its limit, so a key that holds
 * thousands of entries is not collected whole to be told "not selective".
 * <p>
 * {@code P} has {@value #VERTICES} vertices with a UNIQUE {@code id} (0 to 399), a NOTUNIQUE {@code g} ({@code id % 10}, 40
 * vertices a key), a NOTUNIQUE {@code h} ({@code id % 200}, 2 vertices a key) and a NOTUNIQUE {@code m} ({@code id % 133}: 4
 * vertices for the key 0, 3 for every other); each has two outgoing {@code K} relationships. A quarter of the label is 100
 * vertices, a hundredth 4.
 */
class CypherRangeAnchoredCountPushDownIssue11889Test extends TestHelper {
  private static final int VERTICES = 400;

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE P");
    database.command("sql", "CREATE EDGE TYPE K");
    database.command("sql", "CREATE PROPERTY P.id INTEGER");
    database.command("sql", "CREATE PROPERTY P.g INTEGER");
    database.command("sql", "CREATE PROPERTY P.h INTEGER");
    database.command("sql", "CREATE PROPERTY P.m INTEGER");
    database.command("sql", "CREATE INDEX ON P (id) UNIQUE");
    database.command("sql", "CREATE INDEX ON P (g) NOTUNIQUE");
    database.command("sql", "CREATE INDEX ON P (h) NOTUNIQUE");
    database.command("sql", "CREATE INDEX ON P (m) NOTUNIQUE");
    database.transaction(() -> {
      database.command("opencypher", "UNWIND range(0, $last) AS i CREATE (:P {id: i, g: i % 10, h: i % 200, m: i % 133})",
          Map.of("last", VERTICES - 1));
      database.command("opencypher", "MATCH (a:P), (b:P) WHERE b.id = (a.id * 7 + 1) % $n OR b.id = (a.id * 13 + 5) % $n "
          + "CREATE (a)-[:K]->(b)", Map.of("n", VERTICES));
    });
  }

  @Test
  void aNarrowRangeIsNotPushedDown() {
    // the shapes of the issue: a star with a filtered arm and without
    assertAnchored("MATCH (p:P) WHERE p.id < 20 MATCH (p)-[:K]->(q:P) WHERE q.id < 200 RETURN count(*) AS c", Map.of());
    assertAnchored("MATCH (p:P) WHERE p.id < 20 MATCH (p)-[:K]->(q:P) RETURN count(*) AS c", Map.of());
    // a chain, whichever end the range is written on
    assertAnchored("MATCH (p:P)-[:K]->(q:P) WHERE p.id < 20 RETURN count(*) AS c", Map.of());
    assertAnchored("MATCH (q:P)-[:K]->(p:P) WHERE p.id < 20 RETURN count(*) AS c", Map.of());
    // both bounds, and a range that holds nothing
    assertAnchored("MATCH (p:P)-[:K]->(q:P) WHERE p.id >= 200 AND p.id < 230 RETURN count(*) AS c", Map.of());
    assertAnchored("MATCH (p:P)-[:K]->(q:P) WHERE p.id < -5 RETURN count(*) AS c", Map.of());
    // the entries of a NOTUNIQUE index are counted, not its keys: one key of ten is 40 vertices
    assertAnchored("MATCH (p:P)-[:K]->(q:P) WHERE p.g < 1 RETURN count(*) AS c", Map.of());
  }

  @Test
  void aWideRangeKeepsItsPushDown() {
    assertPushedDown("MATCH (p:P) WHERE p.id < 300 MATCH (p)-[:K]->(q:P) WHERE q.id < 300 RETURN count(*) AS c", Map.of(),
        "COUNT STAR JOIN");
    assertPushedDown("MATCH (p:P)-[:K]->(q:P) WHERE p.id < 300 RETURN count(*) AS c", Map.of(), "COUNT CHAIN PATHS");
    assertPushedDown("MATCH (p:P)-[:K]->(q:P) WHERE p.g < 5 RETURN count(*) AS c", Map.of(), "COUNT CHAIN PATHS");
  }

  /** The probe stops at a quarter of the label: 99 entries are fewer, 100 are not. */
  @Test
  void theRangeIsCountedUpToAQuarterOfTheLabel() {
    assertAnchored("MATCH (p:P)-[:K]->(q:P) WHERE p.id < 99 RETURN count(*) AS c", Map.of());
    assertPushedDown("MATCH (p:P)-[:K]->(q:P) WHERE p.id < 100 RETURN count(*) AS c", Map.of(), "COUNT CHAIN PATHS");
  }

  /** The range is counted with the parameters of each execution: the same statement takes either plan. */
  @Test
  void aParameterBoundIsCountedPerExecution() {
    final String query = "MATCH (p:P) WHERE p.id < $hi MATCH (p)-[:K]->(q:P) RETURN count(*) AS c";
    assertAnchored(query, Map.of("hi", 20));
    assertPushedDown(query, Map.of("hi", 300), "COUNT STAR JOIN");
    assertAnchored(query, Map.of("hi", 20));
  }

  /**
   * An index declared on a supertype holds the keys of every subtype, and the scan reads every entry of the range to keep the
   * label's own. The probe stops at the cap of entries READ: a range that holds 5 of DocA's entries among 400 of DocB's would
   * be read whole by the ordinary plan, against the 40 vertices of the label the push-down walks.
   */
  @Test
  void anInheritedIndexIsBoundedByTheEntriesRead() {
    createDocs();
    // 5 entries, all DocA's, against a quarter of DocA's 40 vertices
    assertAnchored("MATCH (p:Part)-[:PART_OF]->(d:DocA) WHERE d.key >= 10000 AND d.key < 15000 RETURN count(*) AS c", Map.of());
    // 8 entries, one of them DocA's: fewer than the cap read, and counted by the label
    assertAnchored("MATCH (p:Part)-[:PART_OF]->(d:DocA) WHERE d.key < 8 RETURN count(*) AS c", Map.of());
    // 405 entries, 5 of them DocA's: the cap is reached by what is read, not by what is counted
    assertPushedDown("MATCH (p:Part)-[:PART_OF]->(d:DocA) WHERE d.key < 5000 RETURN count(*) AS c", Map.of(), "COUNT CHAIN PATHS");
    assertPushedDown("MATCH (p:Part)-[:PART_OF]->(d:DocA) WHERE d.key < 30000 RETURN count(*) AS c", Map.of(), "COUNT CHAIN PATHS");
  }

  /** A NOTUNIQUE key is counted up to a hundredth of the label: 3 entries are fewer than the 4 of the cap, 4 are not. */
  @Test
  void aKeyIsCountedUpToAHundredthOfTheLabel() {
    assertAnchored("MATCH (p:P {m: 1})-[:K]->(q:P) RETURN count(*) AS c", Map.of());
    assertPushedDown("MATCH (p:P {m: 0})-[:K]->(q:P) RETURN count(*) AS c", Map.of(), "COUNT CHAIN PATHS");
    // an IN-list is counted value by value: 2 entries a value, 4 for two of them
    assertAnchored("MATCH (p:P) WHERE p.h IN [1] MATCH (p)<-[:K]-(x) RETURN count(x) AS c", Map.of());
    assertPushedDown("MATCH (p:P) WHERE p.h IN [1, 2] MATCH (p)<-[:K]-(x) RETURN count(x) AS c", Map.of(), "COUNT STAR JOIN");
  }

  /** The operators answer the entries they read, at most the cap, which is what the plan compares with it. */
  @Test
  void countEntriesStopsAtTheCap() {
    database.transaction(() -> {
      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(database);

      final NodeIndexRangeScan below50 = rangeScan("P", "P[id]", "id", 50);
      assertThat(below50.countEntries(context, 100L)).isEqualTo(50L);
      assertThat(below50.countEntries(context, 50L)).isEqualTo(50L);
      assertThat(below50.countEntries(context, 10L)).isEqualTo(10L);
      assertThat(rangeScan("P", "P[id]", "id", -5).countEntries(context, 10L)).isZero();

      assertThat(keySeek("P", "P[m]", "m", 1).countEntries(context, 4L)).isEqualTo(3L);
      assertThat(keySeek("P", "P[m]", "m", 0).countEntries(context, 4L)).isEqualTo(4L);
      // a key of 40 entries read to the cap only
      assertThat(keySeek("P", "P[g]", "g", 3).countEntries(context, 5L)).isEqualTo(5L);
      assertThat(keySeek("P", "P[g]", "g", 3).countEntries(context, 100L)).isEqualTo(40L);
      assertThat(keySeek("P", "P[g]", "g", 1000).countEntries(context, 5L)).isZero();

      // nothing is below a cap of one: a negative limit would be no limit at all
      assertThat(keySeek("P", "P[g]", "g", 3).countEntries(context, 0L)).isZero();
      assertThat(below50.countEntries(context, 0L)).isZero();
    });
  }

  /** The entries of the siblings of an inherited index are read, and only the label's own are counted, below the cap. */
  @Test
  void countEntriesOfAnInheritedIndexReadsTheSiblingsEntries() {
    createDocs();
    database.transaction(() -> {
      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(database);

      // keys 0 (DocA) and 1 to 7 (DocB)
      assertThat(rangeScan("DocA", "Doc[key]", "key", 8).countEntries(context, 10L)).isEqualTo(1L);
      // 405 entries, 5 of them DocA's, stopped at the 10 read
      assertThat(rangeScan("DocA", "Doc[key]", "key", 5000).countEntries(context, 10L)).isEqualTo(10L);
      assertThat(rangeScan("DocA", "Doc[key]", "key", 5000).countEntries(context, 1000L)).isEqualTo(5L);
      assertThat(keySeek("DocA", "Doc[key]", "key", 3000).countEntries(context, 10L)).isEqualTo(1L);
      // DocB's key, not DocA's
      assertThat(keySeek("DocA", "Doc[key]", "key", 3).countEntries(context, 10L)).isZero();
    });
  }

  /**
   * A NOTUNIQUE key that holds a few vertices of the label: the shapes of the issue's follow-up, a chain to an unlabeled node and a
   * star, with the anchor written inline, in a WHERE, and passed through a WITH.
   */
  @Test
  void aNotUniqueKeyHoldingFewVerticesIsNotPushedDown() {
    assertAnchored("MATCH (p:P {h: 7})<-[:K]-(x) RETURN count(*) AS c", Map.of());
    assertAnchored("MATCH (p:P) WHERE p.h = 7 MATCH (p)<-[:K]-(x) RETURN count(x) AS c", Map.of());
    assertAnchored("MATCH (p:P) WHERE p.h = 7 WITH p MATCH (p)<-[:K]-(x) RETURN count(x) AS c", Map.of());
    assertAnchored("MATCH (p:P {h: $h})-[:K]->(q:P) RETURN count(*) AS c", Map.of("h", 7));
    // a key no vertex holds
    assertAnchored("MATCH (p:P {h: 1000})<-[:K]-(x) RETURN count(*) AS c", Map.of());
  }

  /** A NOTUNIQUE key holding a hundredth of the label or more keeps the push-down, as before. */
  @Test
  void aNotUniqueKeyHoldingManyVerticesKeepsItsPushDown() {
    assertPushedDown("MATCH (p:P {g: 7})<-[:K]-(x) RETURN count(*) AS c", Map.of(), "COUNT CHAIN PATHS");
    assertPushedDown("MATCH (p:P) WHERE p.g = 7 MATCH (p)<-[:K]-(x) RETURN count(x) AS c", Map.of(), "COUNT STAR JOIN");
  }

  /** DocA has 40 vertices, keys 0, 1000, ... 39000; its sibling DocB 400, keys 1 to 400: one UNIQUE index on Doc holds both. */
  private void createDocs() {
    database.command("sql", "CREATE VERTEX TYPE Doc");
    database.command("sql", "CREATE PROPERTY Doc.key INTEGER");
    database.command("sql", "CREATE INDEX ON Doc (key) UNIQUE");
    database.command("sql", "CREATE VERTEX TYPE DocA EXTENDS Doc");
    database.command("sql", "CREATE VERTEX TYPE DocB EXTENDS Doc");
    database.command("sql", "CREATE VERTEX TYPE Part");
    database.command("sql", "CREATE EDGE TYPE PART_OF");
    database.transaction(() -> {
      database.command("opencypher", "UNWIND range(0, 39) AS i CREATE (:Part)-[:PART_OF]->(:DocA {key: i * 1000})");
      database.command("opencypher", "UNWIND range(1, 400) AS i CREATE (:Part)-[:PART_OF]->(:DocB {key: i})");
    });
  }

  /** The seek of {@code property < upper} over {@code label}, as the optimizer builds it. */
  private static NodeIndexRangeScan rangeScan(final String label, final String index, final String property, final int upper) {
    return new NodeIndexRangeScan("n", label, property,
        List.of(new RangePredicate(property, ComparisonExpression.Operator.LESS_THAN, upper, false)), index, 1.0, 1L);
  }

  /** The seek of {@code property = value} over {@code label}, as the optimizer builds it. */
  private static NodeIndexSeek keySeek(final String label, final String index, final String property, final int value) {
    return new NodeIndexSeek("n", label, property, value, index, 1.0, 1L);
  }

  private void assertAnchored(final String query, final Map<String, Object> params) {
    assertThat(explainOf(query, params)).as(query).doesNotContain("COUNT CHAIN PATHS").doesNotContain("COUNT STAR JOIN");
    assertSameCount(query, params);
  }

  private void assertPushedDown(final String query, final Map<String, Object> params, final String operator) {
    assertThat(explainOf(query, params)).as(query).contains(operator);
    assertSameCount(query, params);
  }

  /** The same statement with sum(1) never reaches the push-downs: the oracle for the count. Every node bound here is non-null. */
  private void assertSameCount(final String query, final Map<String, Object> params) {
    final String oracle = query.replace("count(*)", "sum(1)").replace("count(x)", "sum(1)");
    assertThat(oracle).as(query).isNotEqualTo(query);
    assertThat(scalarOf(query, params)).as(query).isEqualTo(scalarOf(oracle, params));
  }

  private long scalarOf(final String query, final Map<String, Object> params) {
    try (final ResultSet rs = database.query("opencypher", query, params)) {
      assertThat(rs.hasNext()).as(query).isTrue();
      final Number value = rs.next().getProperty("c");
      return value == null ? 0L : value.longValue();
    }
  }

  private String explainOf(final String query, final Map<String, Object> params) {
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query, params)) {
      assertThat(rs.hasNext()).as(query).isTrue();
      return rs.next().getProperty("executionPlanAsString");
    }
  }
}
