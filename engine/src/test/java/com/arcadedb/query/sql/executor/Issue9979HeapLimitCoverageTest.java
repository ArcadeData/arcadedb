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
package com.arcadedb.query.sql.executor;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.exception.HeapLimitExceededException;
import com.arcadedb.function.sql.DefaultSQLFunctionFactory;
import com.arcadedb.function.sql.SQLAggregatedFunction;
import com.arcadedb.graph.MutableVertex;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9979 (#9830: #9792, #9793): every buffer that grows with the input is under both heap limits, the per-operation
 * element cap ({@code arcadedb.queryMaxHeapElementsAllowedPerOp}) and the heap budget all queries share
 * ({@code arcadedb.queryMaxHeapRAM}). A LET subquery, the MATCH prefetch and the SQL aggregates that keep every value were
 * invisible to the cap. The visited set of a TRAVERSE, a couple of primitives per record, is charged to the budget alone.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9979HeapLimitCoverageTest extends TestHelper {
  private static final int  RECORDS = 4_000;
  private static final long CAP     = 100L;

  /** Every statement below holds the whole of D, or far more of it than the cap, in one buffer. */
  private static final List<String> SQL_OVER_CAP = List.of(//
      // the controls: the steps that already charged
      "SELECT k FROM D ORDER BY k",//
      "SELECT DISTINCT k FROM D",//
      "SELECT count(DISTINCT k) FROM D",//
      // #9792
      "SELECT $x.size() AS s FROM D LET $x = (SELECT k FROM D) LIMIT 1",//
      "SELECT $x.size() AS s FROM D LET $x = (SELECT k FROM D WHERE k >= $parent.$current.k) WHERE k = 0",//
      // #10080: the same subquery written inline, and with a modifier that moves the LET to the expression step
      "SELECT (SELECT k FROM D).size() AS s FROM D LIMIT 1",//
      "SELECT $x AS s FROM D LET $x = (SELECT k FROM D).size() LIMIT 1",//
      // #9793
      "SELECT list(k) FROM D",//
      "SELECT set(k) FROM D",//
      "SELECT unionAll(k) FROM D",//
      // intersect() holds at most its first value: two rows carrying the same 200 values
      "SELECT intersect(big) FROM D WHERE k < 2",//
      "SELECT symmetricDifference(k) FROM D",//
      "SELECT map(m) FROM D",//
      "SELECT percentile(k, 0.5) FROM D",//
      "SELECT median(k) FROM D",//
      "SELECT mode(k) FROM D",//
      "SELECT ts.percentile(k, 0.5) FROM D",//
      "SELECT ts.rank(k, t) FROM D",//
      "SELECT ts.movingAvg(k, 3) FROM D",//
      "SELECT ts.lead(k, 1, t) FROM D",//
      "SELECT ts.lag(k, 1, t) FROM D",//
      "SELECT ts.rate(k, t) FROM D",//
      "SELECT ts.interpolate(k, 'prev', t) FROM D",//
      "SELECT g, list(k) FROM D GROUP BY g",//
      "SELECT g, percentile(k, 0.5) FROM D GROUP BY g");

  private static final List<String> CYPHER_OVER_CAP = List.of(//
      // the controls
      "MATCH (d:D) RETURN collect(d.k)",//
      "MATCH (d:D) RETURN collect(DISTINCT d.k)",//
      // #9793
      "MATCH (d:D) RETURN percentileCont(d.k, 0.5)",//
      "MATCH (d:D) RETURN percentileDisc(d.k, 0.5)",//
      "MATCH (d:D) RETURN count(DISTINCT d.k)",//
      // the same SQL aggregates, reached from openCypher
      "MATCH (d:D) RETURN sql.list(d.k)",//
      "MATCH (d:D) RETURN d.g AS g, sql.list(d.k) AS l",//
      "MATCH (d:D) RETURN sql.mode(d.k)");

  /** The aggregates the statements above cover; a newly registered one that keeps every value must be added there. */
  private static final Set<String> COVERED_AGGREGATES = Set.of("list", "set", "unionall", "intersect", "symmetricdifference", "map",
      "percentile", "median", "mode", "ts.percentile", "ts.rank", "ts.movingavg", "ts.lead", "ts.lag", "ts.rate", "ts.interpolate");

  @AfterEach
  void resetBudget() {
    GlobalConfiguration.QUERY_MAX_HEAP_RAM.reset();
  }

  @Test
  void everyBufferFailsPastTheElementCap() {
    createData(false);
    setCap(CAP);
    // every statement that slipped past the cap, all of them at once rather than the first one
    final List<String> notCapped = new ArrayList<>();
    for (final String sql : SQL_OVER_CAP)
      if (!failsOnTheCap("sql", sql))
        notCapped.add(sql);
    for (final String cypher : CYPHER_OVER_CAP)
      if (!failsOnTheCap("opencypher", cypher))
        notCapped.add(cypher);
    assertThat(notCapped).isEmpty();
  }

  private boolean failsOnTheCap(final String language, final String query) {
    try {
      drain(language, query);
      return false;
    } catch (final HeapLimitExceededException e) {
      assertThat(e.getMessage()).as(query).contains(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getKey());
      return true;
    }
  }

  /** The same statements, with room to spare: what failed above was the cap, not the statement. */
  @Test
  void everyStatementRunsBelowTheElementCap() {
    createData(false);
    setCap(100_000L);
    for (final String sql : SQL_OVER_CAP)
      assertThat(drain("sql", sql)).as(sql).isGreaterThan(0);
    for (final String cypher : CYPHER_OVER_CAP)
      assertThat(drain("opencypher", cypher)).as(cypher).isGreaterThan(0);

    assertThat(single("SELECT $x.size() AS s FROM D LET $x = (SELECT k FROM D) LIMIT 1").<Integer>getProperty("s")).isEqualTo(RECORDS);
    assertThat(single("SELECT count(*) AS c FROM (TRAVERSE out('R') FROM (SELECT FROM D WHERE k = 0) WHILE $depth < 5000)")
        .<Number>getProperty("c").intValue()).isEqualTo(RECORDS);
    assertThat(single("SELECT list(k) AS l FROM D").<List<?>>getProperty("l")).hasSize(RECORDS);
    // one bridge per group, each counting its own values from zero
    try (final ResultSet rs = database.query("opencypher", "MATCH (d:D) RETURN d.g AS g, sql.list(d.k) AS l")) {
      int groups = 0;
      while (rs.hasNext()) {
        assertThat(rs.next().<List<?>>getProperty("l")).hasSize(RECORDS / 2);
        ++groups;
      }
      assertThat(groups).isEqualTo(2);
    }
  }

  /** The cap counts what a function holds, not what it is given: a set() of two distinct values over 4000 rows is fine. */
  @Test
  void theCapCountsWhatAFunctionHoldsNotWhatItIsGiven() {
    createData(false);
    setCap(CAP);
    assertThat(single("SELECT set(g) AS s FROM D").<Set<?>>getProperty("s")).hasSize(2);
    assertThat(single("SELECT mode(g) AS m FROM D").<List<?>>getProperty("m")).hasSize(2);
    assertThat(drain("opencypher", "MATCH (d:D) RETURN sql.set(d.g)")).isEqualTo(1);
    // difference() keeps nothing between rows: it answers each row on its own arguments
    assertThat(drain("sql", "SELECT difference(ks, [0]) FROM D")).isEqualTo(RECORDS);
  }

  /** With the shared budget disabled, the element cap is all there is, and it still fails the query. */
  @Test
  void theCapFailsTheQueryWithTheBudgetDisabled() {
    createData(false);
    setCap(CAP);
    GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(0L);
    for (final String sql : List.of("SELECT list(k) FROM D", "SELECT percentile(k, 0.5) FROM D",
        "SELECT $x.size() AS s FROM D LET $x = (SELECT k FROM D) LIMIT 1"))
      assertThatThrownBy(() -> drain("sql", sql)).as(sql).isInstanceOf(HeapLimitExceededException.class);
  }

  /** The bytes of a LET subquery reach the budget all queries share, and go back to it when the query ends. */
  @Test
  void aLetSubqueryIsChargedToTheSharedBudgetAndReleased() {
    createData(true);
    setCap(-1L);
    final long baseline = QueryHeapBudget.getReservedBytes();

    GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(1L);
    for (final String sql : List.of("SELECT u FROM D ORDER BY u",//
        "SELECT $x.size() AS s FROM D LET $x = (SELECT u FROM D) LIMIT 1",//
        "SELECT $x.size() AS s FROM D LET $x = (SELECT u FROM D WHERE k >= $parent.$current.k) WHERE k = 0"))
      assertThatThrownBy(() -> drain("sql", sql)).as(sql).hasMessageContaining(GlobalConfiguration.QUERY_MAX_HEAP_RAM.getKey());
    assertThat(QueryHeapBudget.getReservedBytes()).isLessThanOrEqualTo(baseline);

    // with room, the global LET holds its ~4MB for as long as the query runs, and gives it back when it is closed
    GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(256L);
    try (final ResultSet rs = database.query("sql", "SELECT $x.size() AS s FROM D LET $x = (SELECT u FROM D)")) {
      assertThat(rs.next().<Integer>getProperty("s")).isEqualTo(RECORDS);
      assertThat(QueryHeapBudget.getReservedBytes()).isGreaterThan(baseline + RECORDS * 1_000L);
    }
    assertThat(QueryHeapBudget.getReservedBytes()).isLessThanOrEqualTo(baseline);
  }

  /**
   * The visited set of a TRAVERSE is charged to the shared budget, not capped in number: a traversal reaching more records
   * than the element cap runs, one that would take the budget past its limit fails, and gives back what it took.
   */
  @Test
  void aTraverseVisitedSetIsChargedToTheBudgetNotCapped() {
    createData(false);
    setCap(CAP);
    for (final String strategy : List.of("DEPTH_FIRST", "BREADTH_FIRST"))
      assertThat(single("SELECT count(*) AS c FROM (TRAVERSE out('R') FROM (SELECT FROM D WHERE k = 0) WHILE $depth < 5000 STRATEGY "
          + strategy + ")").<Number>getProperty("c").intValue()).as(strategy).isEqualTo(RECORDS);

    // a hub with 80,000 leaves: about 2MB of visited entries breadth first, 11MB depth first, past a 1MB budget
    final int leaves = 80_000;
    database.command("sql", "CREATE VERTEX TYPE Hub");
    database.command("sql", "CREATE VERTEX TYPE Leaf");
    database.command("sql", "CREATE EDGE TYPE Spoke");
    database.transaction(() -> {
      final MutableVertex hub = database.newVertex("Hub").save();
      for (int i = 0; i < leaves; i++)
        hub.newEdge("Spoke", database.newVertex("Leaf").save());
    });
    final long baseline = QueryHeapBudget.getReservedBytes();
    GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(1L);
    for (final String strategy : List.of("DEPTH_FIRST", "BREADTH_FIRST")) {
      assertThatThrownBy(() -> drain("sql", "SELECT count(*) AS c FROM (TRAVERSE out('Spoke') FROM Hub STRATEGY " + strategy + ")"))
          .as(strategy).hasMessageContaining(GlobalConfiguration.QUERY_MAX_HEAP_RAM.getKey());
      assertThat(QueryHeapBudget.getReservedBytes()).as(strategy).isLessThanOrEqualTo(baseline);
    }

    GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(256L);
    for (final String strategy : List.of("DEPTH_FIRST", "BREADTH_FIRST"))
      assertThat(single("SELECT count(*) AS c FROM (TRAVERSE out('Spoke') FROM Hub STRATEGY " + strategy + ")")
          .<Number>getProperty("c").intValue()).as(strategy).isEqualTo(leaves + 1);
  }

  /** The prefetch a MATCH runs on a small root holds that root's records for the whole statement. */
  @Test
  void theMatchPrefetchFailsPastTheElementCap() {
    createData(false);
    // a root estimated below the planner's prefetch threshold of 100 records
    database.command("sql", "CREATE DOCUMENT TYPE P");
    database.transaction(() -> {
      for (int i = 0; i < 50; i++)
        database.newDocument("P").set("k", i).save();
    });
    final String match = "MATCH {type: P, as: p}, {type: D, as: d, where: (k = 0)} RETURN count(*) AS c";
    try (final ResultSet rs = database.query("sql", "EXPLAIN " + match)) {
      assertThat(rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2)).contains("PREFETCH");
    }
    setCap(10L);
    assertThatThrownBy(() -> drain("sql", match)).isInstanceOf(HeapLimitExceededException.class).hasMessageContaining("MATCH prefetch of p");
    setCap(100L);
    assertThat(single(match).<Number>getProperty("c").intValue()).isEqualTo(50);
  }

  /** Every registered SQL aggregate that keeps every value is one the statements of this test exercise. */
  @Test
  void everyBufferingAggregateIsCovered() {
    final DefaultSQLFunctionFactory factory = DefaultSQLFunctionFactory.getInstance();
    final Set<String> buffering = new TreeSet<>();
    // the cap counts what such a function holds: one that does not say counts every value it is given, which over-counts
    // a function that keeps distinct values only
    final Set<String> notReportingHeld = new TreeSet<>();
    for (final String name : factory.getFunctionNames())
      if (factory.getFunctionInstance(name) instanceof SQLAggregatedFunction function && function.holdsEveryValue()) {
        buffering.add(name.toLowerCase(Locale.ENGLISH));
        try {
          if (function.getClass().getMethod("getHeldValues").getDeclaringClass() == SQLAggregatedFunction.class)
            notReportingHeld.add(name);
        } catch (final NoSuchMethodException e) {
          throw new AssertionError(e);
        }
      }
    assertThat(COVERED_AGGREGATES).containsAll(buffering);
    assertThat(notReportingHeld).isEmpty();
  }

  private void createData(final boolean withPayload) {
    database.command("sql", "CREATE VERTEX TYPE D");
    database.command("sql", "CREATE EDGE TYPE R");
    database.transaction(() -> {
      MutableVertex previous = null;
      for (int i = 0; i < RECORDS; i++) {
        final MutableVertex v = database.newVertex("D").set("k", i).set("g", i % 2).set("t", 1_000L * i).set("ks", List.of(i, i + 1))
            .set("m", Map.of("key" + i, i));
        if (i < 2)
          v.set("big", IntStream.range(0, 200).boxed().toList());
        if (withPayload)
          v.set("u", String.valueOf(i).repeat(1_000 / String.valueOf(i).length() + 1));
        v.save();
        if (previous != null)
          previous.newEdge("R", v);
        previous = v;
      }
    });
  }

  private void setCap(final long cap) {
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP, cap);
  }

  private int drain(final String language, final String query) {
    int rows = 0;
    try (final ResultSet rs = database.query(language, query)) {
      while (rs.hasNext()) {
        rs.next();
        ++rows;
      }
    }
    return rows;
  }

  private Result single(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      assertThat(rs.hasNext()).as(sql).isTrue();
      return rs.next();
    }
  }
}
