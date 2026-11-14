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
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.index.Index;
import com.arcadedb.serializer.BinaryComparator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Index lookups by range and equality bounds: the bound must select exactly the rows a scan with the same condition selects,
 * whatever its kind, its spelling (literal or parameter), the index kind, and whether the planner serves the range from the
 * index or gives way to a scan.
 */
class IndexRangeBoundsTest extends TestHelper {

  // ---- #6188: RID range bound passed as a parameter ----
  /*
   * WHERE @rid > :p bound to a RID, or to its string spelling, answered zero rows while the literal form WHERE @rid > #x:y was
   * correct.
   *
   * Root cause: GtOperator alone among the range operators refused any comparison between an Identifiable left operand and a
   * non-Identifiable right operand, even though RID#compareTo(Object) already knows how to compare a RID against its string
   * spelling. A record's @rid resolves to a DatabaseRID, a subclass of RID that Type#convert does not recognize (it only
   * special-cases an exact RID.class target), so the attempted string-to-RID coercion silently failed and left a bare String on
   * the right - which the now-removed guard then rejected outright. <, >= and <= never had that guard and were unaffected.
   */

  private RID       lowRid;
  private List<RID> allRidsAscending;
  private int       expectedGreaterThanLow;
  private int       expectedGreaterOrEqualLow;
  private int       expectedLessThanLow;
  private int       expectedLessOrEqualLow;

  private void ridRangeSetup() {
    database.getSchema().createDocumentType("Doc", 4);

    database.transaction(() -> {
      for (int i = 0; i < 40; i++)
        database.newDocument("Doc").set("i", i).save();
    });

    // Pick a RID roughly in the middle of the RID space so every range below is meaningfully selective.
    final ResultSet all = database.query("sql", "SELECT @rid AS r FROM Doc ORDER BY @rid");
    allRidsAscending = new ArrayList<>();
    while (all.hasNext())
      allRidsAscending.add(all.next().getProperty("r"));
    all.close();

    lowRid = allRidsAscending.get(allRidsAscending.size() / 2);

    for (final RID r : allRidsAscending) {
      final int cmp = r.compareTo(lowRid);
      if (cmp > 0)
        expectedGreaterThanLow++;
      if (cmp >= 0)
        expectedGreaterOrEqualLow++;
      if (cmp < 0)
        expectedLessThanLow++;
      if (cmp <= 0)
        expectedLessOrEqualLow++;
    }

    assertThat(expectedGreaterThanLow).as("fixture must be selective for the test to mean anything").isGreaterThan(0);
    assertThat(expectedLessThanLow).as("fixture must be selective for the test to mean anything").isGreaterThan(0);
  }

  private long ridCount(final String sql, final Object paramValue) {
    final ResultSet rs = paramValue == null ? database.query("sql", sql) : database.query("sql", sql, Map.of("p", paramValue));
    assertThat(rs.hasNext()).isTrue();
    final long count = rs.next().<Number>getProperty("c").longValue();
    rs.close();
    return count;
  }

  /** Regression test for issue #6188. */
  @Test
  void literalRidRangeReturnsCorrectCounts() {
    ridRangeSetup();
    assertThat(ridCount("SELECT count() as c FROM Doc WHERE @rid > " + lowRid, null)).isEqualTo(expectedGreaterThanLow);
    assertThat(ridCount("SELECT count() as c FROM Doc WHERE @rid >= " + lowRid, null)).isEqualTo(expectedGreaterOrEqualLow);
    assertThat(ridCount("SELECT count() as c FROM Doc WHERE @rid < " + lowRid, null)).isEqualTo(expectedLessThanLow);
    assertThat(ridCount("SELECT count() as c FROM Doc WHERE @rid <= " + lowRid, null)).isEqualTo(expectedLessOrEqualLow);
  }

  /** Regression test for issue #6188. */
  @Test
  void boundRidObjectParameterMatchesLiteralOnEveryRangeOperator() {
    ridRangeSetup();
    assertThat(ridCount("SELECT count() as c FROM Doc WHERE @rid > :p", lowRid)).as(">").isEqualTo(expectedGreaterThanLow);
    assertThat(ridCount("SELECT count() as c FROM Doc WHERE @rid >= :p", lowRid)).as(">=").isEqualTo(expectedGreaterOrEqualLow);
    assertThat(ridCount("SELECT count() as c FROM Doc WHERE @rid < :p", lowRid)).as("<").isEqualTo(expectedLessThanLow);
    assertThat(ridCount("SELECT count() as c FROM Doc WHERE @rid <= :p", lowRid)).as("<=").isEqualTo(expectedLessOrEqualLow);
  }

  /** Regression test for issue #6188. */
  @Test
  void boundRidStringParameterMatchesLiteralOnEveryRangeOperator() {
    ridRangeSetup();
    final String p = lowRid.toString();
    assertThat(ridCount("SELECT count() as c FROM Doc WHERE @rid > :p", p)).as(">").isEqualTo(expectedGreaterThanLow);
    assertThat(ridCount("SELECT count() as c FROM Doc WHERE @rid >= :p", p)).as(">=").isEqualTo(expectedGreaterOrEqualLow);
    assertThat(ridCount("SELECT count() as c FROM Doc WHERE @rid < :p", p)).as("<").isEqualTo(expectedLessThanLow);
    assertThat(ridCount("SELECT count() as c FROM Doc WHERE @rid <= :p", p)).as("<=").isEqualTo(expectedLessOrEqualLow);
  }

  /** Regression test for issue #6188. */
  @Test
  void boundParameterCursorPagingReturnsEveryRowExactlyOnce() {
    ridRangeSetup();
    // The shape called out in the issue: paging through a type with WHERE @rid > :last ORDER BY @rid LIMIT n.
    // Every row must be visited exactly once and the walk must terminate.
    final List<RID> visited = new ArrayList<>();
    Object cursor = null;
    for (int guard = 0; guard < allRidsAscending.size() + 2; guard++) {
      final String sql = cursor == null
          ? "SELECT @rid AS r FROM Doc ORDER BY @rid LIMIT 7"
          : "SELECT @rid AS r FROM Doc WHERE @rid > :p ORDER BY @rid LIMIT 7";
      final ResultSet rs = cursor == null ? database.query("sql", sql) : database.query("sql", sql, Map.of("p", cursor.toString()));
      int fetched = 0;
      RID last = null;
      while (rs.hasNext()) {
        last = rs.next().getProperty("r");
        visited.add(last);
        fetched++;
      }
      rs.close();
      if (fetched == 0)
        break;
      cursor = last;
    }

    assertThat(visited).as("cursor paging with a bound, string-spelled RID must not stop after the first page")
        .containsExactlyElementsOf(allRidsAscending);
  }

  /** Regression test for issue #6188. */
  @Test
  void malformedRidStringParameterIsSafelyIncomparableRatherThanThrowing() {
    ridRangeSetup();
    // Review follow-up on #6188: a right-hand String that isn't RID-shaped reaches RID#compareTo(Object), which
    // parses it as "#bucket:position" and throws IllegalArgumentException/NumberFormatException for anything
    // that doesn't fit. All four range operators must report "not comparable" (false) rather than let that parse
    // failure escape mid-scan - the same "no defined ordering" contract this file already applies to a numeric
    // conversion failure (#5900).
    for (final String malformed : List.of("not-a-rid", "1:2", "#1", "#abc:def")) {
      assertThat(ridCount("SELECT count() as c FROM Doc WHERE @rid > :p", malformed)).as("> %s", malformed).isEqualTo(0);
      assertThat(ridCount("SELECT count() as c FROM Doc WHERE @rid >= :p", malformed)).as(">= %s", malformed).isEqualTo(0);
      assertThat(ridCount("SELECT count() as c FROM Doc WHERE @rid < :p", malformed)).as("< %s", malformed).isEqualTo(0);
      assertThat(ridCount("SELECT count() as c FROM Doc WHERE @rid <= :p", malformed)).as("<= %s", malformed).isEqualTo(0);
    }
  }

  // ---- #8333: index range selectivity, a non-selective range gives way to a scan or physical order ----
  /*
   * The SQL planner served a range from an index whatever share of the type it returned, so a non-selective range (TPC-H Q1
   * reads ~98% of lineitem) was fetched one random page access per record and fell far behind a plain scan once the type
   * outgrew the page cache. The index fetch now reads the matching entries first and either gives way to a scan filtered by the
   * same condition, or loads the records in physical order - wherever the order the rows arrive in cannot show in the output: an
   * aggregation, or an ORDER BY the index does not serve. A query returning its rows as they come keeps the index order it
   * always had.
   */

  private static final int       ROWS = 2_000;
  private static final LocalDate BASE = LocalDate.of(1992, 1, 1);

  private void lineItemSetup() {
    database.command("sql", "CREATE DOCUMENT TYPE LineItem");
    database.command("sql", "CREATE PROPERTY LineItem.l_shipdate STRING");
    database.command("sql", "CREATE PROPERTY LineItem.l_day DATE");
    database.command("sql", "CREATE PROPERTY LineItem.l_seq INTEGER");
    database.command("sql", "CREATE PROPERTY LineItem.l_code STRING");
    database.command("sql", "CREATE INDEX ON LineItem (l_shipdate) NOTUNIQUE");
    database.command("sql", "CREATE INDEX ON LineItem (l_day) NOTUNIQUE");
    database.command("sql", "CREATE INDEX ON LineItem (l_seq) UNIQUE");
    database.command("sql", "CREATE INDEX ON LineItem (l_code COLLATE ci) NOTUNIQUE");
    database.transaction(() -> {
      for (int i = 0; i < ROWS; i++) {
        // Day i, but inserted in a shuffled order so index order and physical order differ
        final int day = (int) ((i * 7919L) % ROWS);
        database.newDocument("LineItem")
            .set("l_shipdate", BASE.plusDays(day).toString())
            .set("l_day", BASE.plusDays(day))
            .set("l_seq", i)
            .set("l_code", i % 2 == 0 ? "abc" : "ABC")
            // The same for every row: sorting on it keeps the order the rows arrive in (the sort is stable)
            .set("l_flag", "x")
            .save();
      }
    });
  }

  @AfterEach
  void restoreBufferCap() {
    PhysicalOrderRidFetcher.maxBufferedRids = 1 << 20;
  }

  /** Regression test for issue #8333. */
  @Test
  void nonSelectiveRangeIsServedByAScan() {
    lineItemSetup();
    // ~98% of the rows, the shape of TPC-H Q1
    final String bound = BASE.plusDays(ROWS * 98 / 100).toString();
    final Execution execution = run("SELECT FROM LineItem WHERE l_shipdate <= '" + bound + "' ORDER BY l_seq", Map.of());

    assertThat(execution.strategy()).isEqualTo(GetValueFromIndexEntryStep.Strategy.SCAN);
    assertThat(execution.plan()).contains("FETCH FROM INDEX LineItem[l_shipdate]").contains("served by full scan");
    assertThat(execution.rids()).containsExactlyInAnyOrderElementsOf(expected(d -> d.getString("l_shipdate").compareTo(bound) <= 0));
  }

  /** Regression test for issue #8333. */
  @Test
  void selectiveRangeIsLoadedInPhysicalOrder() {
    lineItemSetup();
    // ~5% of the rows, a one-year range like TPC-H Q6 on a smaller span
    final String from = BASE.plusDays(1000).toString();
    final String to = BASE.plusDays(1100).toString();
    final Execution execution = run(
        "SELECT FROM LineItem WHERE l_shipdate >= '" + from + "' AND l_shipdate < '" + to + "' ORDER BY l_flag", Map.of());

    assertThat(execution.strategy()).isEqualTo(GetValueFromIndexEntryStep.Strategy.PHYSICAL_ORDER);
    assertThat(execution.rids()).hasSize(100);
    assertThat(execution.rids()).containsExactlyInAnyOrderElementsOf(
        expected(d -> d.getString("l_shipdate").compareTo(from) >= 0 && d.getString("l_shipdate").compareTo(to) < 0));
    assertPhysicalOrder(execution.rids());
  }

  /** Regression test for issue #8333. */
  @Test
  void theSameStatementDecidesPerParameterValue() {
    lineItemSetup();
    // The decision is taken when the statement runs, with its parameters bound, never when it is planned: a plan
    // reused for another value must not keep the first value's choice
    final String query = "SELECT sum(l_seq) AS s FROM LineItem WHERE l_shipdate >= :from";

    final String wide = BASE.plusDays(10).toString();
    assertThat(sumOfSeq(query, wide)).isEqualTo(expectedSumOfSeq(wide));
    assertThat(lastStrategy).isEqualTo(GetValueFromIndexEntryStep.Strategy.SCAN);

    // The same statement, with a value that matches 10 rows
    final String narrow = BASE.plusDays(ROWS - 10).toString();
    assertThat(sumOfSeq(query, narrow)).isEqualTo(expectedSumOfSeq(narrow));
    assertThat(lastStrategy).isEqualTo(GetValueFromIndexEntryStep.Strategy.PHYSICAL_ORDER);
  }

  private GetValueFromIndexEntryStep.Strategy lastStrategy;

  private long sumOfSeq(final String query, final String from) {
    try (final ResultSet rs = database.query("sql", query, Map.of("from", from))) {
      final long sum = rs.next().<Number>getProperty("s").longValue();
      lastStrategy = findStep(rs.getExecutionPlan().get()).getStrategy();
      return sum;
    }
  }

  private long expectedSumOfSeq(final String from) {
    long sum = 0;
    for (final RID rid : expected(d -> d.getString("l_shipdate").compareTo(from) >= 0))
      sum += rid.asDocument().getInteger("l_seq");
    return sum;
  }

  /** Regression test for issue #8333. */
  @Test
  void typedPropertyComparedWithAStringAnswersTheSameThroughTheScan() {
    lineItemSetup();
    // The index converts '1993-06-01' to the DATE the property holds; the scan compares the record's value with the
    // string. Both must pick the same rows.
    final LocalDate bound = BASE.plusDays(517);
    final Execution execution = run("SELECT FROM LineItem WHERE l_day > '" + bound + "' ORDER BY l_seq", Map.of());

    assertThat(execution.strategy()).isEqualTo(GetValueFromIndexEntryStep.Strategy.SCAN);
    assertThat(execution.rids()).containsExactlyInAnyOrderElementsOf(
        expected(d -> d.getLocalDate("l_day").isAfter(bound)));
  }

  /** Regression test for issue #8333. */
  @Test
  void betweenAndResidualConditionsKeepTheirMeaning() {
    lineItemSetup();
    final Execution execution = run(
        "SELECT FROM LineItem WHERE l_seq BETWEEN 100 AND 1900 AND l_seq % 3 = 0 ORDER BY l_flag", Map.of());

    assertThat(execution.strategy()).isEqualTo(GetValueFromIndexEntryStep.Strategy.SCAN);
    assertThat(execution.rids()).containsExactlyInAnyOrderElementsOf(
        expected(d -> d.getInteger("l_seq") >= 100 && d.getInteger("l_seq") <= 1900 && d.getInteger("l_seq") % 3 == 0));
  }

  /** Regression test for issue #8333. */
  @Test
  void betweenOrdersStringsLikeTheIndexOnTheScan() {
    lineItemSetup();
    // U+FF21 sorts above a surrogate pair in UTF-16 but below it in the UTF-8 byte order the index keeps (#6997).
    // The scan standing in for the index must answer BETWEEN in that order too, as it already did <= and >=.
    final String fullWidthA = "\uFF21";
    final String emoji = new String(Character.toChars(0x1F600));
    database.command("sql", "CREATE DOCUMENT TYPE Word");
    database.transaction(() -> {
      database.newDocument("Word").set("w", fullWidthA).save();
      database.newDocument("Word").set("w", emoji).save();
    });

    final Map<String, Object> bounds = Map.of("from", fullWidthA, "to", emoji);
    try (final ResultSet rs = database.query("sql", "SELECT FROM Word WHERE w BETWEEN :from AND :to", bounds)) {
      assertThat(rs.stream().count()).isEqualTo(2);
    }
    try (final ResultSet rs = database.query("sql", "SELECT FROM Word WHERE w >= :from AND w <= :to", bounds)) {
      assertThat(rs.stream().count()).isEqualTo(2);
    }
  }

  /** Regression test for issue #8333. */
  @Test
  void aggregationOverANonSelectiveRange() {
    lineItemSetup();
    final String bound = BASE.plusDays(ROWS / 2).toString();
    try (final ResultSet rs = database.query("sql",
        "SELECT count(*) AS n FROM LineItem WHERE l_shipdate > '" + bound + "' LIMIT 1")) {
      assertThat(rs.next().<Long>getProperty("n")).isEqualTo(
          expected(d -> d.getString("l_shipdate").compareTo(bound) > 0).size());
    }
  }

  /** Regression test for issue #8333. */
  @Test
  void rangeLargerThanTheBufferIsServedInSortedChunks() {
    lineItemSetup();
    PhysicalOrderRidFetcher.maxBufferedRids = 64;
    final String to = BASE.plusDays(300).toString();
    final Execution execution = run("SELECT FROM LineItem WHERE l_shipdate < '" + to + "' ORDER BY l_flag", Map.of());

    assertThat(execution.strategy()).isEqualTo(GetValueFromIndexEntryStep.Strategy.PHYSICAL_ORDER_CHUNKED);
    assertThat(execution.rids()).hasSize(300);
    assertThat(execution.rids()).containsExactlyInAnyOrderElementsOf(expected(d -> d.getString("l_shipdate").compareTo(to) < 0));
    for (int chunk = 0; chunk < execution.rids().size(); chunk += 64)
      assertPhysicalOrder(execution.rids().subList(chunk, Math.min(execution.rids().size(), chunk + 64)));
  }

  /** Regression test for issue #8333. */
  @Test
  void uncommittedChangesAreSeenByBothStrategies() {
    lineItemSetup();
    database.transaction(() -> {
      database.newDocument("LineItem").set("l_shipdate", "1991-01-01").set("l_seq", ROWS).set("l_code", "x").save();
      database.command("sql", "DELETE FROM LineItem WHERE l_seq = 5");

      final String wide = BASE.plusDays(ROWS).toString();
      final Execution scan = run("SELECT FROM LineItem WHERE l_shipdate < '" + wide + "' ORDER BY l_seq", Map.of());
      assertThat(scan.strategy()).isEqualTo(GetValueFromIndexEntryStep.Strategy.SCAN);
      assertThat(scan.rids()).containsExactlyInAnyOrderElementsOf(expected(d -> d.getString("l_shipdate").compareTo(wide) < 0));

      final Execution physical = run("SELECT FROM LineItem WHERE l_shipdate < '1992-01-03' ORDER BY l_seq", Map.of());
      assertThat(physical.strategy()).isEqualTo(GetValueFromIndexEntryStep.Strategy.PHYSICAL_ORDER);
      assertThat(physical.rids()).containsExactlyInAnyOrderElementsOf(
          expected(d -> d.getString("l_shipdate").compareTo("1992-01-03") < 0));
    });
  }

  /** Regression test for issue #8333. */
  @Test
  void subTypesAreScannedToo() {
    lineItemSetup();
    database.command("sql", "CREATE DOCUMENT TYPE SpecialLineItem EXTENDS LineItem");
    database.transaction(() -> {
      for (int i = 0; i < 200; i++)
        database.newDocument("SpecialLineItem").set("l_shipdate", BASE.plusDays(i).toString()).set("l_seq", ROWS + i)
            .set("l_code", "s").save();
    });

    final String bound = BASE.plusDays(1).toString();
    final Execution execution = run("SELECT FROM LineItem WHERE l_shipdate >= '" + bound + "' ORDER BY l_seq", Map.of());
    assertThat(execution.strategy()).isEqualTo(GetValueFromIndexEntryStep.Strategy.SCAN);
    assertThat(execution.rids()).containsExactlyInAnyOrderElementsOf(expected(d -> d.getString("l_shipdate").compareTo(bound) >= 0));
  }

  /** Regression test for issue #8333. */
  @Test
  void indexOrderServingOrderByIsKept() {
    lineItemSetup();
    final Execution execution = run("SELECT FROM LineItem WHERE l_shipdate > '1992-01-01' ORDER BY l_shipdate", Map.of());
    assertThat(execution.step().getScanFallback()).isNull();
    assertThat(execution.rids()).hasSize(ROWS - 1);
  }

  /** Regression test for issue #8333. */
  @Test
  void orderByAnotherPropertyAllowsTheFallback() {
    lineItemSetup();
    final Execution execution = run("SELECT FROM LineItem WHERE l_shipdate > '1992-01-01' ORDER BY l_seq", Map.of());
    assertThat(execution.strategy()).isEqualTo(GetValueFromIndexEntryStep.Strategy.SCAN);
    assertThat(execution.rids()).hasSize(ROWS - 1);
  }

  /** Regression test for issue #8333. */
  @Test
  void rowsReturnedAsTheyComeKeepTheIndexOrder() {
    lineItemSetup();
    for (final String query : new String[] { "SELECT FROM LineItem WHERE l_shipdate > '1992-01-01'",
        "SELECT FROM LineItem WHERE l_shipdate > '1992-01-01' LIMIT 5" }) {
      final Execution execution = run(query, Map.of());
      assertThat(execution.step().getScanFallback()).as(query).isNull();
      String previous = "";
      for (final RID rid : execution.rids()) {
        final String shipDate = rid.asDocument().getString("l_shipdate");
        assertThat(shipDate).as(query).isGreaterThanOrEqualTo(previous);
        previous = shipDate;
      }
    }
  }

  /** Regression test for issue #8333. */
  @Test
  void anAggregationAdaptsWithoutAnOrderBy() {
    lineItemSetup();
    final String bound = BASE.plusDays(ROWS / 10).toString();
    try (final ResultSet rs = database.query("sql",
        "SELECT sum(l_seq) AS s FROM LineItem WHERE l_shipdate >= '" + bound + "'")) {
      long expected = 0;
      for (final RID rid : expected(d -> d.getString("l_shipdate").compareTo(bound) >= 0))
        expected += rid.asDocument().getInteger("l_seq");
      assertThat(rs.next().<Number>getProperty("s").longValue()).isEqualTo(expected);
      assertThat(findStep(rs.getExecutionPlan().get()).getStrategy()).isEqualTo(GetValueFromIndexEntryStep.Strategy.SCAN);
    }
  }

  /** Regression test for issue #8333. */
  @Test
  void uniquePointLookupIsLeftAlone() {
    lineItemSetup();
    final Execution execution = run("SELECT FROM LineItem WHERE l_seq = 42 ORDER BY l_flag", Map.of());
    assertThat(execution.step().getScanFallback()).isNull();
    assertThat(execution.rids()).hasSize(1);
  }

  /** Regression test for issue #8333. */
  @Test
  void caseInsensitiveIndexIsLeftAlone() {
    lineItemSetup();
    // The CI index matches 'ABC' for 'abc', evaluating the condition on the record would not
    final Execution execution = run("SELECT FROM LineItem WHERE l_code.toLowerCase() = 'abc' ORDER BY l_seq", Map.of());
    assertThat(execution.step().getScanFallback()).isNull();
    assertThat(execution.rids()).hasSize(ROWS);
  }

  /** Regression test for issue #8333. */
  @Test
  void settingZeroDisablesIt() {
    lineItemSetup();
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_INDEX_MAX_SELECTIVITY, 0F);
    try {
      final Execution execution = run("SELECT FROM LineItem WHERE l_shipdate > '1992-01-01' ORDER BY l_seq", Map.of());
      assertThat(execution.step().getScanFallback()).isNull();
      assertThat(execution.rids()).hasSize(ROWS - 1);
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.QUERY_INDEX_MAX_SELECTIVITY,
          GlobalConfiguration.QUERY_INDEX_MAX_SELECTIVITY.getDefValue());
    }
  }

  /** Regression test for issue #8333. */
  @Test
  void settingOutsideZeroToOneIsClampedOrDisabling() {
    lineItemSetup();
    final String query = "SELECT FROM LineItem WHERE l_shipdate > '1992-01-01' ORDER BY l_seq";
    try {
      // Above 1: no range can hold more than the whole type, so it never gives way to the scan
      database.getConfiguration().setValue(GlobalConfiguration.QUERY_INDEX_MAX_SELECTIVITY, 2F);
      final Execution clamped = run(query, Map.of());
      assertThat(clamped.strategy()).isEqualTo(GetValueFromIndexEntryStep.Strategy.PHYSICAL_ORDER);
      assertThat(clamped.rids()).hasSize(ROWS - 1);

      // Negative: off, like 0
      database.getConfiguration().setValue(GlobalConfiguration.QUERY_INDEX_MAX_SELECTIVITY, -1F);
      final Execution disabled = run(query, Map.of());
      assertThat(disabled.step().getScanFallback()).isNull();
      assertThat(disabled.rids()).hasSize(ROWS - 1);
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.QUERY_INDEX_MAX_SELECTIVITY,
          GlobalConfiguration.QUERY_INDEX_MAX_SELECTIVITY.getDefValue());
    }
  }

  /** Regression test for issue #8333. */
  @Test
  void orderSensitiveAggregatesKeepTheIndexOrder() {
    lineItemSetup();
    // list() keeps the rows' order in its value, a grouped LIMIT without ORDER BY picks the groups met first: both
    // would show the order, so the index keeps serving them in key order
    final String bound = BASE.plusDays(10).toString();
    for (final String query : new String[] {
        "SELECT list(l_seq) AS seqs FROM LineItem WHERE l_shipdate >= '" + bound + "'",
        "SELECT l_code, count(*) AS n FROM LineItem WHERE l_shipdate >= '" + bound + "' GROUP BY l_code LIMIT 1" })
      try (final ResultSet rs = database.query("sql", query)) {
        rs.stream().count();
        assertThat(findStep(rs.getExecutionPlan().get()).getScanFallback()).as(query).isNull();
      }
    // The same grouped query sorted by the output adapts
    try (final ResultSet rs = database.query("sql",
        "SELECT l_code, count(*) AS n FROM LineItem WHERE l_shipdate >= '" + bound + "' GROUP BY l_code ORDER BY l_code LIMIT 1")) {
      rs.stream().count();
      assertThat(findStep(rs.getExecutionPlan().get()).getStrategy()).isEqualTo(GetValueFromIndexEntryStep.Strategy.SCAN);
    }
  }

  /** Regression test for issue #8333. */
  @Test
  void aCollectionKeyValueKeepsTheIndex() {
    lineItemSetup();
    // The index seeks every element of the list; the scan would compare the value with the whole list. Equality with a list
    // matches nothing either way (#9370), so the multi-value operator is IN
    final List<String> days = new ArrayList<>();
    for (int i = 0; i < ROWS; i++)
      days.add(BASE.plusDays(i).toString());
    final Execution execution = run("SELECT FROM LineItem WHERE l_shipdate IN :days ORDER BY l_seq", Map.of("days", days));
    assertThat(execution.strategy()).isEqualTo(GetValueFromIndexEntryStep.Strategy.INDEX_ORDER);
    assertThat(execution.rids()).hasSize(ROWS);
  }

  /** Regression test for issue #8333. */
  @Test
  void profileKeepsTheMatchedCountAfterTheResultIsClosed() {
    lineItemSetup();
    final String from = BASE.plusDays(1000).toString();
    final ExecutionPlan plan;
    try (final ResultSet rs = database.query("sql",
        "SELECT FROM LineItem WHERE l_shipdate >= '" + from + "' AND l_shipdate < '" + BASE.plusDays(1100) + "' ORDER BY l_seq")) {
      rs.stream().count();
      plan = rs.getExecutionPlan().get();
    }
    assertThat(plan.prettyPrint(0, 2)).contains("physical order (100 entries matched)");
  }

  private record Execution(List<RID> rids, GetValueFromIndexEntryStep step, GetValueFromIndexEntryStep.Strategy strategy,
                           String plan) {
  }

  private Execution run(final String query, final Map<String, Object> params) {
    final List<RID> rids = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", query, params)) {
      while (rs.hasNext())
        rids.add(rs.next().getIdentity().get());
      final ExecutionPlan plan = rs.getExecutionPlan().get();
      final GetValueFromIndexEntryStep step = findStep(plan);
      assertThat(step).as("the plan of " + query + " fetches from an index:\n" + plan.prettyPrint(0, 2)).isNotNull();
      return new Execution(rids, step, step.getStrategy(), plan.prettyPrint(0, 2));
    }
  }

  private static GetValueFromIndexEntryStep findStep(final ExecutionPlan plan) {
    for (final ExecutionStep step : plan.getSteps())
      if (step instanceof GetValueFromIndexEntryStep found)
        return found;
    return null;
  }

  private List<RID> expected(final Predicate<Document> predicate) {
    final List<RID> result = new ArrayList<>();
    final Iterator<Record> it = database.iterateType("LineItem", true);
    while (it.hasNext()) {
      final Document document = it.next().asDocument();
      if (predicate.test(document))
        result.add(document.getIdentity());
    }
    return result;
  }

  private static void assertPhysicalOrder(final List<RID> rids) {
    for (int i = 1; i < rids.size(); i++) {
      final RID previous = rids.get(i - 1);
      final RID current = rids.get(i);
      assertThat(previous.getBucketId() < current.getBucketId() || (previous.getBucketId() == current.getBucketId()
          && previous.getPosition() <= current.getPosition())).as(previous + " before " + current).isTrue();
    }
  }

  // ---- #8885, #8886, #8887: index and scan equality agree for every operand the property's type can read ----
  /*
   * An equality lookup must answer the same through an index (LSM or hash) and through a scan, for every operand the property's
   * type can read.
   */

  /** Regression test for issues #8885, #8886 and #8887. */
  @Test
  void decimalScaleIsIgnoredInScan() {
    for (final String index : new String[] { "NOTUNIQUE", "NOTUNIQUE_HASH" }) {
      final String type = "D" + index;
      database.command("sql", "CREATE DOCUMENT TYPE " + type);
      database.command("sql", "CREATE PROPERTY " + type + ".a DECIMAL");
      database.command("sql", "CREATE PROPERTY " + type + ".b DECIMAL");
      database.command("sql", "CREATE INDEX ON " + type + " (a) " + index);
      database.transaction(() -> {
        database.newDocument(type).set("a", new BigDecimal("19.90"), "b", new BigDecimal("19.90")).save();
        database.newDocument(type).set("a", new BigDecimal("7"), "b", new BigDecimal("7")).save();
      });

      for (final Object v : new Object[] { new BigDecimal("19.9"), new BigDecimal("19.900"), "19.9", 19.9d }) {
        assertThat(count("SELECT FROM " + type + " WHERE a = ?", v)).as(index + " a = " + v).isEqualTo(1);
        assertThat(count("SELECT FROM " + type + " WHERE b = ?", v)).as(index + " b = " + v).isEqualTo(1);
        assertThat(count("SELECT FROM " + type + " WHERE b IN [?]", v)).as(index + " b IN " + v).isEqualTo(1);
        assertThat(count("SELECT FROM " + type + " WHERE a IN [?]", v)).as(index + " a IN " + v).isEqualTo(1);
      }
      assertThat(count("SELECT FROM " + type + " WHERE b = ?", new BigDecimal("7.00"))).isEqualTo(1);
      assertThat(count("SELECT FROM " + type + " WHERE b = ?", new BigDecimal("19.91"))).isEqualTo(0);
      assertThat(count("SELECT FROM " + type + " WHERE b = '19.9'")).isEqualTo(1);
    }
  }

  /** Regression test for issues #8885, #8886 and #8887. */
  @Test
  void instantAndZonedDateTimeMatchInScan() {
    for (final String prop : new String[] { "DATETIME", "DATETIME_MICROS", "DATETIME_NANOS", "DATETIME_SECOND" }) {
      final String type = "T" + prop;
      database.command("sql", "CREATE DOCUMENT TYPE " + type);
      database.command("sql", "CREATE PROPERTY " + type + ".a " + prop);
      database.command("sql", "CREATE PROPERTY " + type + ".b " + prop);
      database.command("sql", "CREATE INDEX ON " + type + " (a) NOTUNIQUE");
      final LocalDateTime t = switch (prop) {
        case "DATETIME" -> LocalDateTime.of(2026, 10, 1, 12, 34, 56, 789_000_000);
        case "DATETIME_MICROS" -> LocalDateTime.of(2026, 10, 1, 12, 34, 56, 789_123_000);
        case "DATETIME_NANOS" -> LocalDateTime.of(2026, 10, 1, 12, 34, 56, 789_123_456);
        default -> LocalDateTime.of(2026, 10, 1, 12, 34, 56);
      };
      database.transaction(() -> database.newDocument(type).set("a", t, "b", t).save());

      final Instant i = t.toInstant(ZoneOffset.UTC);
      final Object[] operands = { t, i, t.atZone(ZoneOffset.UTC), t.atOffset(ZoneOffset.UTC),
          t.atOffset(ZoneOffset.UTC).withOffsetSameInstant(ZoneOffset.ofHours(2)) };
      for (final Object v : operands) {
        assertThat(count("SELECT FROM " + type + " WHERE a = ?", v)).as(prop + " a = " + v).isEqualTo(1);
        assertThat(count("SELECT FROM " + type + " WHERE b = ?", v)).as(prop + " b = " + v).isEqualTo(1);
        assertThat(count("SELECT FROM " + type + " WHERE b IN [?]", v)).as(prop + " b IN " + v).isEqualTo(1);
      }
      assertThat(count("SELECT FROM " + type + " WHERE b = ?", i.plusSeconds(1))).isEqualTo(0);
      assertThat(count("SELECT FROM " + type + " WHERE b = ?", t.plusSeconds(1).atZone(ZoneOffset.UTC))).isEqualTo(0);
      assertThat(count("SELECT FROM " + type + " WHERE b = ?", OffsetDateTime.of(t, ZoneOffset.ofHours(2)))).isEqualTo(0);
    }
    assertThat(count("SELECT FROM TDATETIME WHERE b = ?", new Date(Instant.parse("2026-10-01T12:34:56.789Z").toEpochMilli())))
        .isEqualTo(1);
  }

  /** Regression test for issues #8885, #8886 and #8887. */
  @Test
  void preEpochInstantMatches() {
    database.command("sql", "CREATE DOCUMENT TYPE Old");
    database.command("sql", "CREATE PROPERTY Old.a DATETIME");
    database.command("sql", "CREATE PROPERTY Old.b DATETIME");
    database.command("sql", "CREATE INDEX ON Old (a) NOTUNIQUE");
    final LocalDateTime t = LocalDateTime.of(1960, 3, 4, 5, 6, 7, 89_000_000);
    database.transaction(() -> database.newDocument("Old").set("a", t, "b", t).save());
    final Instant i = t.toInstant(ZoneOffset.UTC);
    assertThat(count("SELECT FROM Old WHERE a = ?", i)).isEqualTo(1);
    assertThat(count("SELECT FROM Old WHERE b = ?", i)).isEqualTo(1);
    assertThat(count("SELECT FROM Old WHERE b = ?", t.atZone(ZoneOffset.UTC))).isEqualTo(1);
  }

  /** Regression test for issues #8885, #8886 and #8887. */
  @Test
  void booleanIndexLookupByStringOrNumber() {
    for (final String index : new String[] { "NOTUNIQUE", "NOTUNIQUE_HASH" }) {
      final String type = "B" + index;
      database.command("sql", "CREATE DOCUMENT TYPE " + type);
      database.command("sql", "CREATE PROPERTY " + type + ".a BOOLEAN");
      database.command("sql", "CREATE PROPERTY " + type + ".b BOOLEAN");
      database.command("sql", "CREATE INDEX ON " + type + " (a) " + index);
      database.transaction(() -> {
        database.newDocument(type).set("a", true, "b", true).save();
        database.newDocument(type).set("a", false, "b", false).save();
      });

      for (final Object v : new Object[] { true, "true", "TRUE", 1 }) {
        assertThat(count("SELECT FROM " + type + " WHERE b = ?", v)).as(index + " b = " + v).isEqualTo(1);
        assertThat(count("SELECT FROM " + type + " WHERE a = ?", v)).as(index + " a = " + v).isEqualTo(1);
        assertThat(count("SELECT FROM " + type + " WHERE a IN [?]", v)).as(index + " a IN " + v).isEqualTo(1);
      }
      assertThat(count("SELECT FROM " + type + " WHERE a = 'true'")).isEqualTo(1);
      assertThat(count("SELECT FROM " + type + " WHERE a = ?", "false")).isEqualTo(1);
      assertThat(count("SELECT FROM " + type + " WHERE a = ?", 0)).isEqualTo(1);
      // a key that does not read as a boolean matches nothing, as the scan does
      assertThat(count("SELECT FROM " + type + " WHERE a = ?", "maybe")).isEqualTo(0);
      assertThat(count("SELECT FROM " + type + " WHERE b = ?", "maybe")).isEqualTo(0);
      assertThat(count("SELECT FROM " + type + " WHERE a IN [?]", "maybe")).isEqualTo(0);

      // Cypher answers the same with and without the index (no row for a non-boolean operand, no exception)
      final long cypherIndexed = cypherCount("MATCH (n:" + type + ") WHERE n.a = $v RETURN n", "true");
      final long cypherScan = cypherCount("MATCH (n:" + type + ") WHERE n.b = $v RETURN n", "true");
      assertThat(cypherIndexed).isEqualTo(cypherScan);
      assertThat(cypherCount("MATCH (n:" + type + ") WHERE n.a = $v RETURN n", 1))
          .isEqualTo(cypherCount("MATCH (n:" + type + ") WHERE n.b = $v RETURN n", 1));
      assertThat(cypherCount("MATCH (n:" + type + ") WHERE n.a = $v RETURN n", "maybe")).isZero();
      assertThat(cypherCount("MATCH (n:" + type + ") WHERE n.b = $v RETURN n", "maybe")).isZero();
    }
  }

  /** Regression test for issues #8885, #8886 and #8887. */
  @Test
  void binaryComparatorDecimalScale() {
    final BigDecimal a = new BigDecimal("19.9");
    final BigDecimal b = new BigDecimal("19.90");
    assertThat(BinaryComparator.equals(a, b)).isTrue();
    assertThat(BinaryComparator.equals(a, new BigDecimal("19.91"))).isFalse();
    // the exact twin stays scale-sensitive: hashed key structures pick their slot with BigDecimal.hashCode()
    assertThat(BinaryComparator.equalsExact(a, b)).isFalse();
    assertThat(BinaryComparator.equalsExact(a, new BigDecimal("19.9"))).isTrue();
  }

  private long count(final String sql, final Object... params) {
    try (final ResultSet rs = database.query("sql", sql, params)) {
      return rs.stream().count();
    }
  }

  private long cypherCount(final String query, final Object value) {
    try (final ResultSet rs = database.query("opencypher", query, Map.of("v", value))) {
      return rs.stream().count();
    }
  }

  // ---- #8970: a lossy parameter bound (follow-up of #8919 / #8872) ----
  /*
   * A bound passed as a parameter that the key type of an index cannot hold (an integer past 2^53 on a DOUBLE, past 2^24 on a
   * FLOAT, a BigDecimal on a DOUBLE) was converted to the key type by the index, which answered for the rounded bound. A literal
   * bound is kept away from the index at planning, but a plan with a parameter is cached and the decision depends on the value.
   * The indexed type must answer as the unindexed one does, for every operator and for a cached plan run with different values.
   */

  private static final long TWO_53 = 1L << 53;

  private void lossyBoundSetup() {
    database.transaction(() -> {
      for (final String type : new String[] { "I", "N" }) {
        database.command("sql", "CREATE VERTEX TYPE " + type);
        database.command("sql", "CREATE PROPERTY " + type + ".id INTEGER");
        database.command("sql", "CREATE PROPERTY " + type + ".d DOUBLE");
        database.command("sql", "CREATE PROPERTY " + type + ".f FLOAT");
      }
      database.command("sql", "CREATE INDEX ON I (d) NOTUNIQUE");
      database.command("sql", "CREATE INDEX ON I (f) NOTUNIQUE");
      database.command("sql", "CREATE INDEX ON I (d, id) NOTUNIQUE");
      for (final String type : new String[] { "C", "CN" }) {
        database.command("sql", "CREATE VERTEX TYPE " + type);
        database.command("sql", "CREATE PROPERTY " + type + ".id INTEGER");
        database.command("sql", "CREATE PROPERTY " + type + ".d DOUBLE");
      }
      database.command("sql", "CREATE INDEX ON C (id, d) NOTUNIQUE");
      for (final String type : new String[] { "C", "CN" }) {
        database.command("sql", "INSERT INTO " + type + " SET id = 1, d = 9007199254740992.0");
        database.command("sql", "INSERT INTO " + type + " SET id = 1, d = 9007199254740994.0");
        database.command("sql", "INSERT INTO " + type + " SET id = 1, d = 0.1");
        database.command("sql", "INSERT INTO " + type + " SET id = 2, d = 9007199254740994.0");
      }
      for (final String type : new String[] { "H", "HN" }) {
        database.command("sql", "CREATE VERTEX TYPE " + type);
        database.command("sql", "CREATE PROPERTY " + type + ".id INTEGER");
        database.command("sql", "CREATE PROPERTY " + type + ".d DOUBLE");
      }
      database.command("sql", "CREATE INDEX ON H (d) NOTUNIQUE_HASH");
      for (final String type : new String[] { "H", "HN" }) {
        database.command("sql", "INSERT INTO " + type + " SET id = 1, d = 9007199254740992.0");
        database.command("sql", "INSERT INTO " + type + " SET id = 2, d = 0.1");
      }
      for (final String type : new String[] { "I", "N" }) {
        database.command("sql", "INSERT INTO " + type + " SET id = 1, d = 5.0, f = 5.0");
        database.command("sql", "INSERT INTO " + type + " SET id = 2, d = 9007199254740992.0, f = 16777216.0");
        database.command("sql", "INSERT INTO " + type + " SET id = 3, d = 9007199254740994.0, f = 16777218.0");
        database.command("sql", "INSERT INTO " + type + " SET id = 4, d = 9007199254740996.0, f = 16777220.0");
        database.command("sql", "INSERT INTO " + type + " SET id = 5, d = 0.1, f = 0.1");
        database.command("sql", "INSERT INTO " + type + " SET id = 6, d = 0.2, f = 0.2");
      }
    });
  }

  /** Regression test for issue #8970. */
  @Test
  void anIntegerParameterPastTwoToThe53OnADoubleAnswersAsTheScanDoes() {
    lossyBoundSetup();
    // 2^53 + 1 is not a double: equality matches nothing, and a value is never equal to a bound and below it at once
    assertThat(sql("I", "d = ?", TWO_53 + 1)).isEmpty();
    assertThat(sql("I", "d < ?", TWO_53 + 1)).containsExactly(1, 2, 5, 6);
    assertThat(sql("I", "d <= ?", TWO_53 + 1)).containsExactly(1, 2, 5, 6);
    assertThat(sql("I", "d >= ?", TWO_53 + 1)).containsExactly(3, 4);
    assertThat(sql("I", "d > ?", TWO_53 + 1)).containsExactly(3, 4);
  }

  /** Regression test for issue #8970. */
  @Test
  void everyOperatorAgreesWithTheUnindexedTypeForUnrepresentableBounds() {
    lossyBoundSetup();
    final Object[] bounds = { TWO_53 + 1, TWO_53 + 3, TWO_53 + 2, TWO_53 - 1, -(TWO_53 + 1), Long.MAX_VALUE, Long.MIN_VALUE,
        BigInteger.valueOf(TWO_53).add(BigInteger.valueOf(5)), new BigDecimal("9007199254740993"), new BigDecimal("0.1"),
        new BigDecimal("0.2"), new BigDecimal("0.10000000000000001"), new BigDecimal("0.100000000000000006"),
        new BigDecimal("0.15"), new BigDecimal(0.1d), new BigDecimal(0.2d) };
    for (final Object bound : bounds)
      for (final String operator : new String[] { "=", "<", "<=", ">", ">=", "<>" })
        assertSameAsScan("d " + operator + " ?", bound);
  }

  /** Regression test for issue #8970. */
  @Test
  void aDescendingOrderOverAnIndexAgreesWithTheUnindexedType() {
    lossyBoundSetup();
    for (final Object bound : new Object[] { TWO_53 + 1, TWO_53 + 3, new BigDecimal("0.1") })
      for (final String operator : new String[] { "<", "<=", ">", ">=" }) {
        final String statement = "SELECT id FROM %s WHERE d " + operator + " ? ORDER BY d DESC";
        assertThat(ids(String.format(statement, "I"), bound)).as("d %s %s DESC", operator, bound)
            .isEqualTo(ids(String.format(statement, "N"), bound));
      }
  }

  private List<Integer> ids(final String statement, final Object... parameters) {
    final List<Integer> ids = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", statement, parameters)) {
      while (rs.hasNext())
        ids.add(rs.next().<Integer>getProperty("id"));
    }
    return ids;
  }

  /** Regression test for issue #8970. */
  @Test
  void aLossyBoundOnTheSecondPropertyOfACompositeIndexAgreesWithTheUnindexedType() {
    lossyBoundSetup();
    final Object[] bounds = { TWO_53 + 1, TWO_53 + 3, new BigDecimal("0.1") };
    for (final Object bound : bounds)
      for (final String operator : new String[] { "=", "<", "<=", ">", ">=" }) {
        final String where = "id = ? AND d " + operator + " ?";
        assertThat(sql("C", where, 1, bound)).as("C %s with %s", where, bound).isEqualTo(sql("CN", where, 1, bound));
      }
    assertThat(sql("C", "id = ? AND d = ?", 1, TWO_53 + 1)).isEmpty();
    assertThat(sql("C", "id = ? AND d BETWEEN ? AND ?", 1, TWO_53 + 1, TWO_53 + 3))
        .isEqualTo(sql("CN", "id = ? AND d BETWEEN ? AND ?", 1, TWO_53 + 1, TWO_53 + 3));
  }

  /** Regression test for issue #8970. */
  @Test
  void aBoundOnAHashIndexAgreesWithTheUnindexedType() {
    lossyBoundSetup();
    // a hash index cannot be widened: the rounded key is read and the entries are checked against the exact bound
    for (final Object bound : new Object[] { TWO_53 + 1, TWO_53, new BigDecimal("0.1"), new BigDecimal("0.100000000000000006") }) {
      assertThat(sql("H", "d = ?", bound)).as("hash d = %s", bound).isEqualTo(sql("HN", "d = ?", bound));
      assertThat(sql("H", "d IN [?, ?]", bound, 5L)).as("hash d IN %s", bound).isEqualTo(sql("HN", "d IN [?, ?]", bound, 5L));
    }
    assertThat(sql("H", "d = ?", TWO_53 + 1)).isEmpty();
    assertThat(sql("H", "d = ?", TWO_53)).containsExactly(1);
  }

  /** Regression test for issue #8970. */
  @Test
  void aCompositeIndexWithAnExtraNonIndexedPredicateAgreesWithTheUnindexedType() {
    lossyBoundSetup();
    assertSameAsScan("d = ? AND id > ? AND f > ?", TWO_53 + 1, 0, 1.0d);
    assertSameAsScan("d >= ? AND f < ?", TWO_53 + 1, 16777220.0d);
    assertSameAsScan("d = ? AND f = ?", new BigDecimal("0.1"), 0.1d);
  }

  /** Regression test for issue #8970. */
  @Test
  void betweenAndInAgreeWithTheUnindexedType() {
    lossyBoundSetup();
    assertSameAsScan("d BETWEEN ? AND ?", TWO_53 + 1, TWO_53 + 5);
    assertSameAsScan("d BETWEEN ? AND ?", TWO_53 + 3, TWO_53 + 3);
    assertSameAsScan("d BETWEEN ? AND ?", new BigDecimal("0.1"), new BigDecimal("0.15"));
    assertSameAsScan("d IN [?, ?]", TWO_53 + 1, TWO_53 + 2);
    assertSameAsScan("d IN [?, ?, ?]", TWO_53 + 1, TWO_53 + 3, 5L);
    assertSameAsScan("d IN ?", List.of(TWO_53 + 1, TWO_53 + 3, new BigDecimal("0.1")));
    assertThat(sql("I", "d IN ?", List.of(TWO_53 + 1, 5L))).containsExactly(1);
  }

  /** Regression test for issue #8970. */
  @Test
  void aBoundOnADoubleCombinedWithAnotherConditionAgreesWithTheUnindexedType() {
    lossyBoundSetup();
    assertSameAsScan("d = ? AND id = ?", TWO_53 + 1, 2);
    assertSameAsScan("d >= ? AND id > ?", TWO_53 + 1, 3);
    assertSameAsScan("d > ? AND d < ?", TWO_53 + 1, TWO_53 + 5);
    assertSameAsScan("d > ? AND d <= ?", new BigDecimal("0.1"), new BigDecimal("0.2"));
  }

  /** Regression test for issue #8970. */
  @Test
  void anIntegerParameterPastTwoToThe24OnAFloatAnswersAsTheScanDoes() {
    lossyBoundSetup();
    for (final long bound : new long[] { 16777217L, 16777219L, 16777218L, -16777217L })
      for (final String operator : new String[] { "=", "<", "<=", ">", ">=", "<>" })
        assertSameAsScan("f " + operator + " ?", bound);
    assertThat(sql("I", "f = ?", 16777217L)).isEmpty();
  }

  /** Regression test for issue #8970. */
  @Test
  void theSamePlanRunWithRepresentableAndUnrepresentableValuesAnswersEachOneRight() {
    lossyBoundSetup();
    // the plan of a parametrised statement is cached by its text: the first value must not decide the next ones
    final long[] sequence = { 5, TWO_53 + 1, TWO_53, TWO_53 + 3, 5, TWO_53 + 2, TWO_53 + 1 };
    for (final long bound : sequence) {
      assertSameAsScan("d = ?", bound);
      assertSameAsScan("d < ?", bound);
      assertSameAsScan("d >= ?", bound);
    }
    assertThat(sql("I", "d = ?", 5L)).containsExactly(1);
    assertThat(sql("I", "d = ?", TWO_53 + 2)).containsExactly(3);
  }

  /** Regression test for issue #8970. */
  @Test
  void aNamedParameterAnswersAsAPositionalOneDoes() {
    lossyBoundSetup();
    final List<Integer> ids = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT id FROM I WHERE d = :bound", Map.of("bound", TWO_53 + 1))) {
      while (rs.hasNext())
        ids.add(rs.next().<Integer>getProperty("id"));
    }
    assertThat(ids).isEmpty();
  }

  /** Regression test for issue #8970. */
  @Test
  void cypherParametersAgreeWithTheUnindexedType() {
    lossyBoundSetup();
    final Object[] bounds = { TWO_53 + 1, TWO_53 + 3, TWO_53 + 2, new BigDecimal("0.1"), new BigDecimal("9007199254740993") };
    for (final Object bound : bounds)
      for (final String operator : new String[] { "=", "<", "<=", ">", ">=", "<>" })
        assertThat(cypher("I", "d " + operator + " $b", bound)).as("cypher d %s %s", operator, bound)
            .isEqualTo(cypher("N", "d " + operator + " $b", bound));
    assertThat(cypher("I", "d = $b", TWO_53 + 1)).isEmpty();
    assertThat(cypher("I", "f = $b", 16777217L)).isEmpty();
  }

  private List<Integer> cypher(final String type, final String where, final Object bound) {
    final List<Integer> ids = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:" + type + ") WHERE n." + where + " RETURN n.id AS id ORDER BY id",
        Map.of("b", bound))) {
      while (rs.hasNext())
        ids.add(rs.next().<Number>getProperty("id").intValue());
    }
    return ids;
  }

  private void assertSameAsScan(final String where, final Object... parameters) {
    final List<Integer> scan = sql("N", where, parameters);
    assertThat(sql("I", where, parameters)).as("WHERE %s with %s", where, List.of(parameters)).isEqualTo(scan);
  }

  /** Ids of the rows of {@code type} matching {@code where}, ascending (shared by the #8970 and #9029 tests). */
  private List<Integer> sql(final String type, final String where, final Object... parameters) {
    final List<Integer> ids = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT id FROM " + type + " WHERE " + where + " ORDER BY id", parameters)) {
      while (rs.hasNext())
        ids.add(rs.next().<Integer>getProperty("id"));
    }
    return ids;
  }

  // ---- #9029: the partner bound of an index range must be computable before the scan ----
  /*
   * The other end of an index range was taken from the second range condition on the indexed property without checking that
   * its bound can be computed before the scan. A bound that reads another property of the record (u <= id) or an expression over
   * one (u <= w * 100) was then evaluated by the index scan with no record, so the indexed type answered differently from an
   * identical unindexed one. Such a condition must stay in the filter.
   */

  private void partnerBoundSetup() {
    for (final String type : new String[] { "I", "S" }) {
      database.command("sql", "CREATE DOCUMENT TYPE " + type);
      database.command("sql", "CREATE PROPERTY " + type + ".id INTEGER");
      database.command("sql", "CREATE PROPERTY " + type + ".u INTEGER");
      database.command("sql", "CREATE PROPERTY " + type + ".w INTEGER");
    }
    database.command("sql", "CREATE INDEX ON I (u) NOTUNIQUE");
    database.transaction(() -> {
      for (final String type : new String[] { "I", "S" }) {
        database.newDocument(type).set("id", 32, "u", 26, "w", 1).save();
        database.newDocument(type).set("id", 3, "u", 5, "w", 2).save();
        database.newDocument(type).set("id", 40, "u", 50, "w", 3).save();
      }
    });
  }

  /** Regression test for issue #9029. */
  @Test
  void upperBoundReadingAnotherPropertyStaysInTheFilter() {
    partnerBoundSetup();
    assertThat(sql("I", "u > 9 AND u <= id")).containsExactly(32);
    assertThat(sql("S", "u > 9 AND u <= id")).containsExactly(32);
    assertThat(sql("I", "u <= id AND u > 9")).containsExactly(32);
  }

  /** Regression test for issue #9029. */
  @Test
  void lowerBoundReadingAnotherPropertyStaysInTheFilter() {
    partnerBoundSetup();
    assertThat(sql("I", "u < 60 AND u > id")).containsExactly(3, 40);
    assertThat(sql("S", "u < 60 AND u > id")).containsExactly(3, 40);
    assertThat(sql("I", "u > id AND u < 60")).containsExactly(3, 40);
  }

  /** Regression test for issue #9029. */
  @Test
  void boundExpressionOverAnotherPropertyStaysInTheFilter() {
    partnerBoundSetup();
    assertThat(sql("I", "u > 9 AND u <= w * 100")).containsExactly(32, 40);
    assertThat(sql("S", "u > 9 AND u <= w * 100")).containsExactly(32, 40);
  }

  /** Regression test for issue #9029. */
  @Test
  void aLaterConstantBoundIsStillTakenAsTheOtherEnd() {
    partnerBoundSetup();
    // the record-dependent condition is skipped, the constant one after it closes the range, and the skipped one filters
    assertThat(sql("I", "u > 9 AND u <= id AND u < 45")).containsExactly(32);
    assertThat(sql("S", "u > 9 AND u <= id AND u < 45")).containsExactly(32);
    final String plan = plan("SELECT id FROM I WHERE u > 9 AND u <= id AND u < 45");
    assertThat(plan).contains("FETCH FROM INDEX I[u]").contains("u > 9 and u < 45");
  }

  /** Regression test for issue #9029. */
  @Test
  void parameterAndConstantBoundsStillCloseTheRange() {
    partnerBoundSetup();
    assertThat(sql("I", "u > 9 AND u <= 30")).containsExactly(32);
    assertThat(plan("SELECT id FROM I WHERE u > 9 AND u <= 30")).contains("u > 9 and u <= 30");
    final List<Integer> ids = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT id FROM I WHERE u > ? AND u <= ? ORDER BY id", 9, 60)) {
      rs.forEachRemaining(r -> ids.add(r.getProperty("id")));
    }
    assertThat(ids).containsExactly(32, 40);
  }

  /** Regression test for issue #9029. */
  @Test
  void planOfARecordDependentPartnerKeepsTheConditionAsAFilter() {
    partnerBoundSetup();
    final String plan = plan("SELECT id FROM I WHERE u > 9 AND u <= id");
    assertThat(plan).contains("FETCH FROM INDEX I[u]");
    assertThat(plan).doesNotContain("u > 9 and u <= id");
    // the condition is moved to the filter, not dropped
    assertThat(plan.substring(plan.indexOf("FILTER ITEMS WHERE"))).contains("u <= id");
  }

  /** Regression test for issue #9029. */
  @Test
  void recordDependentPartnerOnACaseInsensitiveIndexStaysInTheFilter() {
    partnerBoundSetup();
    // the field.toLowerCase() range of #8560 on a COLLATE ci index: its partner must be early calculated too
    for (final String type : new String[] { "CI", "CS" }) {
      database.command("sql", "CREATE DOCUMENT TYPE " + type);
      database.command("sql", "CREATE PROPERTY " + type + ".name STRING");
      database.command("sql", "CREATE PROPERTY " + type + ".upTo STRING");
    }
    database.command("sql", "CREATE INDEX ON CI (name COLLATE ci) NOTUNIQUE");
    database.transaction(() -> {
      for (final String type : new String[] { "CI", "CS" }) {
        database.newDocument(type).set("name", "Anne", "upTo", "b").save();
        database.newDocument(type).set("name", "John", "upTo", "c").save();
        database.newDocument(type).set("name", "mary", "upTo", "z").save();
      }
    });

    for (final String where : new String[] { "name.toLowerCase() >= 'a' AND name.toLowerCase() <= upTo",
        "name.toLowerCase() <= upTo AND name.toLowerCase() >= 'a'",
        "name.toLowerCase() >= 'a' AND name.toLowerCase() <= upTo AND name.toLowerCase() < 'n'" }) {
      final List<String> indexed = names("SELECT name FROM CI WHERE " + where + " ORDER BY name");
      assertThat(indexed).as(where).isEqualTo(names("SELECT name FROM CS WHERE " + where + " ORDER BY name"));
      assertThat(indexed).as(where).containsExactly("Anne", "mary");
    }
    assertThat(plan("SELECT name FROM CI WHERE name.toLowerCase() >= 'a' AND name.toLowerCase() <= upTo"))
        .contains("FETCH FROM INDEX CI[name]");
  }

  private List<String> names(final String query) {
    final List<String> names = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", query)) {
      rs.forEachRemaining(r -> names.add(r.getProperty("name")));
    }
    return names;
  }

  /** Regression test for issue #9029. */
  @Test
  void cypherAgreesBetweenIndexedAndUnindexedTypes() {
    partnerBoundSetup();
    for (final String where : new String[] { "n.u > 9 AND n.u <= n.id", "n.u < 60 AND n.u > n.id", "n.u > 9 AND n.u <= n.w * 100" })
      assertThat(cypher("I", where)).as(where).isEqualTo(cypher("S", where));
  }

  private List<Integer> cypher(final String type, final String where) {
    final List<Integer> ids = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:" + type + ") WHERE " + where + " RETURN n.id AS id ORDER BY id",
        Map.of())) {
      rs.forEachRemaining(r -> ids.add(((Number) r.getProperty("id")).intValue()));
    }
    return ids;
  }

  private String plan(final String query) {
    try (final ResultSet rs = database.query("sql", "EXPLAIN " + query)) {
      return rs.next().getProperty("executionPlanAsString");
    }
  }

  // ---- #9125: DELETE FROM INDEX with an inexact bound on an integral key ----
  /*
   * DELETE FROM INDEX:<name> WHERE key ... on an integral key (BYTE, SHORT, INTEGER, LONG) passed a fractional or out-of-range
   * bound to the index, which truncates or clamps it, so key BETWEEN 11.5 AND 12.5 also removed the entries of key 11. The delete
   * must remove exactly the keys a scan with the same condition matches.
   */

  private static final String[] KEY_TYPES = { "BYTE", "SHORT", "INTEGER", "LONG" };

  /** Keys 9..14 in a fresh indexed type, deletes through the index and returns the keys left, ascending. */
  private List<Integer> deleteAndGetRemaining(final String keyType, final String condition, final Object... params) {
    final String type = "T" + keyType + Math.abs(condition.hashCode());
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE " + type);
      database.command("sql", "CREATE PROPERTY " + type + ".k " + keyType);
      database.command("sql", "CREATE INDEX ON " + type + " (k) NOTUNIQUE");
      for (int k = 9; k <= 14; k++)
        database.command("sql", "INSERT INTO " + type + " SET k = " + k);
    });
    final Index index = database.getSchema().getType(type).getAllIndexes(false).iterator().next().getIndexesOnBuckets()[0];
    database.transaction(() -> {
      // the entries are removed as the result set is pulled
      try (final ResultSet resultSet = database.command("sql", "DELETE FROM INDEX:`" + index.getName() + "` WHERE " + condition, params)) {
        while (resultSet.hasNext())
          resultSet.next();
      }
    });

    final List<Integer> remaining = new ArrayList<>();
    // the entries of a record whose index entry is gone are not found through the index, so ask the index itself
    final var cursor = index.get(new Object[] { 0 });
    cursor.close();
    for (int k = 9; k <= 14; k++) {
      final var c = index.get(new Object[] { k });
      if (c.hasNext())
        remaining.add(k);
      c.close();
    }
    // DELETE FROM INDEX took entries out of the index alone: put them back so the integrity check judges the database
    rebuildIndexesFromRecords();
    return remaining;
  }

  /** Regression test for issue #9125. */
  @Test
  void aNonNumericBoundNeverSurfacesAsAClassCastException() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE TNonNum9125");
      database.command("sql", "CREATE PROPERTY TNonNum9125.k INTEGER");
      database.command("sql", "CREATE INDEX ON TNonNum9125 (k) NOTUNIQUE");
      database.command("sql", "INSERT INTO TNonNum9125 SET k = 1");
    });
    final Index index = database.getSchema().getType("TNonNum9125").getAllIndexes(false).iterator().next().getIndexesOnBuckets()[0];
    try {
      database.transaction(() -> {
        try (final ResultSet resultSet = database.command("sql", "DELETE FROM INDEX:`" + index.getName() + "` WHERE key BETWEEN 11.5 AND 'x'")) {
          while (resultSet.hasNext())
            resultSet.next();
        }
      });
    } catch (final RuntimeException e) {
      assertThat(e).isNotInstanceOf(ClassCastException.class);
    }
  }

  /** Regression test for issue #9125. */
  @Test
  void equalityWithAnExactBoundRemovesTheKey() {
    for (final String keyType : KEY_TYPES)
      assertThat(deleteAndGetRemaining(keyType, "key = 12")).as(keyType).containsExactly(9, 10, 11, 13, 14);
  }

  /** Regression test for issue #9125. */
  @Test
  void exactBoundsRemoveExactlyTheKeysTheOperatorSelects() {
    for (final String keyType : KEY_TYPES) {
      assertThat(deleteAndGetRemaining(keyType, "key > 12")).as(keyType + " >").containsExactly(9, 10, 11, 12);
      assertThat(deleteAndGetRemaining(keyType, "key >= 12")).as(keyType + " >=").containsExactly(9, 10, 11);
      assertThat(deleteAndGetRemaining(keyType, "key < 12")).as(keyType + " <").containsExactly(12, 13, 14);
      assertThat(deleteAndGetRemaining(keyType, "key <= 12")).as(keyType + " <=").containsExactly(13, 14);
      assertThat(deleteAndGetRemaining(keyType, "key BETWEEN 10 AND 12")).as(keyType + " BETWEEN").containsExactly(9, 13, 14);
    }
  }

  /** Regression test for issue #9125. */
  @Test
  void lowerThanWithAFractionalBoundRemovesOnlyTheKeysBelow() {
    for (final String keyType : KEY_TYPES) {
      assertThat(deleteAndGetRemaining(keyType, "key < 11.5")).as(keyType + " <").containsExactly(12, 13, 14);
      assertThat(deleteAndGetRemaining(keyType, "key <= 11.5")).as(keyType + " <=").containsExactly(12, 13, 14);
    }
  }

  /** Regression test for issue #9125. */
  @Test
  void betweenWithFractionalBoundsRemovesOnlyTheKeysInside() {
    for (final String keyType : KEY_TYPES)
      assertThat(deleteAndGetRemaining(keyType, "key BETWEEN 11.5 AND 12.5")).as(keyType).containsExactly(9, 10, 11, 13, 14);
  }

  /** Regression test for issue #9125. */
  @Test
  void betweenWithAFractionalBoundParameterRemovesOnlyTheKeysInside() {
    for (final String keyType : KEY_TYPES)
      assertThat(deleteAndGetRemaining(keyType, "key BETWEEN ? AND ?", 10.5d, 13.5d)).as(keyType).containsExactly(9, 10, 14);
  }

  /** Regression test for issue #9125. */
  @Test
  void betweenWithNoKeyInsideRemovesNothing() {
    for (final String keyType : KEY_TYPES)
      assertThat(deleteAndGetRemaining(keyType, "key BETWEEN 11.2 AND 11.8")).as(keyType).containsExactly(9, 10, 11, 12, 13, 14);
  }

  /** Regression test for issue #9125. */
  @Test
  void betweenWithBoundsPastTheKeyRangeRemovesEveryKey() {
    for (final String keyType : new String[] { "LONG" })
      assertThat(deleteAndGetRemaining(keyType, "key BETWEEN -1e30 AND 1e30")).as(keyType).isEmpty();
  }

  /** Regression test for issue #9125. */
  @Test
  void equalityWithAFractionalBoundRemovesNothing() {
    for (final String keyType : KEY_TYPES)
      assertThat(deleteAndGetRemaining(keyType, "key = 12.5")).as(keyType).containsExactly(9, 10, 11, 12, 13, 14);
  }

  /** Regression test for issue #9125. */
  @Test
  void greaterThanWithAFractionalBoundRemovesOnlyTheKeysAbove() {
    for (final String keyType : KEY_TYPES) {
      assertThat(deleteAndGetRemaining(keyType, "key > 11.5")).as(keyType + " >").containsExactly(9, 10, 11);
      assertThat(deleteAndGetRemaining(keyType, "key >= 12.5")).as(keyType + " >=").containsExactly(9, 10, 11, 12);
    }
  }

  /** Regression test for issue #9125. */
  @Test
  void fractionalBoundsAtTheEdgesOfTheKeyRangeAreHandled() {
    // Integer.MAX_VALUE + 0.5 is above every INTEGER key, -Integer.MAX_VALUE - 1.5 below every one
    assertThat(deleteAndGetRemaining("INTEGER", "key > 2147483647.5")).as("> above the range").containsExactly(9, 10, 11, 12, 13, 14);
    assertThat(deleteAndGetRemaining("INTEGER", "key < -2147483648.5")).as("< below the range").containsExactly(9, 10, 11, 12, 13, 14);
    assertThat(deleteAndGetRemaining("INTEGER", "key >= -2147483648.5")).as(">= below the range").isEmpty();
    assertThat(deleteAndGetRemaining("INTEGER", "key <= 2147483647.5")).as("<= above the range").isEmpty();
  }

  // ---- #10757: CONTAINSALL with an array operand over a BY ITEM index ----
  /*
   * Over a BY ITEM index, tags CONTAINSALL :p bound to a multi-element ARRAY answered like CONTAINSANY. The planner declined the
   * index for a multi-element Collection only, so an int[] reached the index, whose lookup is the union of the items, and
   * nothing re-checked the condition. The decision also read the parameter at planning time without keeping the plan out of the
   * cache, so a plan made for [1] answered [1, 2] with the union too.
   *
   * The contract pinned here: the same statement, over the same rows, answers exactly the same with the index present and
   * absent, for every shape the operand can be bound in, in any order of execution.
   */

  private void createContainsAllTypes() {
    database.command("sql", "CREATE DOCUMENT TYPE Tagged").close();
    database.command("sql", "CREATE PROPERTY Tagged.name STRING").close();
    database.command("sql", "CREATE PROPERTY Tagged.tags LIST OF INTEGER").close();
    database.command("sql", "CREATE INDEX ON Tagged (tags BY ITEM) NOTUNIQUE").close();

    database.command("sql", "CREATE DOCUMENT TYPE Plain").close();
    database.command("sql", "CREATE PROPERTY Plain.name STRING").close();
    database.command("sql", "CREATE PROPERTY Plain.tags LIST OF INTEGER").close();

    database.transaction(() -> {
      for (final String type : List.of("Tagged", "Plain")) {
        database.newDocument(type).set("name", "only1").set("tags", new ArrayList<>(List.of(1))).save();
        database.newDocument(type).set("name", "one_two").set("tags", new ArrayList<>(List.of(1, 2))).save();
        database.newDocument(type).set("name", "only2").set("tags", new ArrayList<>(List.of(2))).save();
        database.newDocument(type).set("name", "two_three_one").set("tags", new ArrayList<>(List.of(2, 3, 1))).save();
        database.newDocument(type).set("name", "empty").set("tags", new ArrayList<>()).save();
        database.newDocument(type).set("name", "missing").save();
      }
    });
  }

  private Set<String> containsAllNames(final String type, final Object operand) {
    final Set<String> names = new TreeSet<>();
    try (final ResultSet rs = database.query("sql", "SELECT name FROM " + type + " WHERE tags CONTAINSALL :p", Map.of("p", operand))) {
      rs.stream().forEach(r -> names.add(r.getProperty("name")));
    }
    return names;
  }

  private String containsAllPlan(final Object operand) {
    try (final ResultSet rs = database.query("sql", "EXPLAIN SELECT name FROM Tagged WHERE tags CONTAINSALL :p", Map.of("p", operand))) {
      return rs.next().getProperty("executionPlanAsString");
    }
  }

  /** Every shape a client can bind the operand in. */
  private static Map<String, Object> shapesOf(final int... items) {
    final Map<String, Object> shapes = new LinkedHashMap<>();
    final List<Integer> list = new ArrayList<>();
    for (final int item : items)
      list.add(item);
    final Integer[] boxed = list.toArray(new Integer[0]);
    final long[] longs = new long[items.length];
    for (int i = 0; i < items.length; i++)
      longs[i] = items[i];
    shapes.put("int[]", items.clone());
    shapes.put("Integer[]", boxed);
    shapes.put("long[]", longs);
    shapes.put("Object[]", list.toArray());
    shapes.put("List", list);
    shapes.put("LinkedHashSet", new LinkedHashSet<>(list));
    return shapes;
  }

  /** Regression test for issue #10757. */
  @Test
  void anArrayOperandAnswersAsTheListDoesAndAsTheUnindexedTypeDoes() {
    createContainsAllTypes();
    // the array binding runs first, on a fresh database, as in the report
    assertThat(containsAllNames("Tagged", new int[] { 1, 2 })).containsExactly("one_two", "two_three_one");
    assertThat(containsAllNames("Tagged", new Integer[] { 1, 2 })).containsExactly("one_two", "two_three_one");
    assertThat(containsAllNames("Plain", new int[] { 1, 2 })).containsExactly("one_two", "two_three_one");
    assertThat(containsAllNames("Tagged", List.of(1, 2))).containsExactly("one_two", "two_three_one");
  }

  /** Regression test for issue #10757. */
  @Test
  void theIndexedAndTheUnindexedTypeAgreeForEveryShapeAndEverySize() {
    createContainsAllTypes();
    final List<int[]> operands = List.of(new int[] {}, new int[] { 1 }, new int[] { 2 }, new int[] { 1, 2 }, new int[] { 2, 1 },
        new int[] { 1, 2, 3 }, new int[] { 1, 1 }, new int[] { 4 }, new int[] { 1, 4 });
    for (final int[] items : operands)
      for (final Map.Entry<String, Object> shape : shapesOf(items).entrySet())
        assertThat(containsAllNames("Tagged", shape.getValue())).as("%s %s", shape.getKey(), Arrays.toString(items))
            .isEqualTo(containsAllNames("Plain", shape.getValue()));
  }

  /** Regression test for issue #10757. */
  @Test
  void aPlanMadeForOneItemIsNotReusedForTwo() {
    createContainsAllTypes();
    // one statement text, executed first with one item (an index-eligible operand) and then with two
    assertThat(containsAllNames("Tagged", List.of(1))).containsExactly("one_two", "only1", "two_three_one");
    assertThat(containsAllNames("Tagged", List.of(1, 2))).containsExactly("one_two", "two_three_one");
    assertThat(containsAllNames("Tagged", new int[] { 1 })).containsExactly("one_two", "only1", "two_three_one");
    assertThat(containsAllNames("Tagged", new int[] { 1, 2 })).containsExactly("one_two", "two_three_one");
    // and an empty operand, which every collection satisfies, is never answered by the index, whatever ran before it
    assertThat(containsAllNames("Tagged", List.of())).containsExactly("empty", "one_two", "only1", "only2", "two_three_one");
    assertThat(containsAllNames("Tagged", new int[0])).containsExactly("empty", "one_two", "only1", "only2", "two_three_one");
  }

  /**
   * Regression test for issue #10757.
   * <p>
   * A whole list is no key of a BY ITEM index (#10448): an array bound to {@code =} is that list too, so it is answered by the scan
   * with or without the index, whatever binding ran before it.
   */
  @Test
  void anArrayEqualToTheWholeListAnswersAsTheListDoes() {
    createContainsAllTypes();
    for (final Map.Entry<String, Object> shape : shapesOf(1, 2).entrySet()) {
      final Map<String, Object> params = Map.of("p", shape.getValue());
      for (final String type : List.of("Tagged", "Plain"))
        try (final ResultSet rs = database.query("sql", "SELECT name FROM " + type + " WHERE tags = :p", params)) {
          assertThat(rs.stream().map(r -> r.<String>getProperty("name")).toList()).as("%s %s", type, shape.getKey())
              .containsExactly("one_two");
        }
    }
  }

  /** Regression test for issue #10757. */
  @Test
  void aMultiItemOperandUsesTheIndexAndRechecksTheCondition() {
    createContainsAllTypes();
    final String plan = containsAllPlan(new int[] { 1, 2 });
    assertThat(plan).contains("FETCH FROM INDEX Tagged[tagsbyitem]");
    assertThat(plan).contains("FILTER ITEMS WHERE");
    assertThat(containsAllPlan(List.of())).doesNotContain("FETCH FROM INDEX");
  }
}
