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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8333: the SQL planner served a range from an index whatever share of the type it returned, so a
 * non-selective range (TPC-H Q1 reads ~98% of lineitem) was fetched one random page access per record and fell far
 * behind a plain scan once the type outgrew the page cache. The index fetch now reads the matching entries first and
 * either gives way to a scan filtered by the same condition, or loads the records in physical order - wherever the
 * order the rows arrive in cannot show in the output: an aggregation, or an ORDER BY the index does not serve. A query
 * returning its rows as they come keeps the index order it always had.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8333IndexRangeSelectivityTest extends TestHelper {
  private static final int       ROWS = 2_000;
  private static final LocalDate BASE = LocalDate.of(1992, 1, 1);

  @Override
  protected void beginTest() {
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

  @Test
  void nonSelectiveRangeIsServedByAScan() {
    // ~98% of the rows, the shape of TPC-H Q1
    final String bound = BASE.plusDays(ROWS * 98 / 100).toString();
    final Execution execution = run("SELECT FROM LineItem WHERE l_shipdate <= '" + bound + "' ORDER BY l_seq", Map.of());

    assertThat(execution.strategy()).isEqualTo(GetValueFromIndexEntryStep.Strategy.SCAN);
    assertThat(execution.plan()).contains("FETCH FROM INDEX LineItem[l_shipdate]").contains("served by full scan");
    assertThat(execution.rids()).containsExactlyInAnyOrderElementsOf(expected(d -> d.getString("l_shipdate").compareTo(bound) <= 0));
  }

  @Test
  void selectiveRangeIsLoadedInPhysicalOrder() {
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

  @Test
  void theSameStatementDecidesPerParameterValue() {
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

  @Test
  void typedPropertyComparedWithAStringAnswersTheSameThroughTheScan() {
    // The index converts '1993-06-01' to the DATE the property holds; the scan compares the record's value with the
    // string. Both must pick the same rows.
    final LocalDate bound = BASE.plusDays(517);
    final Execution execution = run("SELECT FROM LineItem WHERE l_day > '" + bound + "' ORDER BY l_seq", Map.of());

    assertThat(execution.strategy()).isEqualTo(GetValueFromIndexEntryStep.Strategy.SCAN);
    assertThat(execution.rids()).containsExactlyInAnyOrderElementsOf(
        expected(d -> d.getLocalDate("l_day").isAfter(bound)));
  }

  @Test
  void betweenAndResidualConditionsKeepTheirMeaning() {
    final Execution execution = run(
        "SELECT FROM LineItem WHERE l_seq BETWEEN 100 AND 1900 AND l_seq % 3 = 0 ORDER BY l_flag", Map.of());

    assertThat(execution.strategy()).isEqualTo(GetValueFromIndexEntryStep.Strategy.SCAN);
    assertThat(execution.rids()).containsExactlyInAnyOrderElementsOf(
        expected(d -> d.getInteger("l_seq") >= 100 && d.getInteger("l_seq") <= 1900 && d.getInteger("l_seq") % 3 == 0));
  }

  @Test
  void betweenOrdersStringsLikeTheIndexOnTheScan() {
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

  @Test
  void aggregationOverANonSelectiveRange() {
    final String bound = BASE.plusDays(ROWS / 2).toString();
    try (final ResultSet rs = database.query("sql",
        "SELECT count(*) AS n FROM LineItem WHERE l_shipdate > '" + bound + "' LIMIT 1")) {
      assertThat(rs.next().<Long>getProperty("n")).isEqualTo(
          expected(d -> d.getString("l_shipdate").compareTo(bound) > 0).size());
    }
  }

  @Test
  void rangeLargerThanTheBufferIsServedInSortedChunks() {
    PhysicalOrderRidFetcher.maxBufferedRids = 64;
    final String to = BASE.plusDays(300).toString();
    final Execution execution = run("SELECT FROM LineItem WHERE l_shipdate < '" + to + "' ORDER BY l_flag", Map.of());

    assertThat(execution.strategy()).isEqualTo(GetValueFromIndexEntryStep.Strategy.PHYSICAL_ORDER_CHUNKED);
    assertThat(execution.rids()).hasSize(300);
    assertThat(execution.rids()).containsExactlyInAnyOrderElementsOf(expected(d -> d.getString("l_shipdate").compareTo(to) < 0));
    for (int chunk = 0; chunk < execution.rids().size(); chunk += 64)
      assertPhysicalOrder(execution.rids().subList(chunk, Math.min(execution.rids().size(), chunk + 64)));
  }

  @Test
  void uncommittedChangesAreSeenByBothStrategies() {
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

  @Test
  void subTypesAreScannedToo() {
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

  @Test
  void indexOrderServingOrderByIsKept() {
    final Execution execution = run("SELECT FROM LineItem WHERE l_shipdate > '1992-01-01' ORDER BY l_shipdate", Map.of());
    assertThat(execution.step().getScanFallback()).isNull();
    assertThat(execution.rids()).hasSize(ROWS - 1);
  }

  @Test
  void orderByAnotherPropertyAllowsTheFallback() {
    final Execution execution = run("SELECT FROM LineItem WHERE l_shipdate > '1992-01-01' ORDER BY l_seq", Map.of());
    assertThat(execution.strategy()).isEqualTo(GetValueFromIndexEntryStep.Strategy.SCAN);
    assertThat(execution.rids()).hasSize(ROWS - 1);
  }

  @Test
  void rowsReturnedAsTheyComeKeepTheIndexOrder() {
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

  @Test
  void anAggregationAdaptsWithoutAnOrderBy() {
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

  @Test
  void uniquePointLookupIsLeftAlone() {
    final Execution execution = run("SELECT FROM LineItem WHERE l_seq = 42 ORDER BY l_flag", Map.of());
    assertThat(execution.step().getScanFallback()).isNull();
    assertThat(execution.rids()).hasSize(1);
  }

  @Test
  void caseInsensitiveIndexIsLeftAlone() {
    // The CI index matches 'ABC' for 'abc', evaluating the condition on the record would not
    final Execution execution = run("SELECT FROM LineItem WHERE l_code = 'abc' ORDER BY l_seq", Map.of());
    assertThat(execution.step().getScanFallback()).isNull();
    assertThat(execution.rids()).hasSize(ROWS);
  }

  @Test
  void settingZeroDisablesIt() {
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
}
