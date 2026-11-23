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
package com.arcadedb.query.sql;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.IntPredicate;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A LET subquery reads the outer record through {@code $parent.$current} whatever fetched the outer rows (issue #12301). After an
 * index fetch, $current was the index entry ({key, rid}), so {@code $parent.$current.id} was null and every LET found nothing.
 */
class Issue12301LetParentAfterIndexFetchTest extends TestHelper {
  private static final int    RECORDS        = 3_000;
  // what the plan of an index fetch says about how its rows were loaded: nothing when no fallback was planned
  private static final String INDEX_ORDER    = null;
  private static final String PHYSICAL_ORDER = "physical order";
  private static final String FULL_SCAN      = "full scan";
  private static final String SELECT         = "SELECT id, $n AS nxt FROM T LET $n = (SELECT name FROM T WHERE id = $parent.$current.id + 1) ";

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE DOCUMENT TYPE T BUCKETS 4");
    database.command("sql", "CREATE PROPERTY T.id INTEGER");
    database.command("sql", "CREATE INDEX ON T (id) UNIQUE");
    database.command("sql", "CREATE DOCUMENT TYPE S EXTENDS T");
    database.transaction(() -> {
      for (int i = 0; i < RECORDS; i++)
        database.newDocument(i % 5 == 4 ? "S" : "T").set("id", i).set("name", "n" + i).save();
    });
    database.transaction(() -> database.command("sql", "DELETE FROM T WHERE id = 11"));
  }

  @Test
  void anIndexedWhereGivesTheLetItsOuterRecord() {
    assertRows("WHERE id < 40", i -> i < 40, false, INDEX_ORDER);
    assertRows("WHERE id = 10", i -> i == 10, false, INDEX_ORDER);
    assertRows("WHERE id IN [5, 10, 2999]", i -> i == 5 || i == 10 || i == 2999, false, INDEX_ORDER);
    // most of the type, but nothing re-sorts the rows: the planner offers the physical-order load and the scan only to an
    // aggregation or to an ORDER BY the index does not serve, so this stays in index order
    assertRows("WHERE id >= 5", i -> i >= 5, false, INDEX_ORDER);
  }

  @Test
  void anIndexOrderedWalkGivesTheLetItsOuterRecord() {
    assertRows("ORDER BY id LIMIT 30", i -> i < 31, true, INDEX_ORDER);
    assertRows("WHERE id < 100 ORDER BY id DESC", i -> i < 100, true, INDEX_ORDER);
  }

  @Test
  void aSequentialPhysicalOrderLoadGivesTheLetItsOuterRecord() {
    // ORDER BY name is not served by the index, so the records are loaded in physical order: here by the sequential load, which
    // reads every index entry before it loads the first record
    withParallelScan(false, () -> {
      assertRows("WHERE id < 40 ORDER BY name", i -> i < 40, false, PHYSICAL_ORDER);
      assertRows("WHERE id < 800 ORDER BY name", i -> i < 800, false, PHYSICAL_ORDER);
    });
  }

  @Test
  void aPhysicalOrderLoadWithTheDefaultSettingsGivesTheLetItsOuterRecord() {
    // the load may be shared by the workers of a parallel scan, depending on the cores of the machine: either way it is physical order
    assertRows("WHERE id < 40 ORDER BY name", i -> i < 40, false, PHYSICAL_ORDER);
  }

  @Test
  void aFullScanFallbackGivesTheLetItsOuterRecord() {
    // a range this wide gives way to a scan of the type, which sets $current itself
    assertRows("WHERE id >= 5 ORDER BY name", i -> i >= 5, false, FULL_SCAN);
  }

  @Test
  void theSameStatementWithoutTheIndexAgrees() {
    assertThat(rows(SELECT + "WHERE id < 40")).containsExactlyInAnyOrderElementsOf(rows(SELECT + "WHERE id + 0 < 40"));
  }

  private void withParallelScan(final boolean enabled, final Runnable body) {
    final boolean before = database.getConfiguration().getValueAsBoolean(GlobalConfiguration.QUERY_PARALLEL_SCAN);
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, enabled);
    try {
      body.run();
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, before);
    }
  }

  /**
   * The strategy the fetch picks is decided on the first pull and shown by the executed plan. A statement the planner built no
   * fallback for ({@link #INDEX_ORDER}) shows none.
   */
  private void assertServedBy(final String tail, final String strategy) {
    try (final ResultSet rs = database.query("sql", "PROFILE " + SELECT + tail)) {
      while (rs.hasNext())
        rs.next();
      final String plan = rs.getExecutionPlan().map(p -> p.prettyPrint(0, 2)).orElse("");
      assertThat(plan).as(tail).contains("EXTRACT VALUE FROM INDEX ENTRY");
      if (strategy == INDEX_ORDER)
        assertThat(plan).as(tail).doesNotContain("served by");
      else
        assertThat(plan).as(tail).contains("served by " + strategy);
    }
  }

  private void assertRows(final String tail, final IntPredicate outer, final boolean ordered, final String strategy) {
    assertServedBy(tail, strategy);
    final List<String> actual = rows(SELECT + tail);
    final List<String> expected = new ArrayList<>();
    for (int i = 0; i < RECORDS; i++)
      if (i != 11 && outer.test(i))
        expected.add(i + "->" + (i + 1 == 11 || i + 1 == RECORDS ? "" : "n" + (i + 1)));
    if (tail.contains("DESC"))
      expected.sort((a, b) -> Integer.compare(Integer.parseInt(b.split("->")[0]), Integer.parseInt(a.split("->")[0])));
    if (ordered)
      assertThat(actual).as(tail).containsExactlyElementsOf(expected);
    else
      assertThat(actual).as(tail).containsExactlyInAnyOrderElementsOf(expected);
  }

  private List<String> rows(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return rs.stream().map(r -> r.getProperty("id") + "->" + nameOf(r.getProperty("nxt"))).toList();
    }
  }

  private static String nameOf(final Object nxt) {
    if (!(nxt instanceof List<?> list) || list.isEmpty())
      return "";
    return ((Result) list.getFirst()).getProperty("name");
  }
}
