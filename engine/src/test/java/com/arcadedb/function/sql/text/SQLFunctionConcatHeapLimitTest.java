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
package com.arcadedb.function.sql.text;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.HeapLimitExceededException;
import com.arcadedb.query.sql.executor.QueryHeapBudget;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The text the {@code concat()} aggregate gathers is held in memory until the aggregation ends, so it is accounted like
 * every other aggregate that keeps the values it is given: the bytes of the text reach the heap budget the running queries
 * share ({@code arcadedb.queryMaxHeapRAM}), and the number of values it holds the per-operation element cap
 * ({@code arcadedb.queryMaxHeapElementsAllowedPerOp}).
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class SQLFunctionConcatHeapLimitTest extends TestHelper {
  private static final int    ROWS        = 300;
  private static final int    TEXT_LENGTH = 4_000;
  // THE TEXT OF EVERY ROW, WHICH ALL THE ROWS SHARE: THE 300 OF THEM ARE 1.2M CHARACTERS
  private static final String TEXT        = "x".repeat(TEXT_LENGTH);

  private long baseline;

  @Override
  protected void beginTest() {
    database.getSchema().createVertexType("Item");
    database.transaction(() -> {
      for (int i = 0; i < ROWS; i++)
        database.newVertex("Item").set("id", i, "grp", i % 3, "payload", TEXT, "tag", "t" + i).save();
    });
    baseline = QueryHeapBudget.getReservedBytes();
  }

  @Test
  void aWholeTableConcatHoldsItsTextUnderTheBudget() {
    // 1.2M characters of text do not fit a budget of 1MB, whatever the other queries hold
    GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(1L);

    assertRefused("sql", "SELECT concat(payload) AS t FROM Item");
    assertRefused("sql", "SELECT concat(payload, ',') AS t FROM Item");
  }

  @Test
  void aGroupedConcatHoldsItsTextUnderTheBudget() {
    GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(1L);

    // THREE GROUPS OF 100 ROWS EACH: 400K CHARACTERS A GROUP, 1.2M IN ALL
    assertRefused("sql", "SELECT grp, concat(payload, ',') AS t FROM Item GROUP BY grp");
  }

  @Test
  void theDelimiterOfEveryRowIsAccounted() {
    GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(1L);

    // A SHORT VALUE AND A LONG DELIMITER, OF A DIFFERENT SIZE EVERY ROW IN THE SECOND
    assertRefused("sql", "SELECT concat(tag, payload) AS t FROM Item");
    assertRefused("sql", "SELECT concat(tag, payload.substring(0, 3000 + id)) AS t FROM Item");
  }

  @Test
  void aConcatOverTheCapOfValuesFailsTheQuery() {
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP, 100L);

    assertThatThrownBy(() -> drain("sql", "SELECT concat(tag, ',') AS t FROM Item")).isInstanceOf(HeapLimitExceededException.class)
        .hasMessageContaining(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getKey());
    // THREE GROUPS OF 100 VALUES EACH: NONE HOLDS MORE THAN THE CAP
    assertThat(drain("sql", "SELECT grp, concat(tag, ',') AS t FROM Item GROUP BY grp")).isEqualTo(3);

    // WITH THE BUDGET SWITCHED OFF THE CAP IS ALL THERE IS
    GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(0L);
    assertThatThrownBy(() -> drain("sql", "SELECT concat(tag, ',') AS t FROM Item")).isInstanceOf(HeapLimitExceededException.class)
        .hasMessageContaining(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getKey());
  }

  @Test
  void aConcatBridgedFromOpenCypherIsAccountedTheSame() {
    GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(1L);
    assertRefused("opencypher", "MATCH (i:Item) RETURN sql.concat(i.payload, ',') AS t");
    assertRefused("opencypher", "MATCH (i:Item) RETURN i.grp AS g, sql.concat(i.payload, ',') AS t");

    GlobalConfiguration.QUERY_MAX_HEAP_RAM.reset();
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP, 100L);
    assertThatThrownBy(() -> drain("opencypher", "MATCH (i:Item) RETURN sql.concat(i.tag, ',') AS t"))
        .isInstanceOf(HeapLimitExceededException.class)
        .hasMessageContaining(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getKey());
  }

  @Test
  void aConcatThatFitsTheLimitsAnswersTheWholeText() {
    try (final ResultSet rs = database.query("sql", "SELECT concat(tag, ',') AS t FROM Item")) {
      final String text = rs.next().getProperty("t");
      assertThat(text).startsWith("t0,t1,t2,").endsWith(",t" + (ROWS - 1));
      assertThat(text.split(",")).hasSize(ROWS);
    }

    try (final ResultSet rs = database.query("sql", "SELECT grp, concat(payload) AS t FROM Item GROUP BY grp ORDER BY grp")) {
      for (int group = 0; group < 3; group++)
        assertThat(rs.next().<String>getProperty("t")).hasSize(TEXT_LENGTH * ROWS / 3);
      assertThat(rs.hasNext()).isFalse();
    }

    try (final ResultSet rs = database.query("sql", "SELECT concat(DISTINCT grp, ',') AS t FROM Item")) {
      assertThat(rs.next().<String>getProperty("t")).isEqualTo("0,1,2");
    }

    try (final ResultSet rs = database.query("opencypher", "MATCH (i:Item) RETURN sql.concat(i.tag, ',') AS t")) {
      assertThat(rs.next().<String>getProperty("t").split(",")).hasSize(ROWS);
    }
  }

  @Test
  void theTextOfAConcatIsGivenBackWhenTheQueryEnds() {
    // 1.2M CHARACTERS, PAST WHAT A QUERY HOLDS WITHOUT RESERVING FROM THE BUDGET, AND WELL WITHIN THE BUDGET OF THE TEST JVM
    GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(512L);
    try (final ResultSet rs = database.query("sql", "SELECT concat(payload, ',') AS t FROM Item")) {
      assertThat(rs.next().<String>getProperty("t")).hasSize((TEXT_LENGTH + 1) * ROWS - 1);
    }
    assertThat(QueryHeapBudget.getReservedBytes()).isLessThanOrEqualTo(baseline);
  }

  private void assertRefused(final String language, final String query) {
    // A QUERY THAT ALONE NEEDS MORE THAN THE WHOLE BUDGET IS A HeapLimitExceededException, UNLESS A RESERVATION ANOTHER TEST LEFT
    // BEHIND IS WHAT TIPS IT, WHICH IS THE TRANSIENT ONE: BOTH ARE REFUSALS BY THE BUDGET, WHICH THE MESSAGE NAMES
    assertThatThrownBy(() -> drain(language, query)).as(query).isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining(GlobalConfiguration.QUERY_MAX_HEAP_RAM.getKey());
    assertThat(QueryHeapBudget.getReservedBytes()).as("the refused query holds nothing: " + query).isLessThanOrEqualTo(baseline);
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
}
