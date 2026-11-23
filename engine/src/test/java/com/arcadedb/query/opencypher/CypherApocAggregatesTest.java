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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.function.CypherFunctionRegistry;
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.offset;

/**
 * APOC's aggregating functions {@code apoc.agg.*}.
 * <p>
 * Each nested class holds the regression tests of the GitHub issue its name carries.
 */
class CypherApocAggregatesTest {

  /**
   * Regression tests for issue #12102: {@code apoc.agg.*} resolved to ArcadeDB's {@code agg.*}, scalar functions over one list, so
   * {@code UNWIND [1, 2, 3, 4] AS x RETURN apoc.agg.median(x)} answered four rows instead of 2.5. The expected values are what
   * Neo4j 5.26 with APOC 5.26.33 answers for the same statements.
   */
  @Nested
  class Issue12102ApocAggAggregates extends TestHelper {

    @Test
    void eachFunctionFoldsTheRowsIntoOneValue() {
      assertThat(single("UNWIND [1, 2, 3, 4] AS x RETURN apoc.agg.median(x) AS r")).isEqualTo(2.5);
      assertThat(single("UNWIND [3, 1, 2] AS x RETURN apoc.agg.first(x) AS r")).isEqualTo(3L);
      assertThat(single("UNWIND [3, 1, 2] AS x RETURN apoc.agg.last(x) AS r")).isEqualTo(2L);
      assertThat(single("UNWIND [2, 3, 4] AS x RETURN apoc.agg.product(x) AS r")).isEqualTo(24L);
      assertThat(single("UNWIND [1, 2, 3] AS x RETURN apoc.agg.nth(x, 1) AS r")).isEqualTo(2L);
      assertThat(single("UNWIND [1, 2, 3, 4] AS x RETURN apoc.agg.slice(x, 1, 2) AS r")).isEqualTo(List.of(2L, 3L));
      assertThat(single("UNWIND range(1, 10) AS x RETURN apoc.agg.percentiles(x, [0.5]) AS r")).isEqualTo(List.of(5L));
      assertThat(single("UNWIND [{n: 'a', v: 1}, {n: 'b', v: 5}, {n: 'c', v: 5}] AS m RETURN apoc.agg.maxItems(m.n, m.v) AS r"))
          .isEqualTo(Map.of("items", List.of("b", "c"), "value", 5L));
      assertThat(single("UNWIND [{n: 'a', v: 1}, {n: 'b', v: 5}, {n: 'c', v: 1}] AS m RETURN apoc.agg.minItems(m.n, m.v) AS r"))
          .isEqualTo(Map.of("items", List.of("a", "c"), "value", 1L));
    }

    @Test
    void oneValuePerGroup() {
      try (final ResultSet rs = database.query("opencypher", """
          UNWIND [{g: 1, v: 3}, {g: 1, v: 5}, {g: 2, v: 7}] AS m
          RETURN m.g AS g, apoc.agg.median(m.v) AS median, apoc.agg.first(m.v) AS first ORDER BY g""")) {
        final List<Result> rows = rs.stream().toList();
        assertThat(rows).hasSize(2);
        assertThat(rows.get(0).<Double>getProperty("median")).isEqualTo(4.0);
        assertThat(rows.get(0).<Long>getProperty("first")).isEqualTo(3L);
        assertThat(rows.get(1).<Double>getProperty("median")).isEqualTo(7.0);
        assertThat(rows.get(1).<Long>getProperty("first")).isEqualTo(7L);
      }
    }

    @Test
    void nullsAreSkippedAndPositionsCountNonNullValues() {
      assertThat(single("UNWIND [null, 1] AS x RETURN apoc.agg.first(x) AS r")).isEqualTo(1L);
      assertThat(single("UNWIND [1, null] AS x RETURN apoc.agg.last(x) AS r")).isEqualTo(1L);
      assertThat(single("UNWIND [null, 2, 3] AS x RETURN apoc.agg.nth(x, 0) AS r")).isEqualTo(2L);
      assertThat(single("UNWIND [1, null, 3] AS x RETURN apoc.agg.nth(x, 1) AS r")).isEqualTo(3L);
      assertThat(single("UNWIND [1, 2, 3] AS x RETURN apoc.agg.nth(x, -1) AS r")).isEqualTo(3L);
      assertThat(single("UNWIND [1, 2, 3] AS x RETURN apoc.agg.nth(x, 5) AS r")).isNull();
      assertThat(single("UNWIND [1, null, 3] AS x RETURN apoc.agg.slice(x, 0, 3) AS r")).isEqualTo(List.of(1L, 3L));
      assertThat(single("UNWIND [2, null, 3] AS x RETURN apoc.agg.median(x) AS r")).isEqualTo(2.5);
      assertThat(single("UNWIND [{n: null, v: 5}, {n: 'b', v: 1}] AS m RETURN apoc.agg.maxItems(m.n, m.v) AS r"))
          .isEqualTo(Map.of("items", List.of("b"), "value", 1L));
    }

    @Test
    void sliceReadsItsThirdArgumentAsALength() {
      assertThat(single("UNWIND [1, 2, 3, 4] AS x RETURN apoc.agg.slice(x) AS r")).isEqualTo(List.of(1L, 2L, 3L, 4L));
      assertThat(single("UNWIND [1, 2, 3, 4] AS x RETURN apoc.agg.slice(x, 1) AS r")).isEqualTo(List.of(2L, 3L, 4L));
      assertThat(single("UNWIND [1, 2, 3, 4] AS x RETURN apoc.agg.slice(x, 3, 10) AS r")).isEqualTo(List.of(4L));
      assertThat(single("UNWIND [1, 2, 3, 4] AS x RETURN apoc.agg.slice(x, -1, 2) AS r")).isEqualTo(List.of(1L));
      assertThat(single("UNWIND [1, 2, 3] AS x RETURN apoc.agg.slice(x, 1, -2) AS r")).isEqualTo(List.of());
    }

    @Test
    void percentilesAndStatisticsUseApocsDefaultsAndRank() {
      assertThat(single("UNWIND range(1, 10) AS x RETURN apoc.agg.percentiles(x) AS r")).isEqualTo(List.of(5L, 8L, 9L, 10L, 10L));
      assertThat(single("UNWIND range(1, 10) AS x RETURN apoc.agg.percentiles(x, [0, 0.1, 0.25, 0.5, 1.0]) AS r"))
          .isEqualTo(List.of(1L, 1L, 3L, 5L, 10L));
      assertThat(single("UNWIND [1, 2.5] AS x RETURN apoc.agg.percentiles(x, [0.5]) AS r")).isEqualTo(List.of(1.0));
      // the rank is p * count rounded half up: 0.75 of three values is the second one
      assertThat(single("UNWIND [1, 2, 3] AS x RETURN apoc.agg.percentiles(x) AS r")).isEqualTo(List.of(2L, 2L, 3L, 3L, 3L));

      final Map<String, Object> stats = map("UNWIND range(1, 10) AS x RETURN apoc.agg.statistics(x) AS r");
      assertThat(stats).containsEntry("total", 10L).containsEntry("min", 1L).containsEntry("max", 10L).containsEntry("mean", 5.5)
          .containsEntry("minNonZero", 1.0).containsEntry("0.5", 5L).containsEntry("0.75", 8L).containsEntry("0.9", 9L)
          .containsEntry("0.95", 10L).containsEntry("0.99", 10L);
      assertThat((Double) stats.get("stdev")).isCloseTo(2.8722813232690143, offset(1e-12));

      final Map<String, Object> zeros = map("UNWIND [0, 0] AS x RETURN apoc.agg.statistics(x, [0.5]) AS r");
      assertThat(zeros).containsEntry("total", 2L).containsEntry("min", 0L).containsEntry("minNonZero", (double) Long.MAX_VALUE)
          .containsEntry("0.5", 0L);
    }

    @Test
    void anEmptyGroupAnswersApocsEmptyValues() {
      try (final ResultSet rs = database.query("opencypher", """
          UNWIND [] AS x
          RETURN apoc.agg.first(x) AS first, apoc.agg.slice(x) AS slice, apoc.agg.median(x) AS median, apoc.agg.product(x) AS product,
                 apoc.agg.statistics(x) AS stats, apoc.agg.maxItems(x, x) AS items""")) {
        final Result row = rs.next();
        assertThat(row.<Object>getProperty("first")).isNull();
        assertThat(row.<List<Object>>getProperty("slice")).isEmpty();
        assertThat(row.<Object>getProperty("median")).isNull();
        assertThat(row.<Double>getProperty("product")).isEqualTo(0.0);
        assertThat(row.<Map<String, Object>>getProperty("stats")).containsEntry("total", 0L).containsEntry("mean", 0.0)
            .doesNotContainKey("0.5");
        assertThat(row.<Map<String, Object>>getProperty("items")).containsEntry("items", List.of());
        assertThat(rs.hasNext()).isFalse();
      }
    }

    @Test
    void productStaysAnIntegerUntilAFloatAndRefusesAnOverflow() {
      assertThat(single("UNWIND [2, 3.0] AS x RETURN apoc.agg.product(x) AS r")).isEqualTo(6.0);
      assertThatThrownBy(() -> single("UNWIND [9223372036854775807, 2] AS x RETURN apoc.agg.product(x) AS r"))
          .hasMessageContaining("apoc.agg.product");
      assertThatThrownBy(() -> single("UNWIND [2, 'a'] AS x RETURN apoc.agg.product(x) AS r"))
          .isInstanceOf(CommandSemanticException.class);
    }

    @Test
    void groupLimitCapsTheItems() {
      assertThat(single("UNWIND [{n: 'a', v: 1}, {n: 'b', v: 1}, {n: 'c', v: 1}] AS m RETURN apoc.agg.maxItems(m.n, m.v, 2) AS r"))
          .isEqualTo(Map.of("items", List.of("a", "b"), "value", 1L));
      assertThat(single("UNWIND [{n: 'a', v: 1}, {n: 'b', v: 5}, {n: 'c', v: 5}] AS m RETURN apoc.agg.maxItems(m.n, m.v, 0) AS r"))
          .isEqualTo(Map.of("items", List.of("b"), "value", 5L));
    }

    @Test
    void theAggListFunctionsKeepTheirListForm() {
      assertThat(single("RETURN agg.median([1, 2, 3, 4]) AS r")).isEqualTo(2.5);
      assertThat(single("RETURN agg.first([null, 'a', 'b']) AS r")).isEqualTo("a");
      try (final ResultSet rs = database.query("sql", "SELECT agg.median([1, 2, 3]) AS r")) {
        assertThat(rs.next().<Double>getProperty("r")).isEqualTo(2.0);
      }
    }

    /**
     * The end of the slice is {@code from + length}: a huge length or a huge offset used to overflow the long and answer an empty
     * list (or, for a negative offset and a negative length, everything).
     */
    @Test
    void sliceBoundsDoNotOverflow() {
      assertThat(single("UNWIND [1, 2, 3, 4] AS x RETURN apoc.agg.slice(x, 1, 9223372036854775807) AS r")).isEqualTo(
          List.of(2L, 3L, 4L));
      assertThat(single("UNWIND [1, 2, 3, 4] AS x RETURN apoc.agg.slice(x, 0, 9223372036854775807) AS r")).isEqualTo(
          List.of(1L, 2L, 3L, 4L));
      assertThat(single("UNWIND [1, 2, 3, 4] AS x RETURN apoc.agg.slice(x, 9223372036854775807, 2) AS r")).isEqualTo(List.of());
      assertThat(single("UNWIND [1, 2, 3, 4] AS x RETURN apoc.agg.slice(x, -9223372036854775807 - 1, 2) AS r")).isEqualTo(List.of());
      // a negative length other than -1 holds nothing, whatever the offset
      assertThat(single("UNWIND [1, 2, 3, 4] AS x RETURN apoc.agg.slice(x, -9223372036854775807 - 1, -2) AS r")).isEqualTo(
          List.of());
      assertThat(single("UNWIND [1, 2, 3, 4] AS x RETURN apoc.agg.slice(x, 1, -9223372036854775807 - 1) AS r")).isEqualTo(List.of());
    }

    /**
     * Min and max follow the order the percentiles are sorted by, where NaN is the greatest value, so a NaN met first does not pin
     * both of them to NaN.
     */
    @Test
    void statisticsOrderNaNAsTheGreatestValue() {
      final Map<String, Object> stats = map("UNWIND [0.0 / 0.0, 1.0, 2.0] AS x RETURN apoc.agg.statistics(x, [1.0]) AS r");
      assertThat(stats).containsEntry("total", 3L).containsEntry("min", 1.0).containsEntry("max", Double.NaN)
          .containsEntry("1.0", Double.NaN);
      assertThat(single("UNWIND [0.0 / 0.0, 1.0, 2.0] AS x RETURN apoc.agg.median(x) AS r")).isEqualTo(2.0);
    }

    /** A percentile below 0, above 1 or NaN is clamped to the first and to the last value, as HdrHistogram's rank is. */
    @Test
    void percentilesOutOfRangeAreClamped() {
      assertThat(single("UNWIND [1, 2, 3, 4] AS x RETURN apoc.agg.percentiles(x, [-1, 2, 0.0 / 0.0, 1.0 / 0.0, -1.0 / 0.0]) AS r"))
          .isEqualTo(List.of(1L, 4L, 1L, 4L, 1L));
      assertThat(single("UNWIND [1, 2, 3, 4] AS x RETURN apoc.agg.percentiles(x, []) AS r")).isEqualTo(List.of());
    }

    @Test
    void distinctFoldsOnlyTheDistinctValues() {
      assertThat(single("UNWIND [1, 1, 2, 2, 3] AS x RETURN apoc.agg.median(DISTINCT x) AS r")).isEqualTo(2.0);
      assertThat(single("UNWIND [3, 3, 1, 2, 2] AS x RETURN apoc.agg.slice(DISTINCT x, 1, 2) AS r")).isEqualTo(List.of(1L, 2L));
      assertThat(single("UNWIND [5, 5, 6, 6, 7] AS x RETURN apoc.agg.nth(DISTINCT x, 1) AS r")).isEqualTo(6L);
      assertThat(single("UNWIND [2, 2, 3] AS x RETURN apoc.agg.product(DISTINCT x) AS r")).isEqualTo(6L);
    }

    @Test
    void usableInsideWithAndInsideAnExpression() {
      assertThat(single("UNWIND [1, 2, 3, 4] AS x WITH apoc.agg.median(x) AS m RETURN m * 2 AS r")).isEqualTo(5.0);
      assertThat(single("UNWIND [1, 2, 3, 4] AS x RETURN apoc.agg.median(x) * 2 AS r")).isEqualTo(5.0);
      assertThat(single("UNWIND [1, 2, 3, 4] AS x RETURN apoc.agg.first(x) + apoc.agg.last(x) AS r")).isEqualTo(5L);
      assertThat(single("UNWIND [1, 2, 3, 4] AS x RETURN size(apoc.agg.slice(x, 1, 2)) AS r")).isEqualTo(2L);
    }

    @Test
    void aFunctionRegisteredOverTheNameStillWins() {
      final StatelessFunction original = CypherFunctionRegistry.get("agg.median");
      assertThat(original).isNotNull();
      try {
        CypherFunctionRegistry.registerOrReplace(new StatelessFunction() {
          @Override
          public String getName() {
            return "agg.median";
          }

          @Override
          public Object execute(final Object[] args, final CommandContext context) {
            return "mine";
          }
        });
        try (final ResultSet rs = database.query("opencypher", "UNWIND [1, 2, 3, 4] AS x RETURN apoc.agg.median(x) AS r")) {
          final List<Result> rows = rs.stream().toList();
          assertThat(rows).hasSize(4);
          assertThat(rows.getFirst().<String>getProperty("r")).isEqualTo("mine");
        }
      } finally {
        CypherFunctionRegistry.registerOrReplace(original);
      }
      assertThat(single("UNWIND [1, 2, 3, 4] AS x RETURN apoc.agg.median(x) AS r")).isEqualTo(2.5);
    }

    /** What a group holds is charged to the elements cap of the query, as collect() does. */
    @Test
    void theValuesAGroupHoldsAreBounded() {
      database.getConfiguration().setValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP, 5L);
      for (final String call : List.of("apoc.agg.median(x)", "apoc.agg.slice(x)", "apoc.agg.percentiles(x)", "apoc.agg.statistics(x)",
          "apoc.agg.maxItems(x, 1)", "apoc.agg.minItems(x, 1)", "apoc.agg.median(DISTINCT x)"))
        assertThatThrownBy(() -> single("UNWIND range(1, 20) AS x RETURN " + call + " AS r")).as(call)
            .isInstanceOf(CommandExecutionException.class).hasMessageContaining(
                GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getKey());

      // the cap is a count of what is held at once: a group within it, and a group that holds no more than a few values, run
      assertThat(single("UNWIND range(1, 5) AS x RETURN apoc.agg.median(x) AS r")).isEqualTo(3.0);
      assertThat(single("UNWIND range(1, 20) AS x RETURN apoc.agg.slice(x, 0, 5) AS r")).isEqualTo(List.of(1L, 2L, 3L, 4L, 5L));
      assertThat(single("UNWIND range(1, 20) AS x RETURN apoc.agg.product(x % 2 + 1) AS r")).isEqualTo(1024L);
      assertThat(single("UNWIND range(1, 20) AS x RETURN apoc.agg.maxItems(x, x) AS r")).isEqualTo(
          Map.of("items", List.of(20L), "value", 20L));
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> map(final String query) {
      return (Map<String, Object>) single(query);
    }

    private Object single(final String query) {
      try (final ResultSet rs = database.query("opencypher", query)) {
        final List<Result> rows = rs.stream().toList();
        assertThat(rows).as(query).hasSize(1);
        return rows.getFirst().getProperty("r");
      }
    }
  }
}
