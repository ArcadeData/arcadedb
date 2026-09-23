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
package com.arcadedb.engine.timeseries;

import com.arcadedb.TestHelper;
import com.arcadedb.schema.LocalTimeSeriesType;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8189: ONE column-index convention in the TimeSeries aggregation subsystem, not two.
 * <p>
 * After #8140 both halves of the multi-column push-down read
 * {@link MultiColumnAggregationRequest#columnIndex()} as a position in the ENGINE ROW - {@code 0} is the
 * timestamp, {@code 1 +} the ordinal among the non-TIMESTAMP columns is a value column. The single-column
 * {@code TimeSeriesEngine.aggregate} / {@code TimeSeriesSealedStore.aggregate} pair beside it counted
 * non-TIMESTAMP columns instead, with no {@code +1}. Both were correct and both were documented, which is
 * exactly the shape of a foot-gun: the two numbers coincide for any type whose TIMESTAMP column is declared
 * FIRST, so a caller that picked the wrong one saw nothing wrong until issue #7702 made a later TIMESTAMP
 * spellable - the way #8140 itself was found.
 * <p>
 * The pair is deleted rather than converted. Nothing in {@code src/main} called it - the three wire protocols
 * and the SQL push-down all go through {@code aggregateMulti}, which subsumes it - so its only callers were its
 * own tests, and a method that exists to be tested is the cheapest kind of convention to be rid of. Those tests
 * now ask {@code aggregateMulti} the same questions, which is a coverage improvement on top: they exercise the
 * path production actually takes.
 * <p>
 * This test is a REFLECTION GUARD because the property it pins is an absence, and an absence has no call site
 * to assert against. Re-adding either method, under any signature taking a column index, brings the second
 * convention back with it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/8189">issue #8189</a>
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/8140">issue #8140</a>
 */
class Issue8189OneColumnIndexConventionTest extends TestHelper {

  private static final long TS   = 1_700_000_000_000L;
  private static final long HOUR = 3_600_000L;

  @Test
  void neitherAggregationClassCarriesASecondColumnIndexConventionAnyMore() {
    assertThat(singleColumnAggregationMethodsOf(TimeSeriesEngine.class))
        .as("TimeSeriesEngine.aggregate counted non-TIMESTAMP columns; aggregateMulti subsumes it").isEmpty();
    assertThat(singleColumnAggregationMethodsOf(TimeSeriesSealedStore.class))
        .as("and so did its sealed-half counterpart").isEmpty();
  }

  /**
   * The convention that remains, asserted where it can actually be wrong: a type declaring a column BEFORE the
   * timestamp, so the row index and the schema index of the same column are different numbers.
   */
  @Test
  void theOneConventionLeftIsTheRowIndexOnBothStorageHalves() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE Conv8189"
        + " FIELDS (v DOUBLE) TAGS (host STRING) TIMESTAMP ts SHARDS 1");
    for (int i = 0; i < 5; i++)
      database.command("sql", "INSERT INTO Conv8189 SET ts = " + (TS + i * 1000L)
          + ", host = 'h', v = " + (i + 1) + ".0");

    final LocalTimeSeriesType tsType = (LocalTimeSeriesType) database.getSchema().getType("Conv8189");
    final TimeSeriesEngine engine = tsType.getEngine();
    final long bucket = Math.floorDiv(TS, HOUR) * HOUR;

    // 'v' is SCHEMA index 0 and ROW index 1. The request carries the row index, and the gateway is the one
    // place that converts - which is why it is public.
    assertThat(TimeSeriesGateway.findColumnIndex("v", tsType.getTsColumns())).isZero();
    assertThat(TimeSeriesGateway.aggregationRowIndex(tsType.getTsColumns(), 0)).isEqualTo(1);

    final List<MultiColumnAggregationRequest> requests =
        List.of(new MultiColumnAggregationRequest(1, AggregationType.SUM, "sum_v"));

    assertThat(engine.aggregateMulti(Long.MIN_VALUE, Long.MAX_VALUE, requests, HOUR, null).getValue(bucket, 0))
        .as("mutable").isEqualTo(15.0);

    engine.compactAll();

    assertThat(engine.aggregateMulti(Long.MIN_VALUE, Long.MAX_VALUE, requests, HOUR, null).getValue(bucket, 0))
        .as("sealed: the same number names the same column").isEqualTo(15.0);
  }

  /**
   * Two nets, and deliberately the broader pair. The load-bearing one is the SHAPE - anything returning an
   * {@link AggregationResult} is by construction the single-column path, since the multi-column one returns
   * {@link MultiColumnAggregationResult} - so reintroducing the convention under a different name does not slip
   * past. The NAME is matched as well, which is wider than it needs to be: a future method called
   * {@code aggregate} for some unrelated purpose would trip this too. That is the intended trade on a class
   * whose whole subject is one deleted pair of methods - a false positive costs whoever adds it one look at this
   * javadoc, and a false negative costs the second convention coming back unnoticed.
   */
  private static List<String> singleColumnAggregationMethodsOf(final Class<?> type) {
    return Arrays.stream(type.getDeclaredMethods())
        .filter(m -> AggregationResult.class.equals(m.getReturnType()) || "aggregate".equals(m.getName()))
        .map(Method::toGenericString)
        .toList();
  }
}
