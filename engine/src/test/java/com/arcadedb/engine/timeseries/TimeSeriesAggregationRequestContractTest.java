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
import com.arcadedb.engine.timeseries.ColumnDefinition.ColumnRole;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Contract of the aggregation request: zero-width buckets and malformed projections are refused up front.
 */
class TimeSeriesAggregationRequestContractTest extends TestHelper {

  // ---- #7675 ----
  // Issue #7675: the one contract the three time-series wire protocols now share for the edge-case request
  // members they used to answer three different ways, pinned at the unit level because that is where the rule
  // lives. The protocol-level tests drive each surface through its own handler; these fix what those surfaces are
  // agreeing ON.
  // <p>
  // Each of the three rules below was already enforced by exactly one protocol and by none of the others:
  // {@code bucket_interval_ms <= 0} and an empty {@code requests} list were refused by gRPC alone, and an
  // unresolvable projection name was dropped by all three.

  /** ts (TIMESTAMP), location (TAG), temperature (FIELD), humidity (FIELD). */
  private static final List<ColumnDefinition> COLUMNS = List.of(
      new ColumnDefinition("ts", Type.LONG, ColumnRole.TIMESTAMP),
      new ColumnDefinition("location", Type.STRING, ColumnRole.TAG),
      new ColumnDefinition("temperature", Type.DOUBLE, ColumnRole.FIELD),
      new ColumnDefinition("humidity", Type.DOUBLE, ColumnRole.FIELD));

  // #7675
  // ---------------------------------------------------------------------------------------------------------
  // bucketInterval
  // ---------------------------------------------------------------------------------------------------------

  @Test
  void aNonPositiveBucketIntervalIsRefusedAndNamesTheMemberInTheCallersOwnSpelling() {
    assertThatThrownBy(() -> TimeSeriesGateway.requireBucketInterval(0, "aggregation.bucketInterval"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("aggregation.bucketInterval")
        .hasMessageContaining("positive")
        .hasMessageContaining("received 0");

    assertThatThrownBy(() -> TimeSeriesGateway.requireBucketInterval(-1, "TimeSeriesAggregation.bucket_interval_ms"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("TimeSeriesAggregation.bucket_interval_ms")
        .hasMessageContaining("received -1");
  }

  // #7675
  /**
   * The check has to WRAP the read, so it can sit in front of the value the handler goes on to use rather than
   * being a statement the next edit can drop without the compiler noticing.
   */
  @Test
  void aPositiveBucketIntervalIsHandedBackUnchanged() {
    assertThat(TimeSeriesGateway.requireBucketInterval(1, "aggregation.bucketInterval")).isEqualTo(1);
    assertThat(TimeSeriesGateway.requireBucketInterval(60_000, "aggregation.bucketInterval")).isEqualTo(60_000);
    assertThat(TimeSeriesGateway.requireBucketInterval(Long.MAX_VALUE, "aggregation.bucketInterval"))
        .isEqualTo(Long.MAX_VALUE);
  }

  // #7675
  // ---------------------------------------------------------------------------------------------------------
  // aggregation.requests
  // ---------------------------------------------------------------------------------------------------------

  @Test
  void anEmptyAggregationRequestListIsRefused() {
    assertThatThrownBy(() -> TimeSeriesGateway.requireAggregationRequests(0, "aggregation.requests"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("aggregation.requests")
        .hasMessageContaining("at least one");
  }

  // #7675
  @Test
  void oneOrMoreAggregationRequestsAreAccepted() {
    TimeSeriesGateway.requireAggregationRequests(1, "aggregation.requests");
    TimeSeriesGateway.requireAggregationRequests(5, "aggregation.requests");
  }

  // #7675
  // ---------------------------------------------------------------------------------------------------------
  // fields projection
  // ---------------------------------------------------------------------------------------------------------

  @Test
  void aProjectionNameThatMatchesNoColumnIsRefusedAndListsTheDeclaredColumns() {
    assertThatThrownBy(() -> TimeSeriesGateway.requireColumnIndices(List.of("temprature"), COLUMNS))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("temprature")
        .hasMessageContaining("ts, location, temperature, humidity");
  }

  // #7675
  /**
   * The partial case is the damaging one and is what made this the same defect as #7334's dropped tag: the
   * projection came back NARROWED to the names that happened to resolve, and the caller could not tell that
   * from a projection the server answered correctly.
   */
  @Test
  void aProjectionIsRefusedWhenOnlySomeOfItsNamesResolve() {
    assertThatThrownBy(() -> TimeSeriesGateway.requireColumnIndices(List.of("temprature", "humidity"), COLUMNS))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("temprature");
  }

  // #7675
  /**
   * And the wholly-mistyped case is worse than narrowing: every name was dropped, the array came back empty,
   * and the engine reads an empty projection as "every column" - so the projection WIDENED to the full row.
   */
  @Test
  void aProjectionWhereNoNameResolvesIsRefusedRatherThanWidenedToEveryColumn() {
    assertThat(TimeSeriesGateway.resolveColumnIndices(List.of("nope", "alsonope"), COLUMNS))
        .as("the lenient resolver still drops, which is what the refusal exists to replace on the wire")
        .isEmpty();

    assertThatThrownBy(() -> TimeSeriesGateway.requireColumnIndices(List.of("nope", "alsonope"), COLUMNS))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("nope");
  }

  // #7675
  /**
   * Naming the timestamp column is a legitimate spelling, not a typo: it is always projected and always first,
   * so it contributes no further index - the reading the lenient resolver always gave it. Refusing it would
   * have broken {@code "fields": ["ts","temperature"]}, which is how a caller that wants to be explicit writes
   * the projection.
   */
  @Test
  void namingTheTimestampColumnIsAcceptedAndSelectsNothingFurther() {
    assertThat(TimeSeriesGateway.requireColumnIndices(List.of("ts"), COLUMNS)).isEmpty();
    assertThat(TimeSeriesGateway.requireColumnIndices(List.of("ts", "temperature"), COLUMNS)).containsExactly(1);
  }

  // #7675
  /**
   * Everything the lenient resolver answers for a projection whose names all resolve, the strict one answers
   * identically: same ascending order, same de-duplication, same null for "no projection". Only the
   * unresolvable name behaves differently, which is the whole of the change.
   */
  @Test
  void aFullyResolvableProjectionIsUnchangedByTheStricterResolver() {
    assertThat(TimeSeriesGateway.requireColumnIndices(List.of("humidity", "location", "temperature"), COLUMNS))
        .containsExactly(0, 1, 2);
    assertThat(TimeSeriesGateway.requireColumnIndices(List.of("temperature", "temperature"), COLUMNS))
        .containsExactly(1);
    assertThat(TimeSeriesGateway.requireColumnIndices(List.of(), COLUMNS)).isNull();
    assertThat(TimeSeriesGateway.requireColumnIndices(null, COLUMNS)).isNull();
  }

  // #7675
  /**
   * The PromQL discovery endpoints keep the lenient resolver on purpose: a label the store does not carry is a
   * Prometheus-specified empty selection there, not a malformed request. This pins that the two resolvers are
   * still two, so a later edit cannot "simplify" them into one and take PromQL with it.
   */
  @Test
  void theLenientResolverIsStillLenientForThePromQLDiscoveryPath() {
    assertThat(TimeSeriesGateway.resolveColumnIndices(List.of("nosuchlabel"), COLUMNS)).isEmpty();
    assertThat(TimeSeriesGateway.resolveColumnIndices(List.of("nosuchlabel", "location"), COLUMNS))
        .containsExactly(0);
  }

  // ---- #7675 ----
  // Issue #7675, the SQL sibling the sweep found: {@code ts.timeBucket('0s', ts)} answered TWO different things
  // depending on which plan the query got.
  // <p>
  // {@code SQLFunctionTimeBucket.execute} refuses a zero-width interval - "must be a positive amount of time",
  // added for issue #6388 because the division below it threw {@code ArithmeticException: / by zero}. The
  // aggregation push-down in {@code SelectExecutionPlanner} did not go through {@code execute}: it called
  // {@code parseInterval} directly, which carries no such guard, and handed the {@code 0} to
  // {@code TimeSeriesEngine.aggregateMulti}, whose {@code useFlatMode = bucketIntervalMs > 0} reads it as "one
  // bucket over the whole range". So the same query answered one row when the planner pushed it down and threw
  // when it did not - the same shape of divergence, one step further in, as the three wire protocols this issue
  // is about.

  private static final String TYPE = "ZeroBucketMetric";

  /**
   * The push-down-eligible shape: {@code ts.timeBucket} projected, GROUP BY on its alias, an aggregate
   * alongside, no residual WHERE. This is exactly the query {@link #aPositiveIntervalStillPushesDown} proves
   * reaches {@code AggregateFromTimeSeriesStep}, so the refusal below cannot be a query that was never
   * eligible in the first place.
   */
  private static String query(final String interval) {
    return "SELECT ts.timeBucket('" + interval + "', ts) AS tsBucket, sum(value) AS s"
        + " FROM " + TYPE + " GROUP BY tsBucket";
  }

  // #7675
  @Test
  void aZeroWidthBucketIsRefusedRatherThanCollapsingTheWholeRangeIntoOneRow() {
    createTypeWithSamples();

    assertThatThrownBy(() -> {
      try (final ResultSet rs = database.query("sql", query("0s"))) {
        rs.stream().toList();
      }
    })
        .as("'0s' is not a bucketing, and answering one row over the whole range looks like a legitimate answer")
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must be a positive amount of time");
  }

  // #7675
  /**
   * '0m' and '0h' parse through different arms of the unit switch and reach the same zero, so the refusal must
   * not be pinned to one spelling of it.
   */
  @Test
  void everySpellingOfAZeroWidthBucketIsRefusedAlike() {
    createTypeWithSamples();

    for (final String interval : new String[] { "0s", "0m", "0h", "0d", "0w" })
      assertThatThrownBy(() -> {
        try (final ResultSet rs = database.query("sql", query(interval))) {
          rs.stream().toList();
        }
      })
          .as("interval '%s'", interval)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("must be a positive amount of time");
  }

  // #7675
  /**
   * The counter-case, and the one that proves the bail-out is narrow: a positive interval still takes the
   * push-down. Without this the fix above could have disabled the optimisation entirely and nothing would have
   * said so - the generic aggregation path answers the same numbers.
   */
  @Test
  void aPositiveIntervalStillPushesDown() {
    createTypeWithSamples();

    try (final ResultSet rs = database.query("sql", query("1h"))) {
      final String plan = rs.getExecutionPlan()
          .orElseThrow(() -> new AssertionError("the query produced no execution plan to check"))
          .prettyPrint(0, 2);
      assertThat(plan).as("a positive interval must still reach the TimeSeries push-down step")
          .contains("AGGREGATE FROM TIMESERIES " + TYPE);

      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat(row.<Double>getProperty("s")).isEqualTo(10.0);
      assertThat(rs.hasNext()).as("the three samples fall in one hour-wide bucket").isFalse();
    }
  }

  private void createTypeWithSamples() {
    database.command("sql", "CREATE TIMESERIES TYPE " + TYPE + " TIMESTAMP ts FIELDS (value DOUBLE) SHARDS 1");

    final LocalTimeSeriesType tsType = (LocalTimeSeriesType) database.getSchema().getType(TYPE);
    database.transaction(() -> {
      try {
        tsType.getEngine().appendSamples(new long[] { 1_000L, 2_000L, 3_000L }, new Object[] { 4.0, 1.0, 5.0 });
      } catch (final Exception e) {
        throw new IllegalStateException("cannot append the test samples", e);
      }
    });
  }
}
