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

import com.arcadedb.engine.timeseries.ColumnDefinition.ColumnRole;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7675: the one contract the three time-series wire protocols now share for the edge-case request
 * members they used to answer three different ways, pinned at the unit level because that is where the rule
 * lives. The protocol-level tests drive each surface through its own handler; these fix what those surfaces are
 * agreeing ON.
 * <p>
 * Each of the three rules below was already enforced by exactly one protocol and by none of the others:
 * {@code bucket_interval_ms <= 0} and an empty {@code requests} list were refused by gRPC alone, and an
 * unresolvable projection name was dropped by all three.
 */
class Issue7675EdgeCaseRequestContractTest {

  /** ts (TIMESTAMP), location (TAG), temperature (FIELD), humidity (FIELD). */
  private static final List<ColumnDefinition> COLUMNS = List.of(
      new ColumnDefinition("ts", Type.LONG, ColumnRole.TIMESTAMP),
      new ColumnDefinition("location", Type.STRING, ColumnRole.TAG),
      new ColumnDefinition("temperature", Type.DOUBLE, ColumnRole.FIELD),
      new ColumnDefinition("humidity", Type.DOUBLE, ColumnRole.FIELD));

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

  @Test
  void oneOrMoreAggregationRequestsAreAccepted() {
    TimeSeriesGateway.requireAggregationRequests(1, "aggregation.requests");
    TimeSeriesGateway.requireAggregationRequests(5, "aggregation.requests");
  }

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
}
